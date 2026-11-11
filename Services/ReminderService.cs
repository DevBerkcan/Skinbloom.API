using BarberDario.Api.Data;
using BarberDario.Api.Data.Entities;
using Hangfire;
using Microsoft.EntityFrameworkCore;

namespace BarberDario.Api.Services;

public class ReminderService
{
    private readonly SkinbloomDbContext _context;
    private readonly IBackgroundJobClient _backgroundJobs;
    private readonly ILogger<ReminderService> _logger;

    public ReminderService(
        SkinbloomDbContext context,
        IBackgroundJobClient backgroundJobs,
        ILogger<ReminderService> logger)
    {
        _context = context;
        _backgroundJobs = backgroundJobs;
        _logger = logger;
    }

    /// <summary>
    /// Queues reminder emails for bookings that are tomorrow.
    /// Called daily by Hangfire; each reminder runs as its own job so a failed one is retried
    /// on its own. EmailService sets ReminderSentAt once the reminder was actually sent.
    /// </summary>
    public async Task SendDailyRemindersAsync()
    {
        var tomorrow = DateOnly.FromDateTime(DateTime.UtcNow.AddDays(1));

        _logger.LogInformation("Starting daily reminder job for date: {Date}", tomorrow);

        var bookingsToRemind = await _context.Bookings
            .Include(b => b.Customer)
            .Where(b =>
                b.BookingDate == tomorrow &&
                b.Status == BookingStatus.Confirmed &&
                !b.ReminderSentAt.HasValue)
            .ToListAsync();

        _logger.LogInformation("Found {Count} bookings for tomorrow", bookingsToRemind.Count);

        int queuedCount = 0;
        int failureCount = 0;
        int skippedCount = 0;

        foreach (var booking in bookingsToRemind)
        {
            if (booking.Customer == null || string.IsNullOrEmpty(booking.Customer.Email))
            {
                // Mark as processed so we don't look at it again
                skippedCount++;
                booking.ReminderSentAt = DateTime.UtcNow;
                continue;
            }

            try
            {
                _backgroundJobs.Enqueue<EmailService>(s => s.SendBookingReminderAsync(booking.Id));
                queuedCount++;
            }
            catch (Exception ex)
            {
                failureCount++;
                _logger.LogError(ex, "Could not queue reminder for booking {BookingId}", booking.Id);
            }
        }

        await _context.SaveChangesAsync();

        _logger.LogInformation(
            "Daily reminder job completed. Queued: {Queued}, Failed to queue: {Failed}, Skipped (no email): {Skipped}",
            queuedCount,
            failureCount,
            skippedCount);
    }
}
