using Microsoft.Extensions.Options;
using Npgsql;
using Polly;
using RmqConsumerService.Configuration;
using RmqConsumerService.Services.Interfaces;

namespace RmqConsumerService.Services;

public sealed class DatabaseOrchestrator : IDatabaseOrchestrator
{
    private readonly IAdminDbService _admin;
    private readonly ITenantDbService _tenant;
    private readonly ILicenseService _license;
    private readonly DatabaseSettings _settings;
    private readonly ILogger<DatabaseOrchestrator> _logger;

    public DatabaseOrchestrator(
        IAdminDbService admin, ITenantDbService tenant,
        ILicenseService license, IOptions<DatabaseSettings> settings,
        ILogger<DatabaseOrchestrator> logger)
    {
        _admin = admin;
        _tenant = tenant;
        _license = license;
        _settings = settings.Value;
        _logger = logger;
    }

    public async Task<bool> ProvisionTenantAsync(string axiaAcId, string email, CancellationToken ct)
    {
        var id = Sanitise(axiaAcId);

        try
        {
            var template = Sanitise(_settings.TemplateDatabaseName);

            // Polly retry lives here — orchestration concern, not service concern
            var retry = Policy
                .Handle<NpgsqlException>(ex => ex.IsTransient)
                .Or<TimeoutException>()
                .WaitAndRetryAsync(3, i => TimeSpan.FromSeconds(Math.Pow(2, i)));

            await retry.ExecuteAsync(async () =>
            {
                if (!await _admin.DatabaseExistsAsync(id, ct))
                {
                    await _admin.EnsureRoleAsync(id, _settings.DefaultRolePassword, ct);
                    await _admin.TerminateConnectionsAsync(template, ct);
                    await _admin.CloneDatabaseAsync(id, template, ct);
                }
                else
                {
                    _logger.LogWarning("Database '{Id}' already exists — skipping clone.", id);
                }
            });

            await retry.ExecuteAsync(async () =>
            {
                await _tenant.ApplyPostCloneFixesAsync(id, Sanitise(_settings.TemplateSchemaName), ct);
            });

            await _tenant.SeedUserAsync(id, email, ct);

            var lic = await _license.ActivateAsync(id, email, ct);
            await _tenant.UpdateUserKeysAsync(id, email, lic.AuthKey!, lic.UserKey!, ct);

            _logger.LogInformation("Provisioning complete for '{Id}'.", id);
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Provisioning failed for '{Id}' — initiating rollback.", id);
            await _admin.CleanupAsync(id, ct);
            throw;
        }
    }

    private static string Sanitise(string input) =>
        new string(input.ToLower().Where(c => char.IsAsciiLetterOrDigit(c) || c == '_').ToArray());
}
