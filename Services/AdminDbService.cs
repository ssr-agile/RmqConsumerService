using Microsoft.Extensions.Options;
using Npgsql;
using RmqConsumerService.Configuration;
using RmqConsumerService.Services.Interfaces;

namespace RmqConsumerService.Services;

public sealed class AdminDbService : IAdminDbService
{
    private readonly NpgsqlDataSource _dataSource;   // injected singleton
    private readonly DatabaseSettings _settings;
    private readonly ILogger<AdminDbService> _logger;

    public AdminDbService(NpgsqlDataSource dataSource, IOptions<DatabaseSettings> settings, ILogger<AdminDbService> logger)
    {
        _dataSource = dataSource;
        _settings = settings.Value;
        _logger = logger;
    }

    public async Task<bool> DatabaseExistsAsync(string dbName, CancellationToken ct)
    {
        await using var cmd = _dataSource.CreateCommand("SELECT 1 FROM pg_database WHERE datname = $1");
        cmd.Parameters.AddWithValue(dbName);
        return await cmd.ExecuteScalarAsync(ct) is not null;
    }
    // Note: NpgsqlDataSource.CreateCommand() handles open/close internally — no conn.OpenAsync() needed

    public async Task EnsureRoleAsync(string roleName, string password, CancellationToken ct)
    {
        // DO $$ can't use parameters for identifiers — sanitised upstream
        await using var cmd = _dataSource.CreateCommand($"""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '{roleName}') THEN
                    CREATE ROLE "{roleName}" LOGIN PASSWORD '{password}';
                END IF;
            END $$;
            """);
        await cmd.ExecuteNonQueryAsync(ct);
        _logger.LogInformation("Role '{Role}' ensured.", roleName);
    }

    public async Task TerminateConnectionsAsync(string dbName, CancellationToken ct)
    {
        await using var cmd = _dataSource.CreateCommand("""
            SELECT COUNT(pg_terminate_backend(pid))
            FROM   pg_stat_activity
            WHERE  datname = $1
              AND  pid <> pg_backend_pid()
              AND  usename NOT IN ('maintenance_admin', 'backup_service', 'readonly_dev')
            """);
        cmd.Parameters.AddWithValue(dbName);
        var killed = Convert.ToInt64(await cmd.ExecuteScalarAsync(ct) ?? 0L);
        if (killed > 0)
            _logger.LogWarning("Terminated {N} connection(s) on '{Db}' before clone.", killed, dbName);
    }

    public async Task CloneDatabaseAsync(string dbName, string template, CancellationToken ct)
    {
        // CREATE DATABASE must run outside a transaction — NpgsqlDataSource handles this fine
        await using var cmd = _dataSource.CreateCommand(
            $"CREATE DATABASE \"{dbName}\" TEMPLATE \"{template}\" OWNER = \"{dbName}\"");
        cmd.CommandTimeout = 300;
        await cmd.ExecuteNonQueryAsync(ct);
        _logger.LogInformation("Database '{Db}' cloned from '{Template}'.", dbName, template);
    }

    public async Task SetDatabaseOwnerAsync(string dbName, string owner, CancellationToken ct)
    {
        await using var cmd = _dataSource.CreateCommand(
            $"ALTER DATABASE \"{dbName}\" OWNER TO \"{owner}\"");
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task CleanupAsync(string identifier, CancellationToken ct)
    {
        try
        {
            _logger.LogWarning("Rolling back — dropping DB and role for '{Id}'.", identifier);
            //await TerminateConnectionsAsync(identifier, ct);

            //await using var dropDb = _dataSource.CreateCommand($"DROP DATABASE IF EXISTS \"{identifier}\"");
            //await dropDb.ExecuteNonQueryAsync(ct);

            //await using var dropRole = _dataSource.CreateCommand($"DROP ROLE IF EXISTS \"{identifier}\"");
            //await dropRole.ExecuteNonQueryAsync(ct);
        }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, "CRITICAL: Manual cleanup required for '{Id}'.", identifier);
        }
    }
}
