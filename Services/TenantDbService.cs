using Microsoft.Extensions.Options;
using Npgsql;
using RmqConsumerService.Configuration;
using RmqConsumerService.Services.Interfaces;

namespace RmqConsumerService.Services;

public sealed class TenantDbService : ITenantDbService
{
    private readonly DatabaseSettings _settings;
    private readonly ILogger<TenantDbService> _logger;

    public TenantDbService(IOptions<DatabaseSettings> settings, ILogger<TenantDbService> logger)
    {
        _settings = settings.Value;
        _logger = logger;
    }

    public async Task ApplyPostCloneFixesAsync(string dbName, string templateSchema, CancellationToken ct)
    {
        // Dynamic conn string — cannot use a pre-built DataSource
        await using var conn = new NpgsqlConnection(_settings.BuildConnectionString(dbName));
        await conn.OpenAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);

        try
        {
            await using var setupCmd = new NpgsqlCommand($$"""
                ALTER SCHEMA "{{templateSchema}}" RENAME TO "{{dbName}}";
                REASSIGN OWNED BY "{{templateSchema}}" TO "{{dbName}}";
                ALTER SCHEMA "{{dbName}}" OWNER TO "{{dbName}}";
                ALTER SCHEMA public OWNER TO "{{dbName}}";
                GRANT ALL PRIVILEGES ON DATABASE "{{dbName}}" TO "{{dbName}}";
                GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA "{{dbName}}" TO "{{dbName}}";
                GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA "{{dbName}}" TO "{{dbName}}";
                GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA "{{dbName}}" TO "{{dbName}}";
                ALTER DEFAULT PRIVILEGES IN SCHEMA "{{dbName}}" GRANT ALL ON TABLES    TO "{{dbName}}";
                ALTER DEFAULT PRIVILEGES IN SCHEMA "{{dbName}}" GRANT ALL ON SEQUENCES TO "{{dbName}}";
                ALTER DEFAULT PRIVILEGES IN SCHEMA "{{dbName}}" GRANT ALL ON FUNCTIONS TO "{{dbName}}";
                ALTER DATABASE "{{dbName}}" SET search_path TO "{{dbName}}", public;
                SET search_path TO "{{dbName}}", public;
                """, conn, tx);
            await setupCmd.ExecuteNonQueryAsync(ct);

            // Function body rewrite (only if hardcoded refs exist)
            await using var rewriteCmd = new NpgsqlCommand($$"""
                DO $$
                DECLARE r RECORD; v_def TEXT;
                BEGIN
                    FOR r IN SELECT oid FROM pg_proc
                        WHERE pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = '{{dbName}}')
                    LOOP
                        v_def := pg_get_functiondef(r.oid);
                        IF v_def LIKE '%{{templateSchema}}.%' THEN
                            EXECUTE replace(v_def, '{{templateSchema}}.', '{{dbName}}.');
                        END IF;
                    END LOOP;
                END $$;
                """, conn, tx);
            await rewriteCmd.ExecuteNonQueryAsync(ct);

            await tx.CommitAsync(ct);
            _logger.LogInformation("Post-clone fixes committed for '{Db}'.", dbName);
        }
        catch
        {
            await tx.RollbackAsync(ct);
            _logger.LogError("Post-clone fixes rolled back for '{Db}'.", dbName);
            throw;
        }
    }

    public async Task SeedUserAsync(string dbName, string email, CancellationToken ct)
    {
        await using var conn = new NpgsqlConnection(_settings.BuildConnectionString(dbName));
        await conn.OpenAsync(ct);

        var nickname = email.Contains('@') ? email[..email.IndexOf('@')] : email;
        await using var cmd = new NpgsqlCommand($"SELECT \"{dbName}\".setup_new_user(@u, @e, @n)", conn);
        cmd.Parameters.AddWithValue("u", email);
        cmd.Parameters.AddWithValue("e", email);
        cmd.Parameters.AddWithValue("n", nickname);
        await cmd.ExecuteNonQueryAsync(ct);
        _logger.LogInformation("Seed user created in '{Db}'.", dbName);
    }

    public async Task UpdateUserKeysAsync(string dbName, string email, string authKey, string userKey, CancellationToken ct)
    {
        await using var conn = new NpgsqlConnection(_settings.BuildConnectionString(dbName));
        await conn.OpenAsync(ct);

        await using var cmd = new NpgsqlCommand(
            $"UPDATE \"{dbName}\".axusers SET authkey = @ak, userkey = @uk WHERE email = @u", conn);
        cmd.Parameters.AddWithValue("ak", authKey);
        cmd.Parameters.AddWithValue("uk", userKey);
        cmd.Parameters.AddWithValue("u", email);

        var affected = await cmd.ExecuteNonQueryAsync(ct);
        if (affected == 0)
            throw new InvalidOperationException($"User '{email}' not found in {dbName}.axusers — keys not updated.");
    }
}
