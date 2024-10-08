using System.Data;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using SqlKata.Compilers;

namespace Dapper.Issue2117.Test;

public interface IDatabaseConnectionFactory
{
	Task<IDbConnection> CreateConnectionAsync();
	Task<IDbTransaction> CreateTransactionAsync();
}

public interface INpgsqlDatabaseConnectionFactory : IDatabaseConnectionFactory
{
	Task<NpgsqlTransaction> CreateNpgsqlTransactionAsync();
}

public class PostgresConnectionFactory : INpgsqlDatabaseConnectionFactory
{
	private readonly string _connectionString;

	public PostgresConnectionFactory(string connectionString)
	{
		_connectionString = connectionString;
	}

	public async Task<IDbConnection> CreateConnectionAsync()
	{
		return await CreateNpgsqlConnectionAsync();
	}

	public async Task<IDbTransaction> CreateTransactionAsync()
	{
		NpgsqlConnection connection = await CreateNpgsqlConnectionAsync();
		return await connection.BeginTransactionAsync().ConfigureAwait(false);
	}

	public async Task<NpgsqlTransaction> CreateNpgsqlTransactionAsync()
	{
		NpgsqlConnection connection = await CreateNpgsqlConnectionAsync();
		return await connection.BeginTransactionAsync().ConfigureAwait(false);
	}

	private async Task<NpgsqlConnection> CreateNpgsqlConnectionAsync()
	{
		NpgsqlDataSourceBuilder dataSourceBuilder = new(_connectionString);
		NpgsqlMultiHostDataSource? datasource = dataSourceBuilder.BuildMultiHost();
		if (datasource is null)
			throw new NotSupportedException();
		NpgsqlConnection? connection = datasource.CreateConnection();
		if (connection is null)
			throw new NotSupportedException();
		await connection.OpenAsync().ConfigureAwait(false);
		if (connection.State != ConnectionState.Open)
			throw new NotSupportedException();
		return connection;
	}
}

public class SetupBase
{
	private const string DEFAULT_CONNECTION = "User Id=postgres;Password=password;Host=localhost:5432;Database=postgres;Include Error Detail=true";
	protected IDatabaseConnectionFactory? _connectionFactory;

	protected IHost OneTimeSetup()
	{
		IHost host = Host.CreateDefaultBuilder().ConfigureServices((services) =>
		{
			services
				.AddTransient<IChildRepository, ChildRepository>()
				.AddTransient<IParentRepository, ParentRepository>()
				.AddTransient<IParentService, ParentService>()
				.AddTransient<IDatabaseConnectionFactory>(serviceProvider =>
				{
					return serviceProvider.GetKeyedService<IDatabaseConnectionFactory>("PostgreSqlServiceIdentifier");
				})
				.AddKeyedTransient<IDatabaseConnectionFactory>("PostgreSqlServiceIdentifier", (serviceProvider, o) =>
				{
					return new PostgresConnectionFactory(DEFAULT_CONNECTION);
				})
				.AddSingleton<Compiler, PostgresCompiler>();
		}).Build();
		_connectionFactory = host.Services.GetRequiredService<IDatabaseConnectionFactory>();
		return host;
	}

	protected async Task<TResult> TransactionAsync<TResult>(Func<IDbTransaction, Task<TResult>> call,
		bool commit = false)
	{
		using var connection = await _connectionFactory!.CreateConnectionAsync();
		using var transaction = connection.BeginTransaction();
		try
		{
			TResult result = await call(transaction);
			if (commit)
				transaction.Commit();
			else
				transaction.Rollback();
			return result;
		}
		catch (Exception)
		{
			transaction.Rollback();
			throw;
		}
	}

	protected async Task TransactionAsync(Func<IDbTransaction, Task> call, bool commit = false)
	{
		using var connection = await _connectionFactory!.CreateConnectionAsync();
		using var transaction = connection.BeginTransaction();
		try
		{
			await call(transaction);
			if (commit)
				transaction.Commit();
			else
				transaction.Rollback();
		}
		catch (Exception)
		{
			transaction.Rollback();
			throw;
		}
	}
}
