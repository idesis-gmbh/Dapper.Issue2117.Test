using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;

namespace Dapper.Issue2117.Test;

public interface IGenericService<TEntity, TKey>
{
    Task<TEntity> GetAsync(TKey id, CancellationToken token, bool commit = true);
    Task<IEnumerable<TEntity>> GetAsync(CancellationToken token, bool commit = true);
    Task<IEnumerable<TEntity>> GetAsync(int pageSize, int pageNo, CancellationToken token, bool commit = true);
    Task<IEnumerable<TEntity>> GetAsync(IEnumerable<TEntity> filter, CancellationToken token, bool commit = true);
    Task<TEntity> AddAsync(TEntity entity, CancellationToken token, bool commit = true);
    Task<IEnumerable<TEntity>> AddAsync(IEnumerable<TEntity> entities, CancellationToken token, bool commit = true);
    Task<TEntity> UpdateAsync(TEntity entity, CancellationToken token, bool commit = true);
    Task<IEnumerable<TEntity>> UpdateAsync(IEnumerable<TEntity> entities, CancellationToken token, bool commit = true);
    Task DeleteAsync(TEntity entity, CancellationToken token, bool commit = true);
    Task DeleteAsync(IEnumerable<TEntity> entities, CancellationToken token, bool commit = true);
}

public abstract class GenericService<IRepository, TEntity, TKey> : IGenericService<TEntity, TKey> where IRepository: IGenericRepository<TEntity, TKey> where TEntity : class, new() 
{
	protected readonly IDatabaseConnectionFactory _connectionFactory;
	protected readonly IRepository _repository;

	protected GenericService(
		IDatabaseConnectionFactory connectionFactory,
		IRepository repository)
	{
		_connectionFactory = connectionFactory;
		_repository = repository;
	}

	protected async Task<TResult> TransactionAsync<TResult>(Func<IDbTransaction, Task<TResult>> call,
		IsolationLevel isolationLevel = IsolationLevel.ReadCommitted, bool commit = true)
	{
		using var connection = await _connectionFactory.CreateConnectionAsync();
		using var transaction = connection.BeginTransaction(isolationLevel);
		try
		{
			TResult result = await call(transaction);
			if (commit)
				transaction.Commit();
			else
				transaction.Rollback();
			return result;
		}
		catch (Exception e)
		{
			transaction.Rollback();
			throw;
		}
	}
	protected async Task TransactionAsync(Func<IDbTransaction, Task> call, 
		IsolationLevel isolationLevel = IsolationLevel.ReadCommitted, bool commit = true)
	{
		using var connection = await _connectionFactory.CreateConnectionAsync();
		using var transaction = connection.BeginTransaction(isolationLevel);
		try
		{
			await call(transaction);
			if (commit)
				transaction.Commit();
			else
				transaction.Rollback();
		}
		catch (Exception e)
		{
			transaction.Rollback();
			throw;
		}
	}

	public virtual async Task<TEntity> GetAsync(TKey id, CancellationToken token, bool commit = true)
	{
		return await TransactionAsync(async tx =>
		{
			return await _repository.GetAsync(id, tx, token).ConfigureAwait(false);
		}, commit:commit).ConfigureAwait(false);
	}

	public virtual async Task<IEnumerable<TEntity>> GetAsync(CancellationToken token, bool commit = true)
	{
		return await TransactionAsync(async tx =>
		{
			return await _repository.GetAsync(tx, token).ConfigureAwait(false);
		}, commit:commit).ConfigureAwait(false);
	}

	public virtual async Task<IEnumerable<TEntity>> GetAsync(int pageSize, int pageNo, 
		CancellationToken token, bool commit = true)
	{
		return await TransactionAsync(async tx =>
		{
			return await _repository.GetAsync(pageSize, pageNo, tx, token).ConfigureAwait(false);
		}, commit:commit).ConfigureAwait(false);
	}

	public virtual async Task<IEnumerable<TEntity>> GetAsync(IEnumerable<TEntity> filter,
		CancellationToken token, bool commit = true)
	{
		return await TransactionAsync(async tx =>
		{
			return await _repository.GetAsync(filter, tx, token).ConfigureAwait(false);
		}, commit:commit).ConfigureAwait(false);
	}

	public virtual async Task<TEntity> AddAsync(TEntity entity, 
		CancellationToken token, bool commit = true)
	{
		return await TransactionAsync(async tx =>
		{
			return await _repository.AddAsync(entity, tx, token).ConfigureAwait(false);
		}, commit:commit).ConfigureAwait(false);
	}

	public virtual async Task<IEnumerable<TEntity>> AddAsync(IEnumerable<TEntity> entities,
		CancellationToken token, bool commit = true)
	{
		return await TransactionAsync(async tx =>
		{
			return await _repository.AddAsync(entities, tx, token).ConfigureAwait(false);
		}, commit:commit).ConfigureAwait(false);
	}

	public virtual async Task<TEntity> UpdateAsync(TEntity entity, 
		CancellationToken token, bool commit = true)
	{
		return await TransactionAsync(async tx =>
		{
			return await _repository.UpdateAsync(entity, tx, token).ConfigureAwait(false);
		}, commit:commit).ConfigureAwait(false);
	}

	public virtual async Task<IEnumerable<TEntity>> UpdateAsync(IEnumerable<TEntity> entities,
		CancellationToken token, bool commit = true)
	{
		return await TransactionAsync(async tx =>
		{
			return await _repository.UpdateAsync(entities, tx, token).ConfigureAwait(false);
		}, commit:commit).ConfigureAwait(false);
	}

	public virtual async Task DeleteAsync(TEntity entity, 
		CancellationToken token, bool commit = true)
	{
		await TransactionAsync(async tx =>
		{
			await _repository.DeleteAsync(entity, tx, token).ConfigureAwait(false);
		}, commit:commit).ConfigureAwait(false);
	}

	public virtual async Task DeleteAsync(IEnumerable<TEntity> entities, 
		CancellationToken token, bool commit = true)
	{
		await TransactionAsync(async tx =>
		{
			await _repository.DeleteAsync(entities, tx, token).ConfigureAwait(false);
		}, commit:commit).ConfigureAwait(false);
	}
}
