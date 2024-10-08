// #define SYNC
#define CACHE

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using SqlKata;
using SqlKata.Compilers;

namespace Dapper.Issue2117.Test;

public class FallbackTypeMapper : SqlMapper.ITypeMap
{
    private readonly IEnumerable<SqlMapper.ITypeMap> _mappers;

    public FallbackTypeMapper(IEnumerable<SqlMapper.ITypeMap> mappers)
    {
        _mappers = mappers;
    }

    public ConstructorInfo? FindConstructor(string[] names, Type[] types)
    {
        foreach (var mapper in _mappers)
        {
            ConstructorInfo result = mapper.FindConstructor(names, types);
            if (result != null)
                return result;
        }
        return null;
    }

    public SqlMapper.IMemberMap? GetConstructorParameter(ConstructorInfo constructor, string columnName)
    {
        foreach (var mapper in _mappers)
        {
            var result = mapper.GetConstructorParameter(constructor, columnName);
            if (result != null)
                return result;
        }
        return null;
    }

    public SqlMapper.IMemberMap? GetMember(string columnName)
    {
        foreach (var mapper in _mappers)
        {
            var result = mapper.GetMember(columnName);
            if (result != null)
                return result;
        }
        return null;
    }

    public ConstructorInfo? FindExplicitConstructor()
    {
        return _mappers
            .Select(mapper => mapper.FindExplicitConstructor())
            .FirstOrDefault(result => result != null);
    }
}

public class ColumnAttributeTypeMapper<T> : FallbackTypeMapper
{
    public ColumnAttributeTypeMapper()
        : base(new SqlMapper.ITypeMap[]
        {
            new CustomPropertyTypeMap(
                typeof(T),
                (type, columnName) =>
                    type.GetProperties().FirstOrDefault(prop =>
                        prop.GetCustomAttributes(false)
                            .OfType<ColumnAttribute>()
                            .Any(attr => attr.Name == columnName)
                    )
            ),
            new DefaultTypeMap(typeof(T))
        })
    {
    }
}

public interface IGenericRepository<TEntity, TKey>
{
	string IdName { get; }

	Task<TEntity> GetAsync(TKey id, IDbTransaction transaction, CancellationToken token);
	Task<IEnumerable<TEntity>> GetAsync(IDbTransaction transaction, CancellationToken token);
	Task<IEnumerable<TEntity>> GetAsync(int pageSize, int pageNo, IDbTransaction transaction, CancellationToken token);
	Task<IEnumerable<TEntity>> GetAsync(IEnumerable<TEntity> filter, IDbTransaction transaction, CancellationToken token);
	Task<TEntity> AddAsync(TEntity entity, IDbTransaction transaction, CancellationToken token);
	Task<IEnumerable<TEntity>> AddAsync(IEnumerable<TEntity> entities, IDbTransaction transaction, CancellationToken token);
	Task<TEntity> UpdateAsync(TEntity entity, IDbTransaction transaction, CancellationToken token);
	Task<IEnumerable<TEntity>> UpdateAsync(IEnumerable<TEntity> entities, IDbTransaction transaction, CancellationToken token);
	Task DeleteAsync(TEntity entity, IDbTransaction transaction, CancellationToken token);
	Task DeleteAsync(IEnumerable<TEntity> entities, IDbTransaction transaction, CancellationToken token);
	Task DeleteAsync(IDbTransaction transaction, CancellationToken token);
}

public abstract class GenericRepository<TEntity, TKey> where TEntity : class
{
	protected readonly Compiler _compiler;
    protected readonly string _tableName;
    protected readonly string _idName;
    protected readonly string _pkName;
    private static readonly SemaphoreSlim _semaphore = new SemaphoreSlim(1, 1);

	public GenericRepository(Compiler compiler, string tableName, string idName="Id", string pkName="id")
	{
		_compiler = compiler;
		_tableName = tableName;
        _idName = idName;
        _pkName = pkName;
	}

	public virtual string IdName => _idName;

    static GenericRepository()
    {
        Dapper.SqlMapper.SetTypeMap(
            typeof(TEntity),
            new ColumnAttributeTypeMapper<TEntity>());
    }

    protected CommandDefinition CompileQuery(Query query, IDbTransaction transaction, 
        CancellationToken token, CommandFlags flags = CommandFlags.Buffered)
    {
        var statement = _compiler.Compile(query).ToString();
        return new CommandDefinition(statement, transaction:transaction, 
            cancellationToken:token, flags:flags);
    } 

    protected async Task<IEnumerable<TEntity>> QueryAsync(Query query, IDbTransaction transaction, CancellationToken token)
    {
#if SYNC        
        await _semaphore.WaitAsync().ConfigureAwait(false);
#endif
        try
        {
            var command = CompileQuery(query, transaction, token, 
#if CACHE            
                CommandFlags.Buffered);
#else                
                CommandFlags.Buffered | CommandFlags.NoCache);
#endif                
            return await transaction.Connection!.QueryAsync<TEntity>(command).ConfigureAwait(false);
        }
        finally
        {
#if SYNC        
            SqlMapper.PurgeQueryCache();
            _semaphore.Release();
#endif
        }
    }

    protected async Task<IEnumerable<TEntity>> QueryAsync<TFirst, TSecond>(Query query, IDbTransaction transaction, CancellationToken token,
        Func<TFirst, TSecond, TEntity> map, string splitOn = "id")
    {
#if SYNC        
        await _semaphore.WaitAsync().ConfigureAwait(false);
#endif
        try
        {
            var command = CompileQuery(query, transaction, token, 
#if CACHE            
                CommandFlags.Buffered);
#else                
                CommandFlags.Buffered | CommandFlags.NoCache);
#endif                
            return await transaction.Connection!.QueryAsync(command, map, splitOn).ConfigureAwait(false);
        }
        finally
        {
#if SYNC        
            SqlMapper.PurgeQueryCache();
            _semaphore.Release();
#endif
        }
    }

    private async Task<int> ExecuteAsync(Query query, IDbTransaction transaction, CancellationToken token)
    {
#if SYNC        
        await _semaphore.WaitAsync().ConfigureAwait(false);
#endif
        try
        {
            var command = CompileQuery(query, transaction, token, 
#if CACHE            
                CommandFlags.Buffered);
#else                
                CommandFlags.Buffered | CommandFlags.NoCache);
#endif                
            return await transaction.Connection!.ExecuteAsync(command).ConfigureAwait(false);
        }
        finally
        {
#if SYNC        
            SqlMapper.PurgeQueryCache();
            _semaphore.Release();
#endif
        }
    }

    protected virtual async Task<IEnumerable<TEntity>> MapAsync(Func<Query, Query> map, IDbTransaction transaction,
		CancellationToken token)
	{
		var query = map(new Query(_tableName));
        return await QueryAsync(query, transaction, token).ConfigureAwait(false);
	}

	public virtual async Task<TEntity> GetAsync(TKey id, IDbTransaction transaction,
		CancellationToken token)
	{
        return (await MapAsync(query => query.Where(_tableName + "." + _pkName, id), transaction, token).ConfigureAwait(false)).Single();
	}

    public virtual async Task<IEnumerable<TEntity>> GetAsync(IDbTransaction transaction, CancellationToken token)
    {
        return await MapAsync(query => query, transaction, token).ConfigureAwait(false);
    }

    public virtual async Task<IEnumerable<TEntity>> GetAsync(int pageSize, int pageNo, IDbTransaction transaction, CancellationToken token)
    {
        return await MapAsync(query => query.Limit(pageSize).Offset(pageNo), transaction, token).ConfigureAwait(false);
    }

	public virtual async Task<IEnumerable<TEntity>> GetAsync(IEnumerable<TEntity> filter,
		IDbTransaction transaction, CancellationToken token)
	{
        return await MapAsync(query => query.WhereIn(_tableName + "." + _pkName, 
            filter.Select(entity => entity.GetType().GetProperty(_idName)!.GetValue(entity))), transaction, token).ConfigureAwait(false);
	}

    public virtual async Task<TEntity> AddAsync(TEntity entity, 
        IDbTransaction transaction, CancellationToken token)
    {
        var query = new Query(_tableName).AsInsert(entity);
        var result = await ExecuteAsync(query, transaction, token).ConfigureAwait(false);
        if(result != 1)
            throw new InvalidOperationException();
        var id = entity.GetType().GetProperty(_idName)!.GetValue(entity)!;
        return await GetAsync((TKey)id, transaction, token).ConfigureAwait(false);
    }

    public virtual async Task<IEnumerable<TEntity>> AddAsync(IEnumerable<TEntity> entities,
        IDbTransaction transaction, CancellationToken token)
    {
		var result = 0;
		foreach(var entity in entities)
		{
            var query = new Query(_tableName).AsInsert(entity);
            result += await ExecuteAsync(query, transaction, token).ConfigureAwait(false);
		}
        if(result != entities.Count())
            throw new InvalidOperationException();
        return await GetAsync(entities, transaction, token).ConfigureAwait(false);
    }

	public virtual async Task<TEntity> UpdateAsync(TEntity entity, 
        IDbTransaction transaction, CancellationToken token)
	{
        var query = new Query(_tableName).Where(_pkName,
            entity.GetType().GetProperty(_idName)!.GetValue(entity)).AsUpdate(entity);
        var result = await ExecuteAsync(query, transaction, token).ConfigureAwait(false);
        if(result != 1)
            throw new InvalidOperationException();
        var id = entity.GetType().GetProperty(_idName)!.GetValue(entity)!;
        return await GetAsync((TKey)id, transaction, token).ConfigureAwait(false);
	}

	public virtual async Task<IEnumerable<TEntity>> UpdateAsync(IEnumerable<TEntity> entities,
		IDbTransaction transaction, CancellationToken token)
	{
		var result = 0;
		foreach(var entity in entities)
		{       
            var query = new Query(_tableName).Where(_pkName, 
                entity.GetType().GetProperty(_idName)!.GetValue(entity)).AsUpdate(entity);
            result += await ExecuteAsync(query, transaction, token).ConfigureAwait(false);
		}
        if(result != entities.Count())
            throw new InvalidOperationException();
        return await GetAsync(entities, transaction, token).ConfigureAwait(false);
	}

	public virtual async Task DeleteAsync(TEntity entity, 
        IDbTransaction transaction, CancellationToken token)
	{
        var query = new Query(_tableName).Where(_pkName, 
            entity.GetType().GetProperty(_idName)!.GetValue(entity)).AsDelete();
        var result = await ExecuteAsync(query, transaction, token).ConfigureAwait(false);
        if(result != 1)
            throw new InvalidOperationException();
	}

	public virtual async Task DeleteAsync(IEnumerable<TEntity> entities, 
        IDbTransaction transaction, CancellationToken token)
	{
		var result = 0;
		foreach(var entity in entities)
		{
            var query = new Query(_tableName).Where(_pkName, 
                entity.GetType().GetProperty(_idName)!.GetValue(entity)).AsDelete();
            result += await ExecuteAsync(query, transaction, token).ConfigureAwait(false);
		}
        if(result != entities.Count())
            throw new InvalidOperationException();
	}

	public virtual async Task DeleteAsync(IDbTransaction transaction, CancellationToken token)
	{
        var count = (await GetAsync(transaction, token).ConfigureAwait(false)).Count();
        var query = new Query(_tableName).AsDelete();
        var result = await ExecuteAsync(query, transaction, token).ConfigureAwait(false);
        if(result != count)
            throw new InvalidOperationException();
	}
}
