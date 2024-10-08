using System.Data;
using Microsoft.Extensions.Logging;

namespace Dapper.Issue2117.Test;

public interface IParentService : IGenericService<ParentEntity, Guid>
{
}

public class ParentService : GenericService<IParentRepository, ParentEntity, Guid>, IParentService
{
	private readonly IChildRepository _childRepository;

	public ParentService(
		IChildRepository childRepository,
		IParentRepository repository,
		IDatabaseConnectionFactory connectionFactory) :
		base(connectionFactory, repository)
	{
		_childRepository = childRepository;
	}

	public override async Task<ParentEntity> AddAsync(ParentEntity entity,
		CancellationToken token, bool commit = true)
	{
		return await TransactionAsync(async tx =>
		{
			entity.Id = Guid.NewGuid();
			var children = entity.Children;
			entity = await base.AddAsync(entity, token, commit: commit).ConfigureAwait(false);
			foreach(var child in children) 
			{
				child.Id = Guid.NewGuid();
				child.ParentId = entity.Id;
			}
			entity.Children = (await _childRepository.AddAsync(children, tx, token).ConfigureAwait(false)).ToList();
			return entity;
		}, commit:commit).ConfigureAwait(false);
	}

	public override async Task<ParentEntity> UpdateAsync(ParentEntity entity,
		CancellationToken token, bool commit = true)
	{
		return await TransactionAsync(async tx =>
		{
			var current = await _repository.GetAsync(entity.Id, tx, token).ConfigureAwait(false);
			var children = entity.Children.Where(es => !current.Children.Any(cs => cs.Id == es.Id));
			foreach(var child in children) 
			{
				child.Id = Guid.NewGuid();
				child.ParentId = entity.Id;
			}
			_ = await _childRepository.AddAsync(children, tx, token).ConfigureAwait(false);
			await _childRepository.DeleteAsync(current.Children.Where(cs => !entity.Children.Any(es => es.Id == cs.Id)), tx, token).ConfigureAwait(false);
			entity.Children = (await _childRepository.UpdateAsync(entity.Children, tx, token).ConfigureAwait(false)).ToList();
			return await _repository.UpdateAsync(entity, tx, token).ConfigureAwait(false);
		}, commit:commit).ConfigureAwait(false);
	}
}
