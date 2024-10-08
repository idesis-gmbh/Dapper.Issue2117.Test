using System.Data;
using Dapper;
using SqlKata;
using SqlKata.Compilers;

namespace Dapper.Issue2117.Test;

public class ParentEntity
{
    [Column("id")] public Guid Id { get; set; }

    [SqlKata.Ignore] public virtual IList<ChildEntity> Children { get; set; } = new List<ChildEntity>();
}

public interface IParentRepository : IGenericRepository<ParentEntity, Guid>
{
}

public class ParentRepository : GenericRepository<ParentEntity, Guid>, IParentRepository
{
	public ParentRepository(Compiler compiler)
		: base(compiler, "parent", pkName: "id")
	{
	}

    protected override async Task<IEnumerable<ParentEntity>> MapAsync(Func<Query, Query> map, IDbTransaction transaction, CancellationToken token)
    {
        var query = new Query(_tableName)
			.LeftJoin("child", "parent.id", "child.parent_id");
		var parents = await QueryAsync<ParentEntity, ChildEntity>(
			map(query), transaction, token,
			(parent, child) => { 
				if (child != null)
					parent.Children.Add(child);
				return parent; 
			}).ConfigureAwait(false);
		return parents.GroupBy(p => p.Id).Select(g => { 
			var p = g.First(); 
			p.Children = g.SelectMany(p => p.Children).ToList();
			return p;
		}).ToList();
    }
}
