using SqlKata;
using SqlKata.Compilers;

namespace Dapper.Issue2117.Test;

public class ChildEntity
{
    [Column("id")] public Guid Id { get; set; }

    [Column("parent_id")] public Guid ParentId { get; set; }
}

public interface IChildRepository: IGenericRepository<ChildEntity, Guid>
{
}

public class ChildRepository : GenericRepository<ChildEntity, Guid>, IChildRepository
{
	public ChildRepository(Compiler compiler)
		: base(compiler, "child", pkName:"id")
	{
	}
}
