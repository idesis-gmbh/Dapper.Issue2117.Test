using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace Dapper.Issue2117.Test;

[TestFixture]
public class ParentRepositoryTests : SetupBase
{
	private static IHost _host;
	private static IParentRepository _repository;

	[OneTimeSetUp]
	public void OneTimeSetup()
	{
		_host = base.OneTimeSetup();
	}

	[OneTimeTearDown]
	public void OneTimeTearDown()
	{
		_host.Dispose();
	}

	[SetUp]
	public void Setup()
	{
		using var scope = _host.Services.CreateScope();
		_repository = scope.ServiceProvider.GetRequiredService<IParentRepository>();
	}

	[Test]
	public async Task TestUpdateManyAsync()
	{
		await TransactionAsync(async tx =>
		{
			var token = new CancellationToken();
			var entities = await _repository.GetAsync(tx, token).ConfigureAwait(false);
			Assert.That(entities.GetType() == typeof(List<ParentEntity>));
			var result = await _repository.UpdateAsync(entities, tx, token).ConfigureAwait(false);
			Assert.That(result.Count() == entities.Count());
		});
	}
}
