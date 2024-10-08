using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;

namespace Dapper.Issue2117.Test;

[TestFixture]
public class ParentServiceTests: SetupBase
{
	private IHost _host;
	private static IParentService _service;

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
		_service = scope.ServiceProvider.GetRequiredService<IParentService>();
	}

	[Test]
	public async Task TestUpdateOneAsync()
	{
		var token = new CancellationToken();
		var entity = (await _service.GetAsync(token, commit: false).ConfigureAwait(false))
			.Where(k => k.Children.Any()).First();
		var result = await _service.UpdateAsync(entity, token, commit: false).ConfigureAwait(false);
		Assert.That(result!.GetType() == typeof(ParentEntity));
	}
}
