using Harmonizer.Worker.DependencyInjection;
using Harmonizer.Worker.Observability;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = Host.CreateApplicationBuilder(args);
builder.AddHarmonizerTelemetry();
builder.Services.AddHarmonizer(builder.Configuration);
builder.Services.Configure<HostOptions>(options => options.ShutdownTimeout = TimeSpan.FromSeconds(45));
await builder.Build().RunAsync();
