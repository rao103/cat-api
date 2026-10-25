using CAT.RawFanOut.Worker.DependencyInjection;
using CAT.RawFanOut.Worker.Hosting;
using CAT.RawFanOut.Worker.Observability;
using Microsoft.Extensions.Hosting;

var builder = Host.CreateApplicationBuilder(args);
builder.AddFanOutTelemetry();
var options = builder.AddWorkerConfiguration();
builder.Services.AddControlPlaneConfiguration(builder.Configuration);
builder.Services.AddFanOutWorkerServices(options, args);
await WorkerExecution.RunAsync(builder, options, args);
