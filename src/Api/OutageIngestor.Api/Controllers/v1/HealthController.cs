using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace OutageIngestor.Api.Controllers.v1;

[ApiController]
[Route("v{version:apiVersion}/health")]
[ApiVersion("1.0")]
public class HealthController : ControllerBase
{
    [HttpGet]
    [AllowAnonymous]
    public IActionResult Get() => Ok(new { Status = "Healthy", HostName = Environment.MachineName });
}
