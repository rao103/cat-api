using Centralize.Api.Security;
using Centralize.Core.Interfaces;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace Centralize.Api.Controllers.v1;

[ApiController]
[ApiVersion("1.0")]
[Route("v{version:apiVersion}/operations")]
[Authorize]
public sealed class OperationsController(IOperationRepository repository) : ControllerBase
{
    [HttpGet("{operationId:guid}")]
    public async Task<IActionResult> Get(Guid operationId, CancellationToken cancellationToken)
    {
        var caller = User.GetObjectId();
        if (string.IsNullOrWhiteSpace(caller)) return StatusCode(403);
        if (operationId == Guid.Empty) return BadRequest(new { Error = "OperationId is required." });
        var operation = await repository.GetAsync(operationId, caller, cancellationToken);
        return operation is null ? NotFound() : Ok(operation);
    }
}
