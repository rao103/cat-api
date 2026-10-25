using System.Security.Claims;

namespace Centralize.Api.Security;

internal static class EntraClaims
{
    private const string ObjectIdentifierClaim = "http://schemas.microsoft.com/identity/claims/objectidentifier";

    public static string? GetObjectId(this ClaimsPrincipal principal) =>
        principal.FindFirstValue("oid") ?? principal.FindFirstValue(ObjectIdentifierClaim);

    public static string? GetCallerId(this ClaimsPrincipal principal) =>
        principal.GetObjectId()
        ?? principal.FindFirstValue("sub")
        ?? principal.FindFirstValue(ClaimTypes.NameIdentifier);
}