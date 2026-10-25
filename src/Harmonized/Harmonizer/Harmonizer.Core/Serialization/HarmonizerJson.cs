using System.Text.Json;
using System.Text.Json.Serialization;
namespace Harmonizer.Core.Serialization;

public static class HarmonizerJson
{
    public static JsonSerializerOptions Options { get; } = new()
    {
        PropertyNameCaseInsensitive = false,
        NumberHandling = JsonNumberHandling.Strict,
        RespectNullableAnnotations = true,
        RespectRequiredConstructorParameters = true,
        MaxDepth = 32
    };
}
