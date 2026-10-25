using System.Globalization;
using Harmonizer.Core.Models;

namespace Harmonizer.Core.Serialization;

/// <summary>Groups immutable source candidates under their windfarm, WTG and business date.</summary>
public sealed class HarmonizedArtifactPathBuilder
{
    public string CreateComponentPrefix(ComponentLease lease, WindfarmIdentity windfarm, TurbineIdentity turbine)
    {
        if (windfarm.WindfarmId != lease.Key.WindfarmId || turbine.WtgId != lease.Key.WtgId ||
            windfarm.WindfarmId <= 0 || turbine.WtgId <= 0 || lease.Revision < 0 || lease.FenceToken <= 0)
        {
            throw new WorkValidationException("Artifact identities and revision must match the leased component.");
        }

        var farmName = EncodeSegment(windfarm.WindfarmName);
        var turbineName = EncodeSegment(turbine.WtgName);
        var source = EncodeSegment(lease.Key.InputType.ToLowerInvariant());
        var revision = checked(lease.Revision + 1);
        return string.Create(CultureInfo.InvariantCulture,
            $"harmonized/wtg/windfarm={farmName}/wtg={turbineName}/date={lease.Key.DataDate:yyyy-MM-dd}/{source}/revision={revision}/attempts/token={lease.FenceToken}");
    }

    private static string EncodeSegment(string value)
    {
        if (string.IsNullOrWhiteSpace(value) || value.Any(char.IsControl))
        {
            throw new WorkValidationException("Artifact partition names must be nonblank and contain no control characters.");
        }

        // Keep ordinary RDSPP names readable; reserved characters cannot introduce another folder.
        return Uri.EscapeDataString(value);
    }
}
