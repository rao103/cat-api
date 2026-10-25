using System;

namespace CAT.RawFanOut.IntegrationTests;
public sealed class ExternalIntegrationFactAttribute : FactAttribute
{
    public ExternalIntegrationFactAttribute()
    {
        if(Environment.GetEnvironmentVariable("RAWFANOUT_INTEGRATION")!="1")
            Skip="External integration tests require RAWFANOUT_INTEGRATION=1 and isolated local services. See README.";
    }
}
