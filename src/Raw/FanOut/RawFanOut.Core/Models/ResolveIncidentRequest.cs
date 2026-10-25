using System;

namespace CAT.RawFanOut.Models;

// Operator is an asserted name (Kubernetes RBAC controls who can run the job), not an authenticated identity.
public sealed record ResolveIncidentRequest(Guid IncidentId, string Reason, string Operator);
