using Newtonsoft.Json;
using System.Collections.Generic;

public class MandateDetailsResponse
{
    [JsonProperty("mandateDetails")]
    public List<MandateDetail> MandateDetails { get; set; }

    [JsonProperty("principalEtpBdrId")]
    public string PrincipalEtpBdrId { get; set; }

    [JsonProperty("principalCompanyId")]
    public string PrincipalCompanyId { get; set; }
}

public class MandateDetail
{
    [JsonProperty("bdrid")]
    public string BdrId { get; set; }

    [JsonProperty("name")]
    public string Name { get; set; }

    [JsonProperty("taskOwnedBy")]
    public string TaskOwnedBy { get; set; }

    [JsonProperty("mandateSuffix")]
    public string MandateSuffix { get; set; }

    [JsonProperty("agentCompanyId")]
    public string AgentCompanyId { get; set; }

    [JsonProperty("custodianId")]
    public string CustodianId { get; set; }

    [JsonProperty("dmerId")]
    public string DmerId { get; set; }

    [JsonProperty("bdrMnemonic")]
    public string BdrMnemonic { get; set; }

    [JsonProperty("type")]
    public string Type { get; set; }

    [JsonProperty("subType")]
    public string SubType { get; set; }

    [JsonProperty("followUpClass")]
    public Dictionary<string, object> FollowUpClass { get; set; }

    [JsonProperty("key")]
    public string Key { get; set; }

    [JsonProperty("value")]
    public string Value { get; set; }
}
