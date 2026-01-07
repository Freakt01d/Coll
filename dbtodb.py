public class RegTaxSearchResponse
{
    [JsonProperty("bdrId")]
    public string BdrId { get; set; }

    [JsonProperty("sftrData")]
    public List<SftrData> SftrData { get; set; }
}

public class SftrData
{
    [JsonProperty("category")]
    public string Category { get; set; }
}
