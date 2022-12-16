using MongoDB.Bson.Serialization.Attributes;
using System.Collections.Generic;

namespace Universalis.Entities.Uploads;

public class UploadCountHistory
{
    [BsonElement("lastPush")]
    public double LastPush { get; set; }

    [BsonElement("uploadCountByDay")]
    public List<double> UploadCountByDay { get; set; }
}