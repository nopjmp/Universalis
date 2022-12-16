﻿using Cassandra;
using Cassandra.Data.Linq;
using Cassandra.Mapping;
using System;
using System.Threading;
using System.Threading.Tasks;
using Universalis.Entities.Uploads;

namespace Universalis.DbAccess.Uploads;

public class FlaggedUploaderStore : IFlaggedUploaderStore
{
    private readonly IMapper _mapper;

    public FlaggedUploaderStore(ICluster cluster)
    {
        var scylla = cluster.Connect();
        scylla.CreateKeyspaceIfNotExists("flagged_uploader");
        scylla.ChangeKeyspace("flagged_uploader");
        var table = scylla.GetTable<FlaggedUploader>();
        table.CreateIfNotExists();

        _mapper = new Mapper(scylla);
    }

    public Task Insert(FlaggedUploader uploader, CancellationToken cancellationToken = default)
    {
        if (uploader == null)
        {
            throw new ArgumentNullException(nameof(uploader));
        }

        return _mapper.InsertAsync(uploader);
    }

    public Task<FlaggedUploader> Retrieve(string uploaderIdSha256, CancellationToken cancellationToken = default)
    {
        if (uploaderIdSha256 == null)
        {
            throw new ArgumentNullException(nameof(uploaderIdSha256));
        }

        return _mapper.FirstOrDefaultAsync<FlaggedUploader>("SELECT * FROM flagged_uploader WHERE id_sha256=?", uploaderIdSha256);
    }
}