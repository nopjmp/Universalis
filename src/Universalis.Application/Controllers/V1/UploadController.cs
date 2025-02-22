﻿using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.AspNetCore.Mvc;
using System.Reflection;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Universalis.Application.Swagger;
using Universalis.Application.Uploads.Attributes;
using Universalis.Application.Uploads.Behaviors;
using Universalis.Application.Uploads.Schema;
using Universalis.DbAccess.AccessControl;
using Universalis.DbAccess.Queries.Uploads;
using Universalis.DbAccess.Uploads;

namespace Universalis.Application.Controllers.V1;

[ApiController]
[ApiVersion("1")]
[Route("upload/{apiKey}")]
public class UploadController : ControllerBase
{
    private readonly ITrustedSourceDbAccess _trustedSourceDb;
    private readonly IFlaggedUploaderDbAccess _flaggedUploaderDb;
    private readonly IEnumerable<IUploadBehavior> _uploadBehaviors;
    private readonly ILogger<UploadController> _logger;

    public UploadController(
        ITrustedSourceDbAccess trustedSourceDb,
        IFlaggedUploaderDbAccess flaggedUploaderDb,
        IEnumerable<IUploadBehavior> uploadBehaviors,
        ILogger<UploadController> logger = null)
    {
        _trustedSourceDb = trustedSourceDb;
        _flaggedUploaderDb = flaggedUploaderDb;
        _uploadBehaviors = uploadBehaviors;
        _logger = logger;
    }

    [HttpPost]
    [ApiTag("Upload")]
    [ProducesResponseType(200)]
    [ProducesResponseType(400)]
    public async Task<IActionResult> Post(string apiKey, [FromBody] UploadParameters parameters, CancellationToken cancellationToken = default)
    {
        using var activity = Util.ActivitySource.StartActivity("UploadControllerV1.Post");

        var source = await _trustedSourceDb.Retrieve(new TrustedSourceQuery
        {
            ApiKeySha512 = await TrustedSourceHashCache.GetHash(apiKey, _trustedSourceDb, cancellationToken),
        }, cancellationToken);

        if (source is not { CanUpload: true })
        {
            return Forbid();
        }

        activity?.AddTag("source", source.Name);

        if (string.IsNullOrEmpty(parameters.UploaderId))
        {
            return BadRequest();
        }

        // Hash the uploader ID
        using (var sha256 = SHA256.Create())
        {
            parameters.UploaderId = Util.Hash(sha256, parameters.UploaderId);
        }

        var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(5));

        // Check if this uploader is flagged, cancel if they are
        if (await _flaggedUploaderDb.Retrieve(new FlaggedUploaderQuery { UploaderIdSha256 = parameters.UploaderId }, cts.Token) !=
            null)
        {
            return Ok("Success");
        }

        // Execute validators
        foreach (var uploadBehavior in _uploadBehaviors.Where(b => b.GetType().GetCustomAttribute<ValidatorAttribute>() != null))
        {
            if (!uploadBehavior.ShouldExecute(parameters)) continue;
            
            var actionResult = await uploadBehavior.Execute(source, parameters, cts.Token);
            if (actionResult != null)
            {
                return actionResult;
            }
        }
        
        // Execute other upload behaviors
        try
        {
            await Task.WhenAll(_uploadBehaviors
                .Where(b => b.GetType().GetCustomAttribute<ValidatorAttribute>() == null)
                .Where(b => b.ShouldExecute(parameters))
                .Select(b => b.Execute(source, parameters, cts.Token)));
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Exception caught in upload procedure");
        }

        return Ok("Success");
    }
}