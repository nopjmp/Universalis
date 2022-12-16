using Microsoft.OpenApi.Models;
using Swashbuckle.AspNetCore.SwaggerGen;
using System;

namespace Universalis.Application.Swagger;

public class ReplaceVersionWithExactFilter : IDocumentFilter
{
    public void Apply(OpenApiDocument swaggerDoc, DocumentFilterContext context)
    {
        if (swaggerDoc == null)
        {
            throw new ArgumentNullException(nameof(swaggerDoc));
        }
        
        var paths = new OpenApiPaths();
        foreach (var (k, v) in swaggerDoc.Paths)
        {
            paths[k.Replace("v{version}", swaggerDoc.Info.Version)] = v;
        }

        swaggerDoc.Paths = paths;
    }
}