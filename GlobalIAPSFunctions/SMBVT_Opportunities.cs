using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Helper;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using System;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;
using System.IO;
using Microsoft.AspNetCore.Http;
namespace GlobalIAPSFunctions
{
    public static class SMBVT_Opportunities
    {
        [FunctionName("SMBVT_Opportunities")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            DateTime StartTime = DateTime.UtcNow;
            log.LogInformation("C# HTTP trigger function processed a request.");

            SSASRevenueDetailsManager crmManager = null;
            string Instancename = System.Environment.GetEnvironmentVariable("Instancename");
            string ClientId = System.Environment.GetEnvironmentVariable("ClientId");
            string ClientSecret = System.Environment.GetEnvironmentVariable("ClientSecret");
            string sqlConnection = System.Environment.GetEnvironmentVariable("sqlConnection");
            string ssasServer = System.Environment.GetEnvironmentVariable("ssasServer");
            string MSXClientId = System.Environment.GetEnvironmentVariable("MSXClientId");
            string MSXClientSecret = System.Environment.GetEnvironmentVariable("MSXClientSecret");
            string blobConnection = System.Environment.GetEnvironmentVariable("blobConnection");
            string vtcpManagedIdentity = System.Environment.GetEnvironmentVariable("VTCPManagedIdentity");
            crmManager = new SSASRevenueDetailsManager(Instancename, ClientId, ClientSecret, MSXClientId, MSXClientSecret, sqlConnection, ssasServer, blobConnection,vtcpManagedIdentity, log);

            crmManager.InsertSSASCubeData("[dbo].[SMBVT_Opportunities]",StartTime);

            await Dispose(log);
            string name = req.Query["name"];

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);
            name = name ?? data?.name;

            string responseMessage = string.IsNullOrEmpty(name)
                ? "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response."
                : $"Hello, {name}. This HTTP triggered function executed successfully.";

            return new OkObjectResult(responseMessage);
        }

        public static async Task Dispose(ILogger log)
        {
            await Task.Run(() =>
            {
                for (int i = 0; i < 100; i++)
                {
                    log.LogInformation(" Update SMBVT_Opportunities method is successfully executed");
                }
            });
        }
    }
}
