using Helper;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Net.Http;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;
using System.IO;

namespace GlobalIAPSFunctions
{
    public static class UpdateAutoLeadCreationRequestStatus
    {
        [FunctionName("UpdateAutoLeadCreationRequestStatus")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);
            string RequestID = data?.RequestID;
            log.LogInformation("C# HTTP trigger function processed a request.");

            AutoLeadCreationManager crmManager = null;
            string Instancename = System.Environment.GetEnvironmentVariable("Instancename");//vendortelecp
            string ClientId = System.Environment.GetEnvironmentVariable("ClientId");
            string ClientSecret = System.Environment.GetEnvironmentVariable("ClientSecret");
            string sqlConnection = System.Environment.GetEnvironmentVariable("sqlConnection");
            string ssasServer = System.Environment.GetEnvironmentVariable("ssasServer");
            string MSXClientId = System.Environment.GetEnvironmentVariable("MSXClientId");
            string MSXClientSecret = System.Environment.GetEnvironmentVariable("MSXClientSecret");
            string blobConnection = System.Environment.GetEnvironmentVariable("blobConnection");
            string NymeriaSchema = System.Environment.GetEnvironmentVariable("NymeriaSchema");
            string NymeriaSqlConnection = System.Environment.GetEnvironmentVariable("NymeriaSqlConnection");
            string vtcpManagedIdentity = System.Environment.GetEnvironmentVariable("VTCPManagedIdentity");
            crmManager = new AutoLeadCreationManager(Instancename, ClientId, ClientSecret, MSXClientId, MSXClientSecret, sqlConnection, ssasServer, blobConnection, log, NymeriaSchema, NymeriaSqlConnection,vtcpManagedIdentity);
            crmManager.UpdateAutoLeadCreationRequestStatus(RequestID);

            await Dispose(log);
            return new OkObjectResult(HttpStatusCode.OK);
        }

        public static async Task Dispose(ILogger log)
        {
            await Task.Run(() =>
            {
                for (int i = 0; i < 100; i++)
                {
                    log.LogInformation(" Update Auto Lead Creation Request Status method is successfully executed");
                }
            });
        }
    }

}
