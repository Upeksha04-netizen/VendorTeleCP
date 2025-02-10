using System.Configuration;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Helper;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;


namespace GlobalIAPSFunctions
{
    public static class RemoveDuplicateLeaNumber
    {
        [FunctionName("RemoveDuplicateLeaNumber")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            LeadCleanupFunctions crmManager = null;
            string Instancename = System.Environment.GetEnvironmentVariable("Instancename");//vendortelecp
            string ClientId = System.Environment.GetEnvironmentVariable("ClientId");
            string ClientSecret = System.Environment.GetEnvironmentVariable("ClientSecret");
            string sqlConnection = System.Environment.GetEnvironmentVariable("sqlConnection");
            string ssasServer = System.Environment.GetEnvironmentVariable("ssasServer");
            string MSXClientId = System.Environment.GetEnvironmentVariable("MSXClientId");
            string MSXClientSecret = System.Environment.GetEnvironmentVariable("MSXClientSecret");
            string blobConnection = System.Environment.GetEnvironmentVariable("blobConnection");
            string vtcpManagedIdentity = System.Environment.GetEnvironmentVariable("VTCPManagedIdentity");
            crmManager = new LeadCleanupFunctions(Instancename, ClientId, ClientSecret, MSXClientId, MSXClientSecret, sqlConnection, ssasServer, blobConnection,vtcpManagedIdentity, log);
            crmManager.RemoveDuplicateLeaNumber();

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
                    log.LogInformation("Remove Duplicate Lead Number method is successfully executed");
                }
            });
        }
    }
}