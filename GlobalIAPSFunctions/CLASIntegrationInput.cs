using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Configuration;
using Helper;

namespace GlobalIAPSFunctions
{
    public static class CLASIntegrationInput
    {
        [FunctionName("CLASIntegrationInput")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");
            
            LeadValidationFunctions crmManager = null;
            string Instancename = System.Environment.GetEnvironmentVariable("Instancename");//vendortelecp
            string ClientId = System.Environment.GetEnvironmentVariable("ClientId");
            string ClientSecret = System.Environment.GetEnvironmentVariable("ClientSecret");
            string sqlConnection = System.Environment.GetEnvironmentVariable("sqlConnection");
            string ssasServer = System.Environment.GetEnvironmentVariable("ssasServer");
            string MSXClientId = System.Environment.GetEnvironmentVariable("MSXClientId");
            string MSXClientSecret = System.Environment.GetEnvironmentVariable("MSXClientSecret");
            string blobConnection = System.Environment.GetEnvironmentVariable("blobConnection");
            string vtcpManagedIdentity = System.Environment.GetEnvironmentVariable("VTCPManagedIdentity");
            crmManager = new LeadValidationFunctions(Instancename, ClientId, ClientSecret, MSXClientId, MSXClientSecret, sqlConnection, ssasServer, blobConnection,vtcpManagedIdentity, log);
            crmManager.InsertCLASDetailsIntoTable();

            string name = req.Query["name"];

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);
            name = name ?? data?.name;

            string responseMessage = string.IsNullOrEmpty(name)
                ? "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response."
                : $"Hello, {name}. This HTTP triggered function executed successfully.";

            return new OkObjectResult(responseMessage);
        }
    }
}
