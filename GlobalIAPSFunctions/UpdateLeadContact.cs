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
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;
using System.IO;
using Microsoft.AspNetCore.Http;

namespace GlobalIAPSFunctions
{
    public static class UpdateLeadContact
    {
        [FunctionName("UpdateLeadContactDetails")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");
            try
            {
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
                crmManager.UpdateContactDetails();

                JObject result = new JObject();
                result.Add("status", "complete");
                log.LogInformation(" Lead Contacts Updated Sucessfully");
                string name = req.Query["name"];

                string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
                dynamic data = JsonConvert.DeserializeObject(requestBody);
                name = name ?? data?.name;

                string responseMessage = string.IsNullOrEmpty(name)
                    ? "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response."
                    : $"Hello, {name}. This HTTP triggered function executed successfully.";

                return new OkObjectResult(responseMessage);
            }
            catch (Exception ex)
            {
                log.LogInformation(ex.Message + "Stack Trace " + ex.StackTrace);
                throw new Exception(ex.Message);
            }
        }

    }
}
