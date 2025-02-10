using System;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Xml.Linq;
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
    public static class MSXAcknowledge
    {
        [FunctionName("MSXAcknowledge")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");
            try
            {
                string VTCPLeadId = string.Empty;
                string IsSuccessful = string.Empty;
                string MSXLeadId = string.Empty;
                string FailureType = string.Empty;
                string ErrorMessage = string.Empty;
                bool StatusCode = false;
                // Get request body
                string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
                dynamic data = JsonConvert.DeserializeObject(requestBody);
                //string jsondata = req.Content.ReadAsStringAsync().Result;
                VTCPLeadId = data?.CorrelationID;
                IsSuccessful = data?.IsSuccessful;
                MSXLeadId = data?.LeadId;
                FailureType = data?.FailureType;
                ErrorMessage = data?.ErrorMessage;

                log.LogInformation(string.Concat(("Lead Acknowledgement", "LeadId:" + data?.CorrelationID, "IsSuccessful:" + data?.IsSuccessful, "MSXLeadId:" + data?.LeadId, "MessageOperation:" + data?.MessageOperation, "OriginSystemName:" + data?.OriginSystemName,
                    "FailureType:" + data?.FailureType, "ErrorMessage:" + data?.ErrorMessage)));
                log.LogInformation("Complete Json Response :" + requestBody);
                if (!string.IsNullOrWhiteSpace(IsSuccessful) && IsSuccessful.ToLower() == "true") { StatusCode = true; }

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
                crmManager.UpdateMSXLeadIDToVTCP(VTCPLeadId, StatusCode, MSXLeadId, FailureType, ErrorMessage);
                JObject result = new JObject();
                result.Add("status", "complete");
                log.LogInformation(" MSX Acknowledgement method is successfully executed");
                //string responseMessage = string.IsNullOrEmpty(data)
                //? "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response."
                //: $"Hello, {data}. This HTTP triggered function executed successfully.";
                return new OkObjectResult(HttpStatusCode.OK);
            }
            catch (Exception ex)
            {
                log.LogInformation(ex.Message + "Stack Trace " + ex.StackTrace);
                throw new Exception(ex.Message);
            }
        }
    }
}
