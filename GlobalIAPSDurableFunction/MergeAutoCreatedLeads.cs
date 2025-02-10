using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Helper;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace GlobalIAPSDurableFunction
{
    public static class MergeAutoCreatedLeads
    {
        [FunctionName("MergeAutoCreatedLeads_HttpStart")]
        public static async Task<IActionResult> Run(
    [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req, [DurableClient] IDurableOrchestrationClient starter,
    ILogger log)
        {
            string instanceId = await starter.StartNewAsync("MergeAutoCreatedLeads_Orchestration", null);

            log.LogInformation($"Started orchestration with ID = '{instanceId}'.");

            return starter.CreateCheckStatusResponse(req, instanceId);
        }

        [FunctionName("MergeAutoCreatedLeads_Orchestration")]
        public static async Task<List<string>> RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            var outputs = new List<string>();

            outputs.Add(await context.CallActivityAsync<string>("MergeAutoCreatedLeads_Activity", "MergeAutoCreatedLeads"));

            return outputs;
        }

        [FunctionName("MergeAutoCreatedLeads_Activity")]
        public static string ExecuteBusinessLogic([ActivityTrigger] string name, ILogger log)
        {
            log.LogInformation($"MergeAutoCreatedLeads_Activity Started");
            LeadValidationFunctions crmManager = null;
            string Instancename = Environment.GetEnvironmentVariable("Instancename", EnvironmentVariableTarget.Process);
            string ClientId = Environment.GetEnvironmentVariable("ClientId", EnvironmentVariableTarget.Process);
            string ClientSecret = Environment.GetEnvironmentVariable("ClientSecret", EnvironmentVariableTarget.Process);
            string sqlConnection = Environment.GetEnvironmentVariable("sqlConnection", EnvironmentVariableTarget.Process);
            string ssasServer = Environment.GetEnvironmentVariable("ssasServer", EnvironmentVariableTarget.Process);
            string MSXClientId = Environment.GetEnvironmentVariable("MSXClientId", EnvironmentVariableTarget.Process);
            string MSXClientSecret = Environment.GetEnvironmentVariable("MSXClientSecret", EnvironmentVariableTarget.Process);
            string blobConnection = Environment.GetEnvironmentVariable("blobConnection", EnvironmentVariableTarget.Process);
            string vtcpManagedIdentity = System.Environment.GetEnvironmentVariable("VTCPManagedIdentity");
            crmManager = new LeadValidationFunctions(Instancename, ClientId, ClientSecret, MSXClientId, MSXClientSecret, sqlConnection, ssasServer, blobConnection,vtcpManagedIdentity, log);
            crmManager.MergeLeadsWithSameTPID();

            return name + " Successeded";
        }
    }
}
