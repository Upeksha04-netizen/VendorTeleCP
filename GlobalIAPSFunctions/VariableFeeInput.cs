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
    public static class VariableFeeInput
    {
        [FunctionName("VariableFeeInput")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            VariableFeeManager crmManager = null;
            string Instancename = System.Environment.GetEnvironmentVariable("Instancename");//vendortelecp
            string ClientId = System.Environment.GetEnvironmentVariable("ClientId");
            string ClientSecret = System.Environment.GetEnvironmentVariable("ClientSecret");
            string sqlConnection = System.Environment.GetEnvironmentVariable("sqlConnection");
            string ssasServer = System.Environment.GetEnvironmentVariable("ssasServer");
            string MSXClientId = System.Environment.GetEnvironmentVariable("MSXClientId");
            string MSXClientSecret = System.Environment.GetEnvironmentVariable("MSXClientSecret");
            string blobConnection = System.Environment.GetEnvironmentVariable("blobConnection");
            string vtcpManagedIdentity = System.Environment.GetEnvironmentVariable("VTCPManagedIdentity");
            crmManager = new VariableFeeManager(Instancename, ClientId, ClientSecret, MSXClientId, MSXClientSecret, sqlConnection, ssasServer, blobConnection,vtcpManagedIdentity, log);
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);
            string fiscalQuarter = data?.FiscalQuarter;
            string variableFeeCalculationOutputType = data?.VariableFeeCalculationOutputType;
            log.LogInformation("fiscalQuarter:" + fiscalQuarter + "variableFeeCalcType:" + variableFeeCalculationOutputType);
            crmManager.VariableFeeInput(fiscalQuarter, variableFeeCalculationOutputType);
            await Dispose(log);
           

            return new OkObjectResult(HttpStatusCode.OK);
        }
        public static async Task Dispose(ILogger log)
        {
            await Task.Run(() =>
            {
                for (int i = 0; i < 100; i++)
                {
                    log.LogInformation("Upload to VariableFeeInput method is successfully executed");
                }
            });
        }
    }
}