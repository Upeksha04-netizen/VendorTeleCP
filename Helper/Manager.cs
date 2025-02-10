using Microsoft.Crm.Sdk.Messages;
using Microsoft.Extensions.Logging;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Sdk.WebServiceClient;
using Microsoft.Xrm.Tooling.Connector;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AnalysisServices.AdomdClient;
using Newtonsoft.Json.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.IO;

using Newtonsoft.Json;

using System.Configuration;
using System.Text.RegularExpressions;

using System.Security.Principal;
using System.Runtime.InteropServices;
using Azure.Identity;
using Azure.Core;
using Microsoft.Identity.Client;
using static System.Formats.Asn1.AsnWriter;
using Microsoft.Identity.Client.NativeInterop;
using System.ServiceModel;
using Microsoft.AnalysisServices;
using Microsoft.PowerPlatform.Dataverse.Client;
using ClientCredential = Microsoft.IdentityModel.Clients.ActiveDirectory.ClientCredential;
using AuthenticationResult = Microsoft.IdentityModel.Clients.ActiveDirectory.AuthenticationResult;



namespace Helper
{
    public class Manager
    {
        /// <summary>
        /// CRM Service 
        /// </summary>
        private static IOrganizationService _orgService;

        /// <summary>
        /// Username
        /// </summary>
        private static string _clientId;

        /// <summary>
        /// Password
        /// </summary>
        private static string _secret;

        /// <summary>
        /// MSXUsername
        /// </summary>
        private static string _msxclientId;

        /// <summary>
        /// MSXPassword
        /// </summary>
        private static string _msxsecret;

        /// <summary>
        /// CRM Connection string
        /// </summary>
        private static string _instancename;

        /// <summary>
        /// SQL Connection string
        /// </summary>
        private static string _sqlConnection;

        /// <summary>
        /// SSAS server connection string
        /// </summary>
        private static string _ssasServer;

        /// <summary>
        /// Blob Connection string
        /// </summary>
        private static string _blobConnection;
        private static string _vtcpManagedIdentity;

        private ILogger _log;
        //private ILogger _logger;
        private static int isYes = Convert.ToInt32(100000000);
        private static int isNo = Convert.ToInt32(100000001);
        private static int isNotFound = Convert.ToInt32(100000002);
        private static int statusDisqualified = Convert.ToInt32(2);
        private static DateTime startDatetime;
        private const string MalAccountIdMatch = "Matched By VTCP System";
        public int TotalProcessedRecords = 0;
        public Manager(string instancename, string clientId, string secret, string msxclientId, string msxsecret, string sqlconnection, string ssasServer, string blobConnection,string vtcpManagedIdentity, ILogger log)
        {
            _instancename = instancename;
            _clientId = clientId;
            _secret = secret;
            _msxclientId = msxclientId;
            _msxsecret = msxsecret;
            _sqlConnection = sqlconnection;
            _ssasServer = ssasServer;
            _blobConnection = blobConnection;
            _vtcpManagedIdentity = vtcpManagedIdentity;
            _log = log;
            _orgService = GetOrganizationService().Result;
        }

        #region Logic for Retrieving CRM organization service

        /// <summary>
        /// To get Access Token
        /// </summary>
        /// <param name="serviceUrl"></param>
        /// <returns></returns>
        //public string? GetAccessToken(string serviceUrl, bool flagMSX = false)
        //{
        //    //ClientCredential credential = null;
        //    //AuthenticationContext authContext = new AuthenticationContext("https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/authorize", false);
        //    //if (flagMSX) { credential = new ClientCredential(_msxclientId, _msxsecret); }
        //    //else { credential = new ClientCredential(_clientId, _secret); }
        //    //var authResult = authContext.AcquireTokenAsync(serviceUrl, credential);
        //    //authResult.Wait();
        //    //return authResult.Result.AccessToken;
        //}

        /// <summary>
        /// To Get CRM Organization service
        /// </summary> 
        /// <returns></returns>
        
        public async Task<IOrganizationService> GetOrganizationService()
        {
            try
            {
                string resource = $"https://{System.Environment.GetEnvironmentVariable("Instancename")}.crm.dynamics.com";
                string authResult =  getAccessToken(resource, System.Environment.GetEnvironmentVariable("VTCPManagedIdentity")).GetAwaiter().GetResult();
                IOrganizationService service = new ServiceClient(tokenProviderFunction: f => getAccessToken(resource, System.Environment.GetEnvironmentVariable("VTCPManagedIdentity")), instanceUrl: new Uri(resource), useUniqueInstance: true);

                return service;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error getting CRM service client: {ex.Message}");
                throw;
            }
            
        }
        
        #endregion

        #region Logic to Remove Agent Roster Details      
        /// <summary>
        /// To Remove agentRoaster details
        /// </summary>.
        public List<AgentRoster> GetAgentRoasterDetails()
        {
            _log.LogInformation("Execution started: ", "Remove agent roster details");
            List<AgentRoster> agentRoaster = new List<AgentRoster>();
            EntityCollection agentRoasterColl = new EntityCollection();

            QueryExpression AgentRoasterQuery = new QueryExpression()
            {
                EntityName = "new_agentroster",
                ColumnSet = new ColumnSet("new_agentrosterid"),
                Criteria = new FilterExpression(),
                TopCount = 1000

            };

            AgentRoasterQuery.Criteria.AddCondition("new_billingenddate", ConditionOperator.OlderThanXMonths, 21);
            AgentRoasterQuery.Criteria.AddCondition("new_agentalias", ConditionOperator.DoesNotBeginWith, "Removed");

            agentRoasterColl = _orgService.RetrieveMultiple(AgentRoasterQuery);

            if (agentRoasterColl != null && agentRoasterColl.Entities.Count > 0)
            {
                foreach (Entity entityObject in agentRoasterColl.Entities)
                {
                    agentRoaster.Add(new AgentRoster
                    {
                        agentrosterId = entityObject.Contains("new_agentrosterid") ? new System.Guid(entityObject["new_agentrosterid"].ToString()) : Guid.Empty,
                    });
                }
            }
            return agentRoaster;
        }
        /// <summary>
        /// To Remove AgentRoaster  details
        /// </summary>
        public int RemoveAgentRoasterDetails()
        {
            DateTime StartTime = DateTime.UtcNow;
            _log.LogInformation("Execution started Remove AgentRoaster Details");
            List<AgentRoster> agentRoasterDetails = GetAgentRoasterDetails();

            ExecuteMultipleSettings excuteMultipleSettings = new ExecuteMultipleSettings();
            excuteMultipleSettings.ContinueOnError = true;
            excuteMultipleSettings.ReturnResponses = true;

            ExecuteMultipleRequest updateAgentRequest = new ExecuteMultipleRequest();
            updateAgentRequest.Requests = new OrganizationRequestCollection();
            updateAgentRequest.Settings = excuteMultipleSettings;

            int batch = 0;
            try
            {
                if (agentRoasterDetails.Count > 0)
                {
                    foreach (var agentRoster in agentRoasterDetails)
                    {
                        batch++;
                        Entity objAgentEntity = new Entity("new_agentroster");
                        objAgentEntity.Id = new Guid(agentRoster.agentrosterId.ToString());
                        objAgentEntity["new_name"] = "PII Data Removed " + DateTime.UtcNow.ToString();
                        objAgentEntity["new_agentname"] = "PII Data Removed " + DateTime.UtcNow.ToString();
                        objAgentEntity["new_agentalias"] = "Removed " + DateTime.UtcNow.ToString();

                        var updateRequest = new UpdateRequest();
                        updateRequest.Target = objAgentEntity;
                        updateAgentRequest.Requests.Add(updateRequest);

                        if (updateAgentRequest.Requests.Count == 1000)
                        {
                            ExecuteBulkRequest(updateAgentRequest, "Updated Agent roster details.");
                            updateAgentRequest.Requests.Clear();
                            batch = 0;
                        }
                    }
                    if (batch > 0)
                    {
                        ExecuteBulkRequest(updateAgentRequest, "Updated Agent roster details.");
                        updateAgentRequest.Requests.Clear();
                        batch = 0;
                    }
                }
            }
            catch (Exception ex)
            {
                _log.LogError("Removing agent roster details", "Failed", ex);
            }
            finally
            {
                Dispose();
                DateTime EndTime = DateTime.UtcNow;
                SqlConnection con = new SqlConnection(Manager._sqlConnection);
                //AuthenticationResult authenticationResult = AADAunthenticationResult();
                con.AccessToken = Manager.getAccessToken(System.Environment.GetEnvironmentVariable("ResourceId"), System.Environment.GetEnvironmentVariable("VTCPManagedIdentity"), true).Result;
                SqlCommand SqlCommands = new SqlCommand();
                SqlCommands = new SqlCommand("AzureFunctionsDataLoad", con);
                SqlCommands.CommandType = CommandType.StoredProcedure;
                SqlCommands.Parameters.AddWithValue("@FunctionName", "RemoveAgentRoasterDetails");
                SqlCommands.Parameters.AddWithValue("@ProcessStartTime", StartTime);
                SqlCommands.Parameters.AddWithValue("@FunctionStartTime", StartTime);
                SqlCommands.Parameters.AddWithValue("@FunctionEndTime", EndTime);
                SqlCommands.Parameters.AddWithValue("@InitialRowsAffected", agentRoasterDetails.Count.ToString());
                SqlCommands.Parameters.AddWithValue("@TotalRowsAffected", TotalProcessedRecords.ToString());
                con.Open();
                SqlCommands.ExecuteNonQuery();
                con.Close();
            }
            return agentRoasterDetails.Count;
        }

        #endregion

        #region Logic to Update Agent Roster User ID
        public void UpdateUserIdAgentRoster()
        {
            DateTime StartTime = DateTime.UtcNow;
            int batch = 0;
            ExecuteMultipleSettings excuteMultipleSettings = new ExecuteMultipleSettings();
            excuteMultipleSettings.ContinueOnError = true;
            excuteMultipleSettings.ReturnResponses = true;

            ExecuteMultipleRequest updateLeadsRequest = new ExecuteMultipleRequest();
            updateLeadsRequest.Requests = new OrganizationRequestCollection();
            updateLeadsRequest.Settings = excuteMultipleSettings;

            string query = "select * from [dbo].[SSIS_AgentRoster_Output] (nolock)";
            DataTable dt = RetrieveDatafromSQLDatabase(query);
            string ProcessStartTime = string.Empty;
            if (dt != null && dt.Rows.Count > 0)
            {
                foreach (DataRow row in dt.Rows)
                {
                    batch++;

                    string AgentID = row["AgentID"].ToString();
                    string UpdateRequired = row["UpdateRequired"].ToString();
                    string SystemUserID = row["SystemUserId"].ToString();
                    Entity objAgentEntity = new Entity("new_agentroster");
                    objAgentEntity.Id = new Guid(AgentID);

                    if (UpdateRequired == "Set System User ID")
                        objAgentEntity["new_msxsystemuserid"] = SystemUserID;
                    else if (UpdateRequired == "Deactivate")
                        objAgentEntity["new_isactiveresource"] = new OptionSetValue(100000001);
                    else if (UpdateRequired == "Deactivate and Set End Date")
                    {
                        objAgentEntity["new_isactiveresource"] = new OptionSetValue(100000001);
                        objAgentEntity["new_billingenddate"] = DateTime.UtcNow;
                    }
                    else if (UpdateRequired == "Remove PII")
                    {
                        objAgentEntity["new_name"] = "PII Data Removed " + DateTime.UtcNow.ToString();
                        objAgentEntity["new_agentname"] = "PII Data Removed " + DateTime.UtcNow.ToString();
                        objAgentEntity["new_agentalias"] = "Removed " + DateTime.UtcNow.ToString();
                    }

                    var updateRequest = new UpdateRequest();
                    updateRequest.Target = objAgentEntity;
                    updateLeadsRequest.Requests.Add(updateRequest);
                    if (updateLeadsRequest.Requests.Count == 1000)
                    {
                        ExecuteBulkRequest(updateLeadsRequest, "Updated Agent Roster User ID Details");
                        updateLeadsRequest.Requests.Clear();
                        batch = 0;
                    }
                }
                if (batch > 0)
                {
                    ExecuteBulkRequest(updateLeadsRequest, "Updated Agent Roster User ID Details");
                    updateLeadsRequest.Requests.Clear();
                    batch = 0;
                }

            }
            DateTime EndTime = DateTime.UtcNow;
            SqlConnection con = new SqlConnection(_sqlConnection);
            //AuthenticationResult authenticationResult = AADAunthenticationResult();
            con.AccessToken = Manager.getAccessToken(System.Environment.GetEnvironmentVariable("ResourceId"), System.Environment.GetEnvironmentVariable("VTCPManagedIdentity"), true).Result;
            SqlCommand SqlCommands = new SqlCommand();
            SqlCommands = new SqlCommand("AzureFunctionsDataLoad", con);
            SqlCommands.CommandType = CommandType.StoredProcedure;
            SqlCommands.Parameters.AddWithValue("@FunctionName", "AssignSystemUserIDAgentRoster");
            SqlCommands.Parameters.AddWithValue("@ProcessStartTime", ProcessStartTime);
            SqlCommands.Parameters.AddWithValue("@FunctionStartTime", StartTime.ToString());
            SqlCommands.Parameters.AddWithValue("@FunctionEndTime", EndTime.ToString());
            SqlCommands.Parameters.AddWithValue("@InitialRowsAffected", dt.Rows.Count.ToString());
            SqlCommands.Parameters.AddWithValue("@TotalRowsAffected", TotalProcessedRecords.ToString());
            con.Open();
            SqlCommands.ExecuteNonQuery();
            con.Close();
        }
        #endregion

        #region Generic methods for the CRM and SQL Database

        /// <summary>
        /// To retrieve data from SQL server
        /// </summary>
        /// <param name="storedProcedure"></param>
        /// <param name="dataTable"></param>
        /// <returns></returns>
        public DataTable RetrieveDatafromSQLDatabase(string query)
        {
            _log.LogInformation("Execution started: ", "RetrieveDatafromSQLDatabase");
            DataTable dt = null;
            int timeout = 0;
            try
            {
                using (SqlConnection conn = new SqlConnection(_sqlConnection))
                {
                    //AuthenticationResult authenticationResult = AADAunthenticationResult();
                    string resourceId = System.Environment.GetEnvironmentVariable("ResourceId");
                    conn.AccessToken = Manager.getAccessToken(resourceId, System.Environment.GetEnvironmentVariable("VTCPManagedIdentity"), true).Result;
                    conn.Open();
                    using (SqlDataAdapter da = new SqlDataAdapter())
                    {
                        da.SelectCommand = new SqlCommand(query, conn);
                        DataSet ds = new DataSet();
                        da.SelectCommand.CommandTimeout = timeout;
                        da.Fill(ds, "result_name");
                        dt = ds.Tables["result_name"];
                    }
                    conn.Close();
                }
            }
            catch (Exception ex)
            {
                _log.LogError("Retrieve Data from SQL Database", "Failed", ex);
            }
            return dt;
        }

        /// <summary>
        /// Insert Details into SQL DB
        /// </summary>
        /// <param name="TableName"></param>
        /// <param name="dataTable"></param>
        public void InsertDataIntoSQLDatabase(string TableName, DataTable dataTable, [Optional] string sqlNymeriaConn, [Optional] string nymeriaSchema)
        {
            string sqlCon = string.Empty;
            _log.LogInformation("Execution started: ", "InsertDataIntoSQLDatabase");
            try
            {
                if (TableName == nymeriaSchema + ".[AgentAssignmentDetails]" || TableName == nymeriaSchema + ".[LeadCreationRequests]")
                {
                    sqlCon = sqlNymeriaConn;
                }
                else
                {
                    sqlCon = _sqlConnection;
                }
                using (SqlConnection conn = new SqlConnection(sqlCon))
                {
                    //AuthenticationResult authenticationResult = AADAunthenticationResult();
                    conn.AccessToken = Manager.getAccessToken(System.Environment.GetEnvironmentVariable("ResourceId"), System.Environment.GetEnvironmentVariable("VTCPManagedIdentity"), true).Result;
                    SqlBulkCopy objbulk = new SqlBulkCopy(conn);
                    objbulk.DestinationTableName = TableName;
                    objbulk.BulkCopyTimeout = 0;
                    objbulk.BatchSize = 2500;
                    conn.Open();
                    foreach (var column in dataTable.Columns)
                    {
                        objbulk.ColumnMappings.Add(column.ToString(), column.ToString());
                    }
                    objbulk.WriteToServer(dataTable);
                    conn.Close();
                }
            }
            catch (Exception ex)
            {
                _log.LogError("Insert Data into SQL Database", "Failed", ex);
            }
        }

        /// <summary>
        /// To execute bulk request to CRM
        /// </summary>
        /// <param name="executeMultipleRequest"></param>
        /// <param name="message"></param>
        public void ExecuteBulkRequest(ExecuteMultipleRequest executeMultipleRequest, string message, bool flag = false)
        {
            _log.LogInformation("Execution started: ", "ExecuteBulkRequest");
            //var method = "ExecuteBulkRequest";
            ExecuteMultipleResponse responseWithResults = ExecuteBatchRequest(executeMultipleRequest);
            if (responseWithResults != null)
            {
                var responses = (ExecuteMultipleResponseItemCollection)responseWithResults.Results["Responses"];
                int countOfProcessedRecords = 0;
                foreach (var response in responses)
                {
                    if (response.Fault != null)
                    {
                        if (flag) { _log.LogInformation("Failed to " + message + ((Microsoft.Xrm.Sdk.Messages.CreateRequest)(executeMultipleRequest.Requests[countOfProcessedRecords])).Target.Id.ToString()); }
                        else if (message == "Removed Agent roster details.")
                        { _log.LogInformation("Failed to " + message + ((Microsoft.Xrm.Sdk.Messages.DeleteRequest)(executeMultipleRequest.Requests[countOfProcessedRecords])).Target.Id.ToString()); }
                        else { _log.LogInformation("Failed to " + message + ((Microsoft.Xrm.Sdk.Messages.UpdateRequest)(executeMultipleRequest.Requests[countOfProcessedRecords])).Target.Id.ToString()); }
                    }
                    else
                    {
                        if (flag) { _log.LogInformation("Succeeded to " + message + ((Microsoft.Xrm.Sdk.Messages.CreateRequest)(executeMultipleRequest.Requests[countOfProcessedRecords])).Target.Id.ToString()); }
                        else if (message == "Removed Agent roster details.")
                        { _log.LogInformation("Succeeded to " + message + ((Microsoft.Xrm.Sdk.Messages.DeleteRequest)(executeMultipleRequest.Requests[countOfProcessedRecords])).Target.Id.ToString()); }
                        else { _log.LogInformation("Succeeded to " + message + ((Microsoft.Xrm.Sdk.Messages.UpdateRequest)(executeMultipleRequest.Requests[countOfProcessedRecords])).Target.Id.ToString()); }
                        countOfProcessedRecords++;
                    }
                }
                TotalProcessedRecords += countOfProcessedRecords;
            }
        }

        /// <summary>
        /// Execute Multiple requests to CRM
        /// </summary>
        /// <param name="ReqCollection"></param>
        /// <returns></returns>
        public ExecuteMultipleResponse ExecuteBatchRequest(ExecuteMultipleRequest ReqCollection)
        {
            _log.LogInformation("Execution started: ", "ExecuteBatchRequest");
            var method = "ExecuteBatchRequest";
            ExecuteMultipleResponse responseWithResults = null;

            try
            {
                if (ReqCollection.Requests.Count > 0)
                {
                    responseWithResults = (ExecuteMultipleResponse)_orgService.Execute(ReqCollection);
                    _log.LogInformation("Updated record count is" + responseWithResults.Responses.Count.ToString());
                }
            }
            catch (FaultException<OrganizationServiceFault> fault)
            {
                if (fault.Detail.ErrorDetails.Contains("MaxBatchSize"))
                {
                    _log.LogInformation(method + "Fault Exception", fault.Message);
                    throw new ApplicationException(fault.ToString());
                }
                return responseWithResults;
            }
            catch (Exception e)
            {
                _log.LogInformation(method + "Excpetion", e.Message);
                throw new Exception(e.ToString());
            }
            return responseWithResults;
        }

        /// <summary>
        /// Lead entity fields
        /// </summary>
        public class CsLeads
        {
            public Guid LeadID;
            public OptionSetValue? Is_LIR_Contacted;
            public OptionSetValue? Is_MSX_Contacted;
            public string? MSSalesTPID;
            public string? MSXAccountID;
            public string? ImportedAccountName;
            public string? Subsidiary;
            public OptionSetValue? Is_MAL;
            public OptionSetValue? Is_Mandatory;
            public OptionSetValue? ExistingMSXActivity;
            public string? AgreementID;
            public OptionSetValue? Is_Exclusionlist;
            public string? MSXStatus;
            public string? MSXAccountIdSource;
            public string? City;
            public string? CLAS_VLAllocation;
            public string? CLAS_MalDomain;
            public string? CLAS_MalExactNameMatch;
            public string? CLAS_SubSegment;
            public string? CLAS_Nro;
            public string? CLAS_TPNameWaste;
            public string? ContactSource;
            public string? Firstname;
            public string? Lastname;
            public string? EmailAddress;
            public string? BusinessPhone;
            public string? JobTitle;
            public string? Createdby;
            public DateTime Createdon;
            public string? Leadsource;
            public string? LeadsourceSubType;
            public string? LeadTitle;
            public string? Address;
            public string? State;
            public string? PostalCode;
            public string? WebsiteURL;
            public string? Need;
            public string? Notes;
            public string? CampaignCode;
            public DateTime? AgreementExpirationDate;
            public string? MSXLeadOwner;
            public string? DefaultTeamOwner;
            public string? DefaultTeamSubsidiaryOwner;
            public string? AgreementIDList;
            public string? Country;
            public Guid CustomerList;
            public string? CampaignType;
            public OptionSetValue? PreApproveForMSXUpload;
            public string? IsAutoCreatedLead;
            public string? IsMergedLead;
            public string? ResellerName;
            public string? DistributorName;
            public string? AdvisorName;
            public string? ExpiringProductDetails;
            public string? MergedAgreementDetails;
            public string? MergedProductDetails;
            public DateTime? MergedMinExpirationDate;
            public decimal MergedExpiringAmount;
            public decimal? ExpiringAmount;
            public string? PrimaryProduct;
            public string? TopUnmanaged_CSM;
            public string? CustomerTPIDList;
            public OptionSetValue? MergeMultipleLeadsForSameCustomer;
            public string? AddressSource;
            public string? CLAS_SmcType;
            public string? CLAS_PropensityDetails;
            public string? CLAS_ProductOwnershipDetails;
            public string? VendorOwner;
            public string? CLAS_Propensity;
            public string? StatusReason;
            public string? ExistingMSXActivitySameVendor;
            public string? ExistingMSXActivityOtherTeam;
            public string? VendorName;
            public string? MSXAccountIDMatched;
            public string? MSXLeadOppID;
            public string? AdditionalContactDetails;
            public DateTime ValidationCompletedTime;
            public DateTime MSXUploadTriggerTime;
            public string? MSXUploadLeadNumber;
            public Guid MasterID;
            public OptionSetValueCollection? DisqualifyReasons;
            public int MSXUploadRetryAttempts;
            public string? MSXUploadStatusDetail;
            public string? MergedContactDetails;
            public string? MergeStatus;
            public decimal NymeriaRanking;
            public string? CLAS_VT_Priority;
            public string? NymeriaPriority;
            public OptionSetValue? CallPrepSheet;
            public string? CallPrepSheetLink;
            public string? LeadNumber;
        }

        public class AgentRoster
        {
            public Guid agentrosterId;
        }
        /// <summary>
        /// To create security protocol
        /// </summary>
        /// <returns></returns>
        public static SecurityProtocolType SecurityProtocol()
        {
            return ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
        }

        public void Dispose()
        {
            //_orgService.Dispose();
        }

        /// <summary>
        /// To set Character Limit for fields
        /// </summary>
        /// <param name="fieldValue"></param>
        /// <returns></returns>
        public string? LimitCharacterCount(string fieldValue, int MaxLength)
        {
            if (!string.IsNullOrEmpty(fieldValue) && fieldValue.Length > MaxLength)
            {
                fieldValue = fieldValue.Substring(0, MaxLength);
            }

            return fieldValue;
        }

        /// <summary>
        /// To replace the Hexadecimal characters in strings
        /// </summary>
        /// <param name="hexaText"></param>
        /// <returns></returns>
        public string? ReplaceHexadecimalSymbols(string hexaText)
        {
            string r = "[\x00-\x08\x0B\x0C\x0E-\x1F]";
            return Regex.Replace(hexaText, r, "", RegexOptions.Compiled);
        }


        /// <summary>
        /// Generic method to Query Multiple Records
        /// </summary>
        /// <param name="entityName"></param>
        /// <param name="allColumn">set 'true' to get all fields</param>
        /// <param name="columnSet">pass 'null' if allColumn param is set to 'true'</param>
        /// <param name="upperLimit">set 'null' to get all posible items</param>
        /// <returns></returns>
        public EntityCollection Retrieve5000PlusRecordsUsingQueryExpression(string entityName, bool allColumn, ColumnSet columnSet, List<FilterExpression> filterExpression, int? upperLimit)
        {

            // Query using the paging cookie.
            // Define the paging attributes.

            //EntityCollection Object
            EntityCollection results = null;
            EntityCollection finalResults = new EntityCollection();

            // The number of records per page to retrieve.
            int queryCount = 5000;
            int totalRecordsFetched = 0;

            // Initialize the page number.
            int pageNumber = 1;

            // Create the query expression and add condition.
            QueryExpression pagequery = new QueryExpression();
            pagequery.EntityName = entityName;

            if (allColumn)
                pagequery.ColumnSet.AllColumns = true;
            else
                pagequery.ColumnSet = columnSet;

            if (filterExpression?.Count > 0)
                foreach (var item in filterExpression)
                    pagequery.Criteria.AddFilter(item);

            // Assign the pageinfo properties to the query expression.
            pagequery.PageInfo = new PagingInfo();
            pagequery.PageInfo.Count = queryCount;
            pagequery.PageInfo.PageNumber = pageNumber;

            // The current paging cookie. When retrieving the first page
            // pagingCookie should be null.
            pagequery.PageInfo.PagingCookie = null;
            while (true)
            {
                // Retrieve the page.
                results = _orgService.RetrieveMultiple(pagequery);
                if (results.Entities != null)
                {
                    // Retrieve all records from the result set.
                    foreach (var acct in results.Entities)
                    {
                        finalResults.Entities.Add(acct);
                        //_log.LogInformation(Convert.ToString(++recordCount));
                    }
                    totalRecordsFetched += results.Entities.Count;
                }
                // Check for more records, if it returns true.
                if (results.MoreRecords)
                {
                    // Increment the page number to retrieve the next page.
                    pagequery.PageInfo.PageNumber++;
                    // Set the paging cookie to the paging cookie returned from current results.
                    pagequery.PageInfo.PagingCookie = results.PagingCookie;
                }
                else
                {
                    // If no more records are in the result nodes, exit the loop.
                    break;
                }
                if (upperLimit != null)
                    if (totalRecordsFetched >= upperLimit)
                        break;
            }
            return finalResults;
        }
        public AuthenticationResult AADAunthenticationResult()
        {
            string clientId = System.Environment.GetEnvironmentVariable("SPNClientId");
            string clientSecretKey = System.Environment.GetEnvironmentVariable("SPNClientSecretKey");
            string aadInstance = System.Environment.GetEnvironmentVariable("AadInstance");
            string ResourceId = System.Environment.GetEnvironmentVariable("ResourceId");
            AuthenticationContext authenticationContext = new AuthenticationContext(aadInstance);
            ClientCredential clientCredential = new ClientCredential(clientId, clientSecretKey);
            AuthenticationResult authenticationResult = authenticationContext.AcquireTokenAsync(ResourceId, clientCredential).Result;
            return authenticationResult;
        }
        #endregion

        public static async Task<string> getAccessToken(string? organizationUrl, string? clientId,bool msx=false)
        {
            var credential = new Azure.Identity.DefaultAzureCredential(new DefaultAzureCredentialOptions { ManagedIdentityClientId = clientId }); // new ManagedIdentityCredential(clientId);// new Azure.Identity.DefaultAzureCredential(new DefaultAzureCredentialOptions { ManagedIdentityClientId = clientId });
            var token = (await credential.GetTokenAsync(new Azure.Core.TokenRequestContext(new[] { $"{organizationUrl}/.default" })));
            return token.Token;
        }

        public string? GetAccessTokenClientId(string serviceUrl, bool flagMSX = false)
        {
            ClientCredential credential = null;
            AuthenticationContext authContext = new AuthenticationContext("https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/authorize", false);
            if (flagMSX) { credential = new ClientCredential(_msxclientId, _msxsecret); }
            else { credential = new ClientCredential(_clientId, _secret); }
            var authResult = authContext.AcquireTokenAsync(serviceUrl, credential);
            authResult.Wait();
            return authResult.Result.AccessToken;
        }

    }
}