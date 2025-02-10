using Microsoft.Crm.Sdk.Messages;
using Microsoft.Extensions.Logging;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Tooling.Connector;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Newtonsoft.Json.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.IO;
using Newtonsoft.Json;
using System.Configuration;
using System.Text.RegularExpressions;
using static Helper.Manager;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.Xrm.Sdk.WebServiceClient;
using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Core;

namespace Helper
{
    public class LeadValidationFunctions
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

        Manager _manager;
        public LeadValidationFunctions(string instancename, string clientId, string secret, string msxclientId, string msxsecret, string sqlconnection, string ssasServer, string blobConnection,string vtcpManagedIdentity, ILogger log)
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
            _manager = new Manager(_instancename, _clientId, _secret, _msxclientId, _msxsecret, _sqlConnection, _ssasServer, _blobConnection,_vtcpManagedIdentity, _log);
         
            _orgService = _manager.GetOrganizationService().Result;
        }
        #region Logic For Renewal Leads Check
        public void InsertRenewalLeadsDtailsIntoTable()
        {
            DateTime StartTime = DateTime.UtcNow;
            Stopwatch timer = new Stopwatch();
            _log.LogInformation("Execution started: ", "UpdateLirDetails");
            int i = 0;
            List<CsLeads> leadList = GetIAPSLeadsForRenewalLeadsCheck();
            DataTable dt = new DataTable();
            try
            {
                if (leadList.Count > 0)
                {
                    DataTable _dtLeadDetails = new DataTable();
                    _dtLeadDetails.Columns.Add("Leadref", typeof(Guid));
                    _dtLeadDetails.Columns.Add("AgreementID", typeof(string));
                    _dtLeadDetails.Columns.Add("ProcessStartTime", typeof(string));

                    foreach (var lead in leadList)
                    {
                        i++;
                        DataRow drLeadNumbers = _dtLeadDetails.NewRow();
                        drLeadNumbers["Leadref"] = lead.LeadID;
                        drLeadNumbers["AgreementID"] = string.IsNullOrEmpty(lead.AgreementID) ? null : lead.AgreementID;
                        drLeadNumbers["ProcessStartTime"] = StartTime.ToString();
                        _dtLeadDetails.Rows.Add(drLeadNumbers);
                        if (i == leadList.Count)
                        {
                            string tableName = "[dbo].[SSIS_Staging_RenewalLeadsCheck] ";
                            _manager.InsertDataIntoSQLDatabase(tableName, _dtLeadDetails);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _log.LogError("UpdateRenewalLeadCheckDetails", "Failed", ex);
            }
            finally
            {
                _manager.Dispose();
                _log.LogInformation(timer.Elapsed.TotalSeconds.ToString());
                timer.Stop();
                DateTime EndTime = DateTime.UtcNow;
                SqlConnection con = new SqlConnection(LeadValidationFunctions._sqlConnection);
                ////AuthenticationResult authenticationResult = _manager.AADAunthenticationResult();
                con.AccessToken = Manager.getAccessToken(System.Environment.GetEnvironmentVariable("ResourceId"), System.Environment.GetEnvironmentVariable("VTCPManagedIdentity"), true).Result;

                SqlCommand SqlCommands = new SqlCommand();
                SqlCommands = new SqlCommand("AzureFunctionsDataLoad", con);
                SqlCommands.CommandType = CommandType.StoredProcedure;
                SqlCommands.Parameters.AddWithValue("@FunctionName", "RenewalLeadsCheckInput");
                SqlCommands.Parameters.AddWithValue("@ProcessStartTime", StartTime.ToString());
                SqlCommands.Parameters.AddWithValue("@FunctionStartTime", StartTime.ToString());
                SqlCommands.Parameters.AddWithValue("@FunctionEndTime", EndTime.ToString());
                SqlCommands.Parameters.AddWithValue("@InitialRowsAffected", leadList.Count.ToString());
                SqlCommands.Parameters.AddWithValue("@TotalRowsAffected", leadList.Count.ToString());
                con.Open();
                SqlCommands.ExecuteNonQuery();
                con.Close();
            }
        }
        public void RetrieveRenewalLeadsFromTable()
        {
            DateTime StartTime = DateTime.UtcNow;
            int batch = 0;
            ExecuteMultipleSettings excuteMultipleSettings = new ExecuteMultipleSettings();
            excuteMultipleSettings.ContinueOnError = true;
            excuteMultipleSettings.ReturnResponses = true;

            ExecuteMultipleRequest updateLeadsRequest = new ExecuteMultipleRequest();
            updateLeadsRequest.Requests = new OrganizationRequestCollection();
            updateLeadsRequest.Settings = excuteMultipleSettings;

            string query = "select * from [dbo].[SSIS_Output_RenewalLeadsCheck]  (nolock) ";
            DataTable dt = _manager.RetrieveDatafromSQLDatabase(query);
            string ProcessStartTime = string.Empty;
            if (dt != null && dt.Rows.Count > 0)
            {
                foreach (DataRow row in dt.Rows)
                {
                    batch++;

                    string LeadRefId = row["Leadref"].ToString();
                    ProcessStartTime = Convert.ToString(row["ProcessStartTime"]);
                    Entity objLeadEntity = new Entity("lead");
                    objLeadEntity.Id = new Guid(LeadRefId);
                    if (row["IsPresentCLAS_Exp"] != null && Convert.ToString(row["IsPresentCLAS_Exp"]).ToLower() == "yes")
                    {
                        objLeadEntity["statecode"] = new OptionSetValue(Convert.ToInt32(2));
                        objLeadEntity["statuscode"] = new OptionSetValue(Convert.ToInt32(100000037));
                    }

                    objLeadEntity["new_leadupdatedby"] = "RenewalLeadsCheck Output Job";
                    objLeadEntity["new_is_renewalleadscheckcomplete"] = new OptionSetValue(Convert.ToInt32(100000000));
                    objLeadEntity["new_pendingvalidationstepsortorder"] = new OptionSetValue(Convert.ToInt32(100000001)); ;
                    var updateRequest = new UpdateRequest();
                    updateRequest.Target = objLeadEntity;
                    updateLeadsRequest.Requests.Add(updateRequest);
                    if (updateLeadsRequest.Requests.Count == 1000)
                    {
                        _manager.ExecuteBulkRequest(updateLeadsRequest, "Updated RenewalLeadsCheck Details");
                        updateLeadsRequest.Requests.Clear();
                        batch = 0;
                    }
                }
                if (batch > 0)
                {
                    _manager.ExecuteBulkRequest(updateLeadsRequest, "Updated RenewalLeadsCheck Details");
                    updateLeadsRequest.Requests.Clear();
                    batch = 0;
                }

            }
            DateTime EndTime = DateTime.UtcNow;
            SqlConnection con = new SqlConnection(LeadValidationFunctions._sqlConnection);
            ////AuthenticationResult authenticationResult = _manager.AADAunthenticationResult();
            con.AccessToken = Manager.getAccessToken(System.Environment.GetEnvironmentVariable("ResourceId"), System.Environment.GetEnvironmentVariable("VTCPManagedIdentity"), true).Result;

            SqlCommand SqlCommands = new SqlCommand();
            SqlCommands = new SqlCommand("AzureFunctionsDataLoad", con);
            SqlCommands.CommandType = CommandType.StoredProcedure;
            SqlCommands.Parameters.AddWithValue("@FunctionName", "RenewalLeadsCheckOutput");
            SqlCommands.Parameters.AddWithValue("@ProcessStartTime", ProcessStartTime);
            SqlCommands.Parameters.AddWithValue("@FunctionStartTime", StartTime.ToString());
            SqlCommands.Parameters.AddWithValue("@FunctionEndTime", EndTime.ToString());
            SqlCommands.Parameters.AddWithValue("@InitialRowsAffected", dt.Rows.Count.ToString());
            SqlCommands.Parameters.AddWithValue("@TotalRowsAffected", TotalProcessedRecords.ToString());
            con.Open();
            SqlCommands.ExecuteNonQuery();
            con.Close();

        }

        public List<CsLeads> GetIAPSLeadsForRenewalLeadsCheck()
        {
            _log.LogInformation("Execution started GetIAPSLeadsForRenewalLeadsCheck");
            List<CsLeads> Leads = new List<CsLeads>();
            EntityCollection leadscoll = new EntityCollection();
            int statusReason = Convert.ToInt32(100000008);
            // Manual Renewal Check Pending Validation Step
            int pendingValidationStepSortOrder = Convert.ToInt32(100000000);
            int status = Convert.ToInt32(0);
            int topCount = 0;
            topCount = 1000;

            _log.LogInformation("user id: ", "GetIAPSLeadsForRenewalLeadsCheck");
            QueryExpression leadattributes = new QueryExpression()
            {
                EntityName = "lead",
                ColumnSet = new ColumnSet(true),
                Criteria = new FilterExpression(),
                TopCount = topCount
            };
            leadattributes.Criteria = new FilterExpression();
            leadattributes.Criteria.AddCondition("statuscode", ConditionOperator.Equal, statusReason);
            //leadattributes.Criteria.AddCondition("statecode", ConditionOperator.Equal, status);
            leadattributes.Criteria.AddCondition("new_ismandatory", ConditionOperator.Equal, isYes);
            //leadattributes.Criteria.AddCondition("new_is_lir_contacted", ConditionOperator.Null);
            //leadattributes.Criteria.AddCondition("new_is_renewalleadscheckcomplete", ConditionOperator.Null);
            //leadattributes.Criteria.AddCondition("new_renewalleadcheckrequired", ConditionOperator.NotNull);
            leadattributes.Criteria.AddCondition("new_pendingvalidationstepsortorder", ConditionOperator.Equal, pendingValidationStepSortOrder);
            leadattributes.AddOrder("prioritycode", OrderType.Descending);
            leadattributes.AddOrder("createdon", OrderType.Ascending);

            leadscoll = _orgService.RetrieveMultiple(leadattributes);

            if (leadscoll != null && leadscoll.Entities.Count > 0)
            {
                foreach (Entity entityObject in leadscoll.Entities)
                {
                    Leads.Add(new CsLeads
                    {
                        LeadID = entityObject.Contains("leadid") ? new System.Guid(entityObject["leadid"].ToString()) : Guid.Empty,
                        AgreementID = entityObject.Contains("new_agreementid") ? entityObject["new_agreementid"].ToString() : string.Empty
                    });

                }
            }
            return Leads;
        }
        #endregion

        #region Logic for CLAS Integration

        /// <summary>
        /// To Retrieve CLAS details from CRM
        /// </summary>
        /// <returns></returns>
        public List<CsLeads> GetIAPSLeadsForCLAS()
        {
            _log.LogInformation("Execution started: ", "GetIAPSLeadsForCLAS");
            List<CsLeads> Leads = new List<CsLeads>();
            EntityCollection leadscoll = new EntityCollection();
            int statusReason = Convert.ToInt32(100000008);
            // CLASLookup Pending Validation Step
            int pendingValidationStepSortOrder = Convert.ToInt32(100000001);
            int status = Convert.ToInt32(0);
            int topCount = 1000;

            QueryExpression leadattributes = new QueryExpression()
            {
                EntityName = "lead",
                ColumnSet = new ColumnSet(true),
                Criteria = new FilterExpression(),
                TopCount = topCount
            };
            leadattributes.Criteria.AddCondition("statuscode", ConditionOperator.Equal, statusReason);
            // leadattributes.Criteria.AddCondition("new_is_lir_contacted", ConditionOperator.NotNull);
            // leadattributes.Criteria.AddCondition("new_mssalestpid", ConditionOperator.NotNull);
            // leadattributes.Criteria.AddCondition("new_isclascontacted", ConditionOperator.Null);
            leadattributes.Criteria.AddCondition("new_pendingvalidationstepsortorder", ConditionOperator.Equal, pendingValidationStepSortOrder);
            leadattributes.AddOrder("prioritycode", OrderType.Descending);
            leadattributes.AddOrder("createdon", OrderType.Ascending);

            leadscoll = _orgService.RetrieveMultiple(leadattributes);
            
            if (leadscoll != null && leadscoll.Entities.Count > 0)
            {
                foreach (Entity entityObject in leadscoll.Entities)
                {
                    Leads.Add(new CsLeads
                    {
                        LeadID = entityObject.Contains("leadid") ? new System.Guid(entityObject["leadid"].ToString()) : Guid.Empty,
                        MSSalesTPID = entityObject.Contains("new_mssalestpid") ? entityObject["new_mssalestpid"].ToString() : Convert.ToString(0),
                        ImportedAccountName = entityObject.Contains("new_importedaccount") ? entityObject["new_importedaccount"].ToString() : string.Empty,
                        Address = entityObject.Contains("address1_line1") ? entityObject["address1_line1"].ToString() : string.Empty,
                        City = entityObject.Contains("address1_city") ? entityObject["address1_city"].ToString() : string.Empty,
                        PostalCode = entityObject.Contains("address1_postalcode") ? entityObject["address1_postalcode"].ToString() : string.Empty,
                        State = entityObject.Contains("address1_stateorprovince") ? entityObject["address1_stateorprovince"].ToString() : string.Empty,
                        WebsiteURL = entityObject.Contains("websiteurl") ? entityObject["websiteurl"].ToString() : string.Empty,
                        AddressSource = entityObject.Contains("new_addresssource") ? entityObject["new_addresssource"].ToString() : string.Empty,
                        Is_LIR_Contacted = entityObject.Contains("new_is_lir_contacted") ? (OptionSetValue)entityObject.Attributes["new_is_lir_contacted"] : null

                    });
                }
            }
            return Leads;
        }

        /// <summary>
        /// To Insert Leads in CLAS Input table for Processing
        /// </summary>
        public void InsertCLASDetailsIntoTable()
        {
            DateTime StartTime = DateTime.UtcNow;
            Stopwatch timer = new Stopwatch();
            timer.Start();
            _log.LogInformation("Execution started: ", "InsertCLASDetails");
            int i = 0;
            List<CsLeads> leadList = GetIAPSLeadsForCLAS();
            DataTable dt = new DataTable();
            try
            {
                if (leadList.Count > 0)
                {
                    DataTable _dtCLASDetails = new DataTable();
                    _dtCLASDetails.Columns.Add("Id", typeof(int));
                    _dtCLASDetails.Columns.Add("LeadId", typeof(Guid));
                    _dtCLASDetails.Columns.Add("TPID", typeof(long));
                    _dtCLASDetails.Columns.Add("AccountName", typeof(string));
                    _dtCLASDetails.Columns.Add("CLASStartDate", typeof(DateTime));
                    _dtCLASDetails.Columns.Add("AddressLine1_Staging", typeof(string));
                    _dtCLASDetails.Columns.Add("City_Staging", typeof(string));
                    _dtCLASDetails.Columns.Add("State_Staging", typeof(string));
                    _dtCLASDetails.Columns.Add("PostalCode_Staging", typeof(string));
                    _dtCLASDetails.Columns.Add("Domain_Staging", typeof(string));
                    _dtCLASDetails.Columns.Add("AddressSource", typeof(string));
                    _dtCLASDetails.Columns.Add("ProcessStartTime", typeof(string));
                    _dtCLASDetails.Columns.Add("Is_LIR_Contacted", typeof(string));
                    foreach (var lead in leadList)
                    {
                        i++;
                        DataRow drCLAS = _dtCLASDetails.NewRow();

                        drCLAS["LeadId"] = lead.LeadID;
                        drCLAS["TPID"] = lead.MSSalesTPID == "" ? null : lead.MSSalesTPID;
                        drCLAS["AccountName"] = string.IsNullOrEmpty(lead.ImportedAccountName) ? null : lead.ImportedAccountName;
                        drCLAS["CLASStartDate"] = DateTime.UtcNow;
                        drCLAS["AddressLine1_Staging"] = lead.Address;
                        drCLAS["City_Staging"] = lead.City;
                        drCLAS["State_Staging"] = lead.State;
                        drCLAS["PostalCode_Staging"] = lead.PostalCode;
                        drCLAS["Domain_Staging"] = lead.WebsiteURL;
                        drCLAS["AddressSource"] = lead.AddressSource;
                        drCLAS["ProcessStartTime"] = StartTime.ToString();
                        if (lead.Is_LIR_Contacted != null)
                            drCLAS["Is_LIR_Contacted"] = lead.Is_LIR_Contacted.Value.ToString();

                        _dtCLASDetails.Rows.Add(drCLAS);

                        if (i == leadList.Count)
                        {
                            string tableName = "[dbo].[SSIS_Staging_CLAS]";
                            _manager.InsertDataIntoSQLDatabase(tableName, _dtCLASDetails);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _log.LogError("InsertCLASDetails", "Failed", ex);
            }
            finally
            {
                _manager.Dispose();
                _log.LogInformation(timer.Elapsed.TotalSeconds.ToString());
                timer.Stop();
                DateTime EndTime = DateTime.UtcNow;
                SqlConnection con = new SqlConnection(LeadValidationFunctions._sqlConnection);
                ////AuthenticationResult authenticationResult = _manager.AADAunthenticationResult();
                con.AccessToken = Manager.getAccessToken(System.Environment.GetEnvironmentVariable("ResourceId"), System.Environment.GetEnvironmentVariable("VTCPManagedIdentity"), true).Result;

                SqlCommand SqlCommands = new SqlCommand();
                SqlCommands = new SqlCommand("AzureFunctionsDataLoad", con);
                SqlCommands.CommandType = CommandType.StoredProcedure;
                SqlCommands.Parameters.AddWithValue("@FunctionName", "CLASIntegrationInput");
                SqlCommands.Parameters.AddWithValue("@ProcessStartTime", StartTime.ToString());
                SqlCommands.Parameters.AddWithValue("@FunctionStartTime", StartTime.ToString());
                SqlCommands.Parameters.AddWithValue("@FunctionEndTime", EndTime.ToString());
                SqlCommands.Parameters.AddWithValue("@InitialRowsAffected", leadList.Count.ToString());
                SqlCommands.Parameters.AddWithValue("@TotalRowsAffected", leadList.Count.ToString());
                con.Open();
                SqlCommands.ExecuteNonQuery();
                con.Close();
            }
        }
        /// <summary>
        /// To Update CLAS output to CRM
        /// </summary>
        public void RetrieveCLASRecordsFromTable()
        {
            DateTime StartTime = DateTime.UtcNow;
            int batch = 0;
            ExecuteMultipleSettings excuteMultipleSettings = new ExecuteMultipleSettings();
            excuteMultipleSettings.ContinueOnError = true;
            excuteMultipleSettings.ReturnResponses = true;

            ExecuteMultipleRequest updateLeadsRequest = new ExecuteMultipleRequest();
            updateLeadsRequest.Requests = new OrganizationRequestCollection();
            updateLeadsRequest.Settings = excuteMultipleSettings;
            string query = "select *,s.AddressLine1_Staging,s.City_Staging,s.State_Staging,s.PostalCode_Staging,s.Domain_Staging,s.Is_LIR_Contacted from [dbo].[SSIS_Output_CLAS]  o (nolock)  join [dbo].[SSIS_Staging_CLAS] s  (nolock)  on o.LeadId=s.LeadId ";
            DataTable dt = _manager.RetrieveDatafromSQLDatabase(query);
            string ProcessStartTime = string.Empty;
            if (dt != null && dt.Rows.Count > 0)
            {
                foreach (DataRow row in dt.Rows)
                {
                    batch++;
                    ProcessStartTime = Convert.ToString(row["ProcessStartTime"]);
                    string LeadId = row["LeadId"].ToString();
                    string TPID = row["TPID"].ToString();
                    DateTime CLASStartDate = Convert.ToDateTime(row["CLASStartDate"]);
                    DateTime CLASEndDate = DateTime.Now;
                    string SubSegmentName = Convert.ToString(row["SubSegmentName"]);
                    string Domain = Convert.ToString(row["Domain"]);
                    string CityName = Convert.ToString(row["CityName"]);
                    string StateName = Convert.ToString(row["StateName"]);
                    string PostalCodeName = Convert.ToString(row["PostalCodeName"]);
                    string Address1 = Convert.ToString(row["Address1"]);
                    string IsVLAllocation = Convert.ToString(row["IsVLAllocation"]);
                    // string TopUnmanaged_ISS = Convert.ToString(row["TopUnmanaged-ISS"]);
                    string TopUnmanaged_DSR = Convert.ToString(row["TopUnmanaged-DSR"]);
                    string TopUnmanaged_CSM = Convert.ToString(row["TopUnmanaged-CSM"]);
                    string IsMALDomain = Convert.ToString(row["IsMALDomain"]);
                    string IsMALNameExactMatch = Convert.ToString(row["IsMALNameExactMatch"]);
                    string IsNRO = Convert.ToString(row["IsNRO"]);
                    string IsMSPP = Convert.ToString(row["IsMSPP"]);
                    string IsTPNameWaste = Convert.ToString(row["IsTPNameWaste"]);
                    string IsEOS_Office = Convert.ToString(row["IsEOS_Office"]);
                    string IsEOS_WinClient = Convert.ToString(row["IsEOS_WinClient"]);
                    string IsEOS_WinServer = Convert.ToString(row["IsEOS_WinServer"]);
                    string IsEOS_SQLServer = Convert.ToString(row["IsEOS_SQLServer"]);
                    string CustomerTPIDList = Convert.ToString(row["CustomerTPIDList"]);
                    string AddressLine1_Staging = Convert.ToString(row["AddressLine1_Staging"]);
                    string City_Staging = Convert.ToString(row["City_Staging"]);
                    string State_Staging = Convert.ToString(row["State_Staging"]);
                    string PostalCode_Staging = Convert.ToString(row["PostalCode_Staging"]);
                    string Domain_Staging = Convert.ToString(row["Domain_Staging"]);
                    string CLASSMCType = Convert.ToString(row["CLASSMCType"]);
                    string CLASPropensityHighLevel = Convert.ToString(row["CLASPropensityHighLevel"]);
                    string CLASPropensityDetails = Convert.ToString(row["CLASPropensityDetails"]);
                    string CLASProductOwnershipDetails = Convert.ToString(row["CLASProductOwnershipDetails"]);
                    string Is_LIR_Contacted = Convert.ToString(row["Is_LIR_Contacted"]);
                    string CLASIsNonProfit = Convert.ToString(row["IsNonProfit"]);

                    Entity objLeadEntity = new Entity("lead");
                    objLeadEntity.Id = new Guid(LeadId);

                    objLeadEntity["new_mssalestpid"] = TPID;
                    objLeadEntity["new_clas_startdate"] = CLASStartDate;
                    objLeadEntity["new_clas_enddate"] = CLASEndDate;
                    objLeadEntity["new_clas_subsegment"] = SubSegmentName;
                    objLeadEntity["new_clas_vlallocation"] = IsVLAllocation;
                    objLeadEntity["new_clas_topunmanageddsr"] = TopUnmanaged_DSR;
                    objLeadEntity["new_clas_topunmanagedcsm"] = TopUnmanaged_CSM;
                    // objLeadEntity["new_clas_topunmanagediss"] = TopUnmanaged_ISS;
                    objLeadEntity["new_clas_maldomain"] = IsMALDomain;
                    objLeadEntity["new_clas_malexactnamematch"] = IsMALNameExactMatch;
                    objLeadEntity["new_clas_nro"] = IsNRO;
                    objLeadEntity["new_clas_mspp"] = IsMSPP;
                    objLeadEntity["new_clas_tpnamewaste"] = IsTPNameWaste;
                    objLeadEntity["new_eos_office"] = IsEOS_Office;
                    objLeadEntity["new_eos_winclient"] = IsEOS_WinClient;
                    objLeadEntity["new_eos_winserver"] = IsEOS_WinServer;
                    objLeadEntity["new_eos_sqlserver"] = IsEOS_SQLServer;
                    objLeadEntity["new_isclascontacted"] = new OptionSetValue(Convert.ToInt32(100000000));
                    objLeadEntity["new_leadupdatedby"] = "CLASIntegration Output Job";
                    objLeadEntity["new_tpidlist"] = !string.IsNullOrWhiteSpace(CustomerTPIDList) ? _manager.LimitCharacterCount(CustomerTPIDList, 4000).Trim().Trim(',') : string.Empty;
                    objLeadEntity["new_classmctype"] = CLASSMCType;
                    objLeadEntity["new_claspropensityhighlevel"] = CLASPropensityHighLevel;
                    objLeadEntity["new_claspropensitydetails"] = CLASPropensityDetails != string.Empty ? _manager.LimitCharacterCount(CLASPropensityDetails.Replace("N/A", "Unknown"), 4000) : string.Empty;
                    objLeadEntity["new_clasproductownershipdetails"] = _manager.LimitCharacterCount(CLASProductOwnershipDetails, 4000);
                    objLeadEntity["new_clas_isnonprofit"] = CLASIsNonProfit;

                    if (!string.IsNullOrWhiteSpace(Is_LIR_Contacted))
                    {
                        objLeadEntity["new_pendingvalidationstepsortorder"] = new OptionSetValue(Convert.ToInt32(100000003)); ;
                    }
                    else
                    {
                        objLeadEntity["new_pendingvalidationstepsortorder"] = new OptionSetValue(Convert.ToInt32(100000002));
                    }

                    if (row["AddressSource"].ToString() != "Imported By User")
                    {
                        objLeadEntity["websiteurl"] = Domain;
                        objLeadEntity["address1_city"] = _manager.ReplaceHexadecimalSymbols(CityName);
                        objLeadEntity["address1_stateorprovince"] = _manager.ReplaceHexadecimalSymbols(StateName);
                        objLeadEntity["address1_postalcode"] = PostalCodeName;
                        objLeadEntity["address1_line1"] = _manager.ReplaceHexadecimalSymbols(Address1);
                        if (!string.IsNullOrWhiteSpace(Domain) || !string.IsNullOrWhiteSpace(CityName) ||
                            !string.IsNullOrWhiteSpace(PostalCodeName) || !string.IsNullOrWhiteSpace(StateName) ||
                            !string.IsNullOrWhiteSpace(Address1))
                        {
                            objLeadEntity["new_addresssource"] = "Cloud Ascent";
                        }
                    }

                    var updateRequest = new UpdateRequest();
                    updateRequest.Target = objLeadEntity;
                    updateLeadsRequest.Requests.Add(updateRequest);
                    if (updateLeadsRequest.Requests.Count == 1000)
                    {
                        _manager.ExecuteBulkRequest(updateLeadsRequest, "Updated CLAS Details");
                        updateLeadsRequest.Requests.Clear();
                        batch = 0;
                    }
                }
                if (batch > 0)
                {
                    _manager.ExecuteBulkRequest(updateLeadsRequest, "Updated CLAS Details");
                    updateLeadsRequest.Requests.Clear();
                    batch = 0;
                }

            }
            DateTime EndTime = DateTime.UtcNow;
            SqlConnection con = new SqlConnection(LeadValidationFunctions._sqlConnection);
            //AuthenticationResult authenticationResult = _manager.AADAunthenticationResult();
            con.AccessToken = Manager.getAccessToken(System.Environment.GetEnvironmentVariable("ResourceId"), System.Environment.GetEnvironmentVariable("VTCPManagedIdentity"), true).Result;
            SqlCommand SqlCommands = new SqlCommand();
            SqlCommands = new SqlCommand("AzureFunctionsDataLoad", con);
            SqlCommands.CommandType = CommandType.StoredProcedure;
            SqlCommands.Parameters.AddWithValue("@FunctionName", "CLASIntegrationOutput");
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

        #region Update LIR details
        /// <summary>
        /// To Insert LIR Details
        /// </summary>
        public void InsertLIRDetailsIntoTable()
        {
            DateTime StartTime = DateTime.UtcNow;
            Stopwatch timer = new Stopwatch();
            _log.LogInformation("Execution started: ", "UpdateLirDetails");
            int i = 0;
            List<CsLeads> leadList = GetIAPSLeadsForLIR();
            DataTable dt = new DataTable();
            try
            {
                if (leadList.Count > 0)
                {
                    DataTable _dtAccountDetails = new DataTable();
                    _dtAccountDetails.Columns.Add("Leadref", typeof(Guid));
                    _dtAccountDetails.Columns.Add("AgreementId", typeof(string));
                    _dtAccountDetails.Columns.Add("TPID", typeof(string));
                    _dtAccountDetails.Columns.Add("LIRStartDate", typeof(DateTime));
                    _dtAccountDetails.Columns.Add("ContactSource", typeof(string));
                    _dtAccountDetails.Columns.Add("IsAutoCreatedLead", typeof(string));
                    _dtAccountDetails.Columns.Add("ProcessStartTime", typeof(string));
                    _dtAccountDetails.Columns.Add("TPIDList", typeof(string));

                    foreach (var lead in leadList)
                    {
                        i++;
                        DataRow drAccountNumbers = _dtAccountDetails.NewRow();
                        drAccountNumbers["Leadref"] = lead.LeadID;
                        drAccountNumbers["AgreementId"] = string.IsNullOrEmpty(lead.AgreementID) ? null : lead.AgreementID;
                        drAccountNumbers["TPID"] = string.IsNullOrEmpty(lead.MSSalesTPID) ? null : lead.MSSalesTPID;
                        drAccountNumbers["LIRStartDate"] = DateTime.UtcNow;
                        drAccountNumbers["ContactSource"] = string.IsNullOrEmpty(lead.ContactSource) ? null : lead.ContactSource;
                        drAccountNumbers["IsAutoCreatedLead"] = string.IsNullOrEmpty(lead.IsAutoCreatedLead) ? null : lead.IsAutoCreatedLead;
                        drAccountNumbers["ProcessStartTime"] = StartTime.ToString();
                        drAccountNumbers["TPIDList"] = string.IsNullOrEmpty(lead.CustomerTPIDList) ? null : lead.CustomerTPIDList;
                        _dtAccountDetails.Rows.Add(drAccountNumbers);
                        if (i == leadList.Count)
                        {
                            string tableName = "[dbo].[SSIS_Staging_LIR]";
                            _manager.InsertDataIntoSQLDatabase(tableName, _dtAccountDetails);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _log.LogError("UpdateMalDetails", "Failed", ex);
            }
            finally
            {
                _manager.Dispose();
                _log.LogInformation(timer.Elapsed.TotalSeconds.ToString());
                timer.Stop();
                DateTime EndTime = DateTime.UtcNow;
                SqlConnection con = new SqlConnection(LeadValidationFunctions._sqlConnection);
                //AuthenticationResult authenticationResult = _manager.AADAunthenticationResult();
                con.AccessToken = Manager.getAccessToken(System.Environment.GetEnvironmentVariable("ResourceId"), System.Environment.GetEnvironmentVariable("VTCPManagedIdentity"), true).Result;
                SqlCommand SqlCommands = new SqlCommand();
                SqlCommands = new SqlCommand("AzureFunctionsDataLoad", con);
                SqlCommands.CommandType = CommandType.StoredProcedure;
                SqlCommands.Parameters.AddWithValue("@FunctionName", "LIRIntegrationInput");
                SqlCommands.Parameters.AddWithValue("@ProcessStartTime", StartTime.ToString());
                SqlCommands.Parameters.AddWithValue("@FunctionStartTime", StartTime.ToString());
                SqlCommands.Parameters.AddWithValue("@FunctionEndTime", EndTime.ToString());
                SqlCommands.Parameters.AddWithValue("@InitialRowsAffected", leadList.Count.ToString());
                SqlCommands.Parameters.AddWithValue("@TotalRowsAffected", leadList.Count.ToString());
                con.Open();
                SqlCommands.ExecuteNonQuery();
                con.Close();
            }
        }

        /// <summary>
        /// To get the list of leads for LIR details updation
        /// </summary>
        /// <returns></returns>
        public List<CsLeads> GetIAPSLeadsForLIR()
        {
            _log.LogInformation("Execution started GetIAPSLeadsForLIR");
            List<CsLeads> Leads = new List<CsLeads>();
            EntityCollection leadscoll = new EntityCollection();
            int statusReason = Convert.ToInt32(100000008);
            int status = Convert.ToInt32(0);
            int clasStatus = Convert.ToInt32(100000001);
            // LIR Contact Check Pending Validation Step
            int pendingValidationStepSortOrder = Convert.ToInt32(100000002);
            int topCount = 0;
            topCount = 1000;
            string promotionalType = "promotional";

            _log.LogInformation("user id: ", "GetIAPSLeadsForLIR");
            QueryExpression leadattributes = new QueryExpression()
            {
                EntityName = "lead",
                ColumnSet = new ColumnSet(true),
                Criteria = new FilterExpression(),
                TopCount = topCount
            };
            leadattributes.Criteria = new FilterExpression();
            leadattributes.Criteria.AddCondition("statuscode", ConditionOperator.Equal, statusReason);
            leadattributes.Criteria.AddCondition("new_ismandatory", ConditionOperator.Equal, isYes);
            leadattributes.Criteria.AddCondition("new_pendingvalidationstepsortorder", ConditionOperator.Equal, pendingValidationStepSortOrder);
            leadattributes.AddOrder("prioritycode", OrderType.Descending);
            leadattributes.AddOrder("createdon", OrderType.Ascending);

            //leadattributes.Criteria.AddCondition("new_is_lir_contacted", ConditionOperator.Null);
            //FilterExpression childFilter = leadattributes.Criteria.AddFilter(LogicalOperator.Or);
            //childFilter.AddCondition("new_mssalestpid", ConditionOperator.NotNull);
            //childFilter.AddCondition("new_agreementid", ConditionOperator.NotNull);

            //FilterExpression ClasFilter = leadattributes.Criteria.AddFilter(LogicalOperator.Or);
            //ClasFilter.AddCondition("new_isclascontacted", ConditionOperator.Null);
            //ClasFilter.AddCondition("new_isclascontacted", ConditionOperator.Equal, clasStatus);

            //FilterExpression RenewalsCheckFilter = leadattributes.Criteria.AddFilter(LogicalOperator.Or);
            //RenewalsCheckFilter.AddCondition("new_renewalleadcheckrequired", ConditionOperator.Null);
            //RenewalsCheckFilter.AddCondition("new_is_renewalleadscheckcomplete", ConditionOperator.NotNull);

            leadscoll = _orgService.RetrieveMultiple(leadattributes);

            if (leadscoll != null && leadscoll.Entities.Count > 0)
            {
                foreach (Entity entityObject in leadscoll.Entities)
                {
                    EntityReference campaignType = entityObject.GetAttributeValue<EntityReference>("new_campaigntype");
                    if (campaignType.Name.ToLower() != promotionalType)
                    {
                        Leads.Add(new CsLeads
                        {
                            LeadID = entityObject.Contains("leadid") ? new System.Guid(entityObject["leadid"].ToString()) : Guid.Empty,
                            MSSalesTPID = entityObject.Contains("new_mssalestpid") ? entityObject["new_mssalestpid"].ToString() : string.Empty,
                            AgreementID = entityObject.Contains("new_agreementid") ? entityObject["new_agreementid"].ToString() : string.Empty,
                            ContactSource = entityObject.Contains("new_contactsource") ? entityObject["new_contactsource"].ToString() : string.Empty,
                            IsAutoCreatedLead = entityObject.Contains("new_isautocreatedlead") ? entityObject["new_isautocreatedlead"].ToString() : string.Empty,
                            CustomerTPIDList = entityObject.Contains("new_tpidlist") ? entityObject["new_tpidlist"].ToString() : string.Empty
                        });
                    }
                }
            }
            return Leads;
        }

        /// <summary>
        /// To get and update LIR details to CRM
        /// </summary>
        public void RetrieveLIRRecordsFromTable()
        {
            DateTime StartTime = DateTime.UtcNow;
            int batch = 0;
            ExecuteMultipleSettings excuteMultipleSettings = new ExecuteMultipleSettings();
            excuteMultipleSettings.ContinueOnError = true;
            excuteMultipleSettings.ReturnResponses = true;

            ExecuteMultipleRequest updateLeadsRequest = new ExecuteMultipleRequest();
            updateLeadsRequest.Requests = new OrganizationRequestCollection();
            updateLeadsRequest.Settings = excuteMultipleSettings;

            string query = "select O.*,I.IsAutoCreatedLead from [dbo].[SSIS_Output_LIR] (nolock) O join [dbo].[SSIS_Staging_LIR]  (nolock) I on O.[Leadref]=I.[Leadref]";
            DataTable dt = _manager.RetrieveDatafromSQLDatabase(query);
            string ProcessStartTime = string.Empty;
            if (dt != null && dt.Rows.Count > 0)
            {
                foreach (DataRow row in dt.Rows)
                {
                    batch++;

                    string LeadRefId = row["Leadref"].ToString();
                    DateTime lirStartTime = Convert.ToDateTime(row["LIRStartDate"].ToString());
                    DateTime lirEndDate = DateTime.Now;
                    ProcessStartTime = Convert.ToString(row["ProcessStartTime"]);
                    Entity objLeadEntity = new Entity("lead");
                    objLeadEntity.Id = new Guid(LeadRefId);

                    //string stringDate = Convert.ToString(row["AgreementExpirationDate"]);
                    //if (row["CRMContactSource"] != null && Convert.ToString(row["CRMContactSource"]) == "Included With Initial Upload")
                    //{

                    //    if (!string.IsNullOrEmpty(stringDate) && row["IsAutoCreatedLead"] != null && Convert.ToString(row["IsAutoCreatedLead"]).ToLower() != "true")
                    //    {
                    //        objLeadEntity["new_agreementexpirationdate"] = Convert.ToDateTime(Convert.ToString(row["AgreementExpirationDate"]));
                    //    }
                    //}
                    //else
                    //{
                    objLeadEntity["firstname"] = Convert.ToString(row["ContactFirstName"]);
                    objLeadEntity["lastname"] = Convert.ToString(row["ContactLastName"]);
                    objLeadEntity["mobilephone"] = Convert.ToString(row["ContactPhone"]);
                    objLeadEntity["emailaddress1"] = Convert.ToString(row["ContactEmailAddress"]);
                    objLeadEntity["jobtitle"] = Convert.ToString(row["ContactJobTitle"]);
                    //if (!string.IsNullOrEmpty(stringDate) && row["IsAutoCreatedLead"] != null && Convert.ToString(row["IsAutoCreatedLead"]).ToLower() != "true")
                    //{
                    //    objLeadEntity["new_agreementexpirationdate"] = Convert.ToDateTime(Convert.ToString(row["AgreementExpirationDate"]));
                    //}
                    objLeadEntity["new_contactsource"] = Convert.ToString(row["ContactSource"]);

                    // }
                    string lirStatus = Convert.ToString(row["LIRCompleteStatus"]);

                    if (lirStatus.ToLower() == "no")
                    {
                        objLeadEntity["new_is_lir_contacted"] = new OptionSetValue(isNo);
                    }
                    else
                    {
                        objLeadEntity["new_is_lir_contacted"] = new OptionSetValue(isYes);
                    }

                    objLeadEntity["new_validationphasestartdatetime"] = lirStartTime;
                    objLeadEntity["new_validationphaseenddatetime"] = lirEndDate;
                    objLeadEntity["new_leadupdatedby"] = "LIRIntegration Output Job";
                    objLeadEntity["new_pendingvalidationstepsortorder"] = new OptionSetValue(Convert.ToInt32(100000003)); ;

                    var updateRequest = new UpdateRequest();
                    updateRequest.Target = objLeadEntity;
                    updateLeadsRequest.Requests.Add(updateRequest);
                    if (updateLeadsRequest.Requests.Count == 1000)
                    {
                        _manager.ExecuteBulkRequest(updateLeadsRequest, "Updated LIR Details");
                        updateLeadsRequest.Requests.Clear();
                        batch = 0;
                    }
                }
                if (batch > 0)
                {
                    _manager.ExecuteBulkRequest(updateLeadsRequest, "Updated LIR Details");
                    updateLeadsRequest.Requests.Clear();
                    batch = 0;
                }

            }
            DateTime EndTime = DateTime.UtcNow;
            SqlConnection con = new SqlConnection(LeadValidationFunctions._sqlConnection);
            //AuthenticationResult authenticationResult = _manager.AADAunthenticationResult();
            con.AccessToken = Manager.getAccessToken(System.Environment.GetEnvironmentVariable("ResourceId"), System.Environment.GetEnvironmentVariable("VTCPManagedIdentity"), true).Result;
            SqlCommand SqlCommands = new SqlCommand();
            SqlCommands = new SqlCommand("AzureFunctionsDataLoad", con);
            SqlCommands.CommandType = CommandType.StoredProcedure;
            SqlCommands.Parameters.AddWithValue("@FunctionName", "LIRIntegrationOutput");
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

        #region Logic for MAL Details updation

        /// <summary>
        /// GetIAPSLead records For MAL Updation
        /// </summary>
        public List<CsLeads> GetIAPSLeadsForMAL()
        {
            _log.LogInformation("Execution started: ", "GetIAPSLeadsForMAL");
            List<CsLeads> Leads = new List<CsLeads>();
            EntityCollection leadscoll = new EntityCollection();
            int statusReason = Convert.ToInt32(100000008);
            int status = Convert.ToInt32(0);
            // Account Match Pending Validation Step 
            int pendingValidationStepSortOrder = Convert.ToInt32(100000003);
            int topCount = 0;
            topCount = 1000;
            _log.LogInformation("user id: ", "GetIAPSLeadsForMAL");
            QueryExpression leadattributes = new QueryExpression()
            {
                EntityName = "lead",
                ColumnSet = new ColumnSet(true),
                Criteria = new FilterExpression(),
                TopCount = topCount
            };
            leadattributes.Criteria.AddCondition("statuscode", ConditionOperator.Equal, statusReason);
            //leadattributes.Criteria.AddCondition("new_isclascontacted", ConditionOperator.NotNull);
            //leadattributes.Criteria.AddCondition("new_accountmatchcomplete", ConditionOperator.Null);
            leadattributes.Criteria.AddCondition("new_pendingvalidationstepsortorder", ConditionOperator.Equal, pendingValidationStepSortOrder);
            leadattributes.AddOrder("prioritycode", OrderType.Descending);
            leadattributes.AddOrder("createdon", OrderType.Ascending);

            leadscoll = _orgService.RetrieveMultiple(leadattributes);

            if (leadscoll != null && leadscoll.Entities.Count > 0)
            {
                foreach (Entity entityObject in leadscoll.Entities)
                {
                    Leads.Add(new CsLeads
                    {
                        LeadID = entityObject.Contains("leadid") ? new System.Guid(entityObject["leadid"].ToString()) : Guid.Empty,
                        Is_LIR_Contacted = entityObject.Contains("new_is_lir_contacted") ? (OptionSetValue)entityObject.Attributes["new_is_lir_contacted"] : null,
                        Is_MSX_Contacted = entityObject.Contains("new_is_msx_contacted") ? (OptionSetValue)entityObject.Attributes["new_is_msx_contacted"] : null,
                        MSSalesTPID = entityObject.Contains("new_mssalestpid") ? entityObject["new_mssalestpid"].ToString() : string.Empty,
                        MSXAccountID = entityObject.Contains("new_msxaccountid") ? entityObject["new_msxaccountid"].ToString() : string.Empty,
                        ImportedAccountName = entityObject.Contains("new_importedaccount") ? entityObject["new_importedaccount"].ToString() : string.Empty,
                        Subsidiary = entityObject.Contains("new_subsidiary") ? entityObject.GetAttributeValue<EntityReference>("new_subsidiary").Name : string.Empty,
                        Is_MAL = entityObject.Contains("new_is_mal") ? (OptionSetValue)entityObject.Attributes["new_is_mal"] : null,
                        MSXAccountIdSource = entityObject.Contains("new_msxaccountidsrc") ? entityObject["new_msxaccountidsrc"].ToString() : string.Empty,
                        City = entityObject.Contains("address1_city") ? entityObject["address1_city"].ToString() : string.Empty,
                        CustomerTPIDList = entityObject.Contains("new_tpidlist") ? entityObject["new_tpidlist"].ToString() : string.Empty,
                        Address = entityObject.Contains("address1_line1") ? entityObject["address1_line1"].ToString() : string.Empty,
                        State = entityObject.Contains("address1_stateorprovince") ? entityObject["address1_stateorprovince"].ToString() : string.Empty,
                        PostalCode = entityObject.Contains("address1_postalcode") ? entityObject["address1_postalcode"].ToString() : string.Empty,
                        WebsiteURL = entityObject.Contains("websiteurl") ? entityObject["websiteurl"].ToString() : string.Empty
                    });
                }
            }
            return Leads;
        }

        /// <summary>
        /// To Insert MAL Details
        /// </summary>
        public void InsertMalDetailsIntoTable()
        {
            DateTime StartTime = DateTime.UtcNow;
            Stopwatch timer = new Stopwatch();
            timer.Start();
            _log.LogInformation("Execution started: ", "UpdateMalDetails");
            int i = 0;
            List<CsLeads> leadList = GetIAPSLeadsForMAL();
            DataTable dt = new DataTable();
            try
            {
                if (leadList.Count > 0)
                {
                    DataTable _dtAccountDetails = new DataTable();
                    _dtAccountDetails.Columns.Add("leadid", typeof(Guid));
                    _dtAccountDetails.Columns.Add("AccountNumber", typeof(string));
                    _dtAccountDetails.Columns.Add("TPID", typeof(string));
                    _dtAccountDetails.Columns.Add("AccountName", typeof(string));
                    _dtAccountDetails.Columns.Add("MALStartDate", typeof(DateTime));
                    _dtAccountDetails.Columns.Add("MSXAccountIDSource", typeof(string));
                    _dtAccountDetails.Columns.Add("Subsidiary", typeof(string));
                    _dtAccountDetails.Columns.Add("City", typeof(string));
                    _dtAccountDetails.Columns.Add("CustomerTPIDList", typeof(string));
                    _dtAccountDetails.Columns.Add("Address", typeof(string));
                    _dtAccountDetails.Columns.Add("State", typeof(string));
                    _dtAccountDetails.Columns.Add("PostalCode", typeof(string));
                    _dtAccountDetails.Columns.Add("WebsiteURL", typeof(string));
                    _dtAccountDetails.Columns.Add("ProcessStartTime", typeof(string));
                    foreach (var lead in leadList)
                    {
                        i++;
                        DataRow drAccountNumbers = _dtAccountDetails.NewRow();
                        drAccountNumbers["leadid"] = lead.LeadID;
                        drAccountNumbers["AccountNumber"] = string.IsNullOrEmpty(lead.MSXAccountID) ? null : lead.MSXAccountID;
                        drAccountNumbers["TPID"] = lead.MSSalesTPID == "" ? null : lead.MSSalesTPID;
                        drAccountNumbers["AccountName"] = string.IsNullOrEmpty(lead.ImportedAccountName) ? null : lead.ImportedAccountName;
                        drAccountNumbers["MALStartDate"] = DateTime.UtcNow;
                        drAccountNumbers["MSXAccountIDSource"] = string.IsNullOrEmpty(lead.MSXAccountIdSource) ? null : lead.MSXAccountIdSource;
                        drAccountNumbers["Subsidiary"] = string.IsNullOrEmpty(lead.Subsidiary) ? null : lead.Subsidiary;
                        drAccountNumbers["City"] = string.IsNullOrEmpty(lead.City) ? null : lead.City;
                        drAccountNumbers["CustomerTPIDList"] = string.IsNullOrEmpty(lead.CustomerTPIDList) ? null : lead.CustomerTPIDList;
                        drAccountNumbers["Address"] = string.IsNullOrEmpty(lead.Address) ? null : lead.Address;
                        drAccountNumbers["State"] = string.IsNullOrEmpty(lead.State) ? null : lead.State;
                        drAccountNumbers["PostalCode"] = string.IsNullOrEmpty(lead.PostalCode) ? null : lead.PostalCode;
                        drAccountNumbers["WebsiteURL"] = string.IsNullOrEmpty(lead.WebsiteURL) ? null : lead.WebsiteURL;
                        drAccountNumbers["ProcessStartTime"] = StartTime.ToString();
                        _dtAccountDetails.Rows.Add(drAccountNumbers);
                        if (i == leadList.Count)
                        {
                            string tableName = "[dbo].[SSIS_Staging_MAL]";
                            _manager.InsertDataIntoSQLDatabase(tableName, _dtAccountDetails);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _log.LogError("UpdateMalDetails", "Failed", ex);
            }
            finally
            {
                _manager.Dispose();
                _log.LogInformation(timer.Elapsed.TotalSeconds.ToString());
                timer.Stop();
                DateTime EndTime = DateTime.UtcNow;
                SqlConnection con = new SqlConnection(LeadValidationFunctions._sqlConnection);
                //AuthenticationResult authenticationResult = _manager.AADAunthenticationResult();
                con.AccessToken = Manager.getAccessToken(System.Environment.GetEnvironmentVariable("ResourceId"), System.Environment.GetEnvironmentVariable("VTCPManagedIdentity"), true).Result;
                SqlCommand SqlCommands = new SqlCommand();
                SqlCommands = new SqlCommand("AzureFunctionsDataLoad", con);
                SqlCommands.CommandType = CommandType.StoredProcedure;
                SqlCommands.Parameters.AddWithValue("@FunctionName", "MALIntegrationInput");
                SqlCommands.Parameters.AddWithValue("@ProcessStartTime", StartTime.ToString());
                SqlCommands.Parameters.AddWithValue("@FunctionStartTime", StartTime.ToString());
                SqlCommands.Parameters.AddWithValue("@FunctionEndTime", EndTime.ToString());
                SqlCommands.Parameters.AddWithValue("@InitialRowsAffected", leadList.Count.ToString());
                SqlCommands.Parameters.AddWithValue("@TotalRowsAffected", leadList.Count.ToString());
                con.Open();
                SqlCommands.ExecuteNonQuery();
                con.Close();

            }
        }

        public void RetrieveMALRecordsFromTable()
        {
            DateTime StartTime = DateTime.UtcNow;
            int batch = 0;
            ExecuteMultipleSettings excuteMultipleSettings = new ExecuteMultipleSettings();
            excuteMultipleSettings.ContinueOnError = true;
            excuteMultipleSettings.ReturnResponses = true;

            ExecuteMultipleRequest updateLeadsRequest = new ExecuteMultipleRequest();
            updateLeadsRequest.Requests = new OrganizationRequestCollection();
            updateLeadsRequest.Settings = excuteMultipleSettings;

            string query = "select * from [dbo].[SSIS_Output_MAL] nolock";
            DataTable dt = _manager.RetrieveDatafromSQLDatabase(query);
            string ProcessStartTime = string.Empty;
            if (dt != null && dt.Rows.Count > 0)
            {
                foreach (DataRow row in dt.Rows)
                {
                    batch++;
                    string IsMalRecord = row["IsMALAccount"].ToString();
                    string LeadRefId = row["Leadid"].ToString();
                    DateTime malStartTime = Convert.ToDateTime(row["MALStartDate"].ToString());
                    DateTime malEndDate = DateTime.Now;
                    string MatchedMSXAccountSource = Convert.ToString(row["MatchedMSXAccountSource"]);
                    string MatchedMSXAccountNumber = Convert.ToString(row["MatchedMSXAccountNumber"]);
                    string MSXAccount_SubSegment = Convert.ToString(row["MSXAccount_SubSegment"]);
                    string NumActiveContacts = !string.IsNullOrWhiteSpace(Convert.ToString(row["NumActiveContacts"])) ? Convert.ToString(row["NumActiveContacts"]) : Convert.ToString(0);
                    ProcessStartTime = Convert.ToString(row["ProcessStartTime"]);
                    Entity objLeadEntity = new Entity("lead");
                    objLeadEntity.Id = new Guid(LeadRefId);
                    if (IsMalRecord == "MAL Account")
                    { objLeadEntity["new_is_mal"] = new OptionSetValue(isYes); }
                    else if (IsMalRecord == "OK - SMB/Scale Account")
                    { objLeadEntity["new_is_mal"] = new OptionSetValue(isNo); }
                    else if (IsMalRecord.ToLower() == "unknown")
                    {
                        objLeadEntity["new_is_mal"] = new OptionSetValue(isNotFound);
                    }
                    else if (IsMalRecord == "Non SMB/Scale Account")
                    {
                        objLeadEntity["new_is_mal"] = new OptionSetValue(Convert.ToInt32(100000003));
                    }

                    if (!string.IsNullOrEmpty(MatchedMSXAccountSource))
                    {
                        objLeadEntity["new_msxaccountidsrc"] = MatchedMSXAccountSource;
                    }

                    if (!string.IsNullOrWhiteSpace(MatchedMSXAccountNumber))
                    {
                        if (!string.IsNullOrEmpty(MSXAccount_SubSegment))
                        {
                            objLeadEntity["new_msxaccount_subsegment"] = MSXAccount_SubSegment;
                        }
                        objLeadEntity["new_matchedmsxaccountid"] = MatchedMSXAccountNumber;
                        objLeadEntity["new_activecontactsinmsxaccount"] = NumActiveContacts;
                    }

                    objLeadEntity["new_accountmatchcomplete"] = new OptionSetValue(Convert.ToInt32(100000000));
                    objLeadEntity["new_malcheckstarttime"] = malStartTime;
                    objLeadEntity["new_malcheckendtime"] = malEndDate;
                    objLeadEntity["new_leadupdatedby"] = "MALIntegration Output Job";
                    objLeadEntity["new_pendingvalidationstepsortorder"] = new OptionSetValue(Convert.ToInt32(100000004));
                    var updateRequest = new UpdateRequest();
                    updateRequest.Target = objLeadEntity;
                    updateLeadsRequest.Requests.Add(updateRequest);
                    if (updateLeadsRequest.Requests.Count == 1000)
                    {
                        _manager.ExecuteBulkRequest(updateLeadsRequest, "Updated MAL Details");
                        updateLeadsRequest.Requests.Clear();
                        batch = 0;
                    }
                }
                if (batch > 0)
                {
                    _manager.ExecuteBulkRequest(updateLeadsRequest, "Updated MAL Details");
                    updateLeadsRequest.Requests.Clear();
                    batch = 0;
                }
            }
            DateTime EndTime = DateTime.UtcNow;
            SqlConnection con = new SqlConnection(LeadValidationFunctions._sqlConnection);
            //AuthenticationResult authenticationResult = _manager.AADAunthenticationResult();
            con.AccessToken = Manager.getAccessToken(System.Environment.GetEnvironmentVariable("ResourceId"), System.Environment.GetEnvironmentVariable("VTCPManagedIdentity"), true).Result;
            SqlCommand SqlCommands = new SqlCommand();
            SqlCommands = new SqlCommand("AzureFunctionsDataLoad", con);
            SqlCommands.CommandType = CommandType.StoredProcedure;
            SqlCommands.Parameters.AddWithValue("@FunctionName", "MALIntegrationOutput");
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

        #region Logic for Updating MSX Details using AccountID/AccountName

        /// <summary>
        /// GetIAPSLead records with MSXID
        /// </summary>
        public List<CsLeads> GetIAPSLeads()
        {
            _log.LogInformation("Execution started: ", "GetIAPSLeads");
            List<CsLeads> Leads = new List<CsLeads>();
            EntityCollection leadscoll = new EntityCollection();
            int statusReason = Convert.ToInt32(100000008);
            int status = Convert.ToInt32(0);
            // MSX Activity Check Pending Validation Step
            int pendingValidationStepSortOrder = Convert.ToInt32(100000004);

            _log.LogInformation("user id: ", "GetIAPSLeads");
            QueryExpression leadattributes = new QueryExpression()
            {
                EntityName = "lead",
                ColumnSet = new ColumnSet(true),
                Criteria = new FilterExpression(),
                TopCount = 1000
            };
            FilterConditions(statusReason, status, leadattributes, pendingValidationStepSortOrder);
            leadscoll = _orgService.RetrieveMultiple(leadattributes);

            if (leadscoll != null && leadscoll.Entities.Count > 0)
            {
                foreach (Entity entityObject in leadscoll.Entities)
                {
                    Leads.Add(new CsLeads
                    {
                        LeadID = entityObject.Contains("leadid") ? new System.Guid(entityObject["leadid"].ToString()) : Guid.Empty,
                        Is_LIR_Contacted = entityObject.Contains("new_is_lir_contacted") ? (OptionSetValue)entityObject.Attributes["new_is_lir_contacted"] : null,
                        Is_MSX_Contacted = entityObject.Contains("new_is_msx_contacted") ? (OptionSetValue)entityObject.Attributes["new_is_msx_contacted"] : null,
                        MSSalesTPID = entityObject.Contains("new_mssalestpid") ? entityObject["new_mssalestpid"].ToString() : string.Empty,
                        MSXAccountID = entityObject.Contains("new_matchedmsxaccountid") ? entityObject["new_matchedmsxaccountid"].ToString() : string.Empty,
                        ImportedAccountName = entityObject.Contains("new_importedaccount") ? entityObject["new_importedaccount"].ToString() : string.Empty,
                        Subsidiary = entityObject.Contains("new_subsidiary") ? entityObject.GetAttributeValue<EntityReference>("new_subsidiary").Name : string.Empty,
                        Is_MAL = entityObject.Contains("new_is_mal") ? (OptionSetValue)entityObject.Attributes["new_is_mal"] : null,
                        ExistingMSXActivity = entityObject.Contains("new_existingmsxactivity") ? entityObject.GetAttributeValue<OptionSetValue>("new_existingmsxactivity") : null,
                        VendorName = entityObject.Contains("new_vendorteam") ? entityObject.GetAttributeValue<EntityReference>("new_vendorteam").Name : null
                    });
                }
            }
            return Leads;
        }

        /// <summary>
        /// Filter conditions to pull data from CRM
        /// </summary>
        /// <param name="statusReason"></param>
        /// <param name="status"></param>
        /// <param name="leadattributes"></param>
        /// <param name="flag"></param>
        public void FilterConditions(int statusReason, int status, QueryExpression leadattributes, int pendingValidationStepSortOrder)
        {
            _log.LogInformation("Execution started: ", "FilterConditions");
            leadattributes.Criteria.AddCondition("statuscode", ConditionOperator.Equal, statusReason);
            leadattributes.Criteria.AddCondition("new_importedaccount", ConditionOperator.NotNull);
            //leadattributes.Criteria.AddCondition("new_is_msx_contacted", ConditionOperator.Null);
            //leadattributes.Criteria.AddCondition("new_accountmatchcomplete", ConditionOperator.Equal, isYes);
            leadattributes.Criteria.AddCondition("new_pendingvalidationstepsortorder", ConditionOperator.Equal, pendingValidationStepSortOrder);
            leadattributes.AddOrder("prioritycode", OrderType.Descending);
            leadattributes.AddOrder("createdon", OrderType.Ascending);

        }

        /// <summary>
        /// To update MSX Details by Account Name
        /// </summary>
        /// <param name="flag"></param>
        public void InsertMSXDetailsIntoInputTable()
        {
            DateTime StartTime = DateTime.UtcNow;
            _log.LogInformation("Execution started InsertMSXDetails ");
            startDatetime = DateTime.UtcNow;
            int i = 0;
            List<CsLeads> leadList = GetIAPSLeads();
            try
            {
                if (leadList.Count > 0)
                {
                    DataTable _dtAccountDetails = new DataTable();
                    _dtAccountDetails.Columns.Add("LeadRef", typeof(Guid));
                    _dtAccountDetails.Columns.Add("MSXAccountID", typeof(string));
                    _dtAccountDetails.Columns.Add("AccountName", typeof(string));
                    _dtAccountDetails.Columns.Add("Subsidiary", typeof(string));
                    _dtAccountDetails.Columns.Add("MSXStartDate", typeof(DateTime));
                    _dtAccountDetails.Columns.Add("VendorTeamName", typeof(string));
                    _dtAccountDetails.Columns.Add("ProcessStartTime", typeof(string));
                    foreach (var lead in leadList)
                    {
                        i++;
                        DataRow drAccountNumbers = _dtAccountDetails.NewRow();
                        drAccountNumbers["LeadRef"] = lead.LeadID;
                        drAccountNumbers["MSXAccountID"] = lead.MSXAccountID;
                        drAccountNumbers["AccountName"] = lead.ImportedAccountName;
                        drAccountNumbers["Subsidiary"] = lead.Subsidiary;
                        drAccountNumbers["MSXStartDate"] = startDatetime;
                        drAccountNumbers["VendorTeamName"] = lead.VendorName;
                        drAccountNumbers["ProcessStartTime"] = StartTime.ToString();
                        _dtAccountDetails.Rows.Add(drAccountNumbers);
                    }
                    if (i == leadList.Count)
                    {
                        string tableName = "[dbo].[SSIS_Staging_MSX]";
                        _manager.InsertDataIntoSQLDatabase(tableName, _dtAccountDetails);
                    }
                }
            }
            catch (Exception ex)
            {
                _log.LogError("InsertMSXDetails", "Failed", ex);
            }
            finally
            {
                _manager.Dispose();
                DateTime EndTime = DateTime.UtcNow;
                SqlConnection con = new SqlConnection(LeadValidationFunctions._sqlConnection);
                //AuthenticationResult authenticationResult = _manager.AADAunthenticationResult();
                con.AccessToken = Manager.getAccessToken(System.Environment.GetEnvironmentVariable("ResourceId"), System.Environment.GetEnvironmentVariable("VTCPManagedIdentity"), true).Result;
                SqlCommand SqlCommands = new SqlCommand();
                SqlCommands = new SqlCommand("AzureFunctionsDataLoad", con);
                SqlCommands.CommandType = CommandType.StoredProcedure;
                SqlCommands.Parameters.AddWithValue("@FunctionName", "MSXIntegrationInput");
                SqlCommands.Parameters.AddWithValue("@ProcessStartTime", StartTime.ToString());
                SqlCommands.Parameters.AddWithValue("@FunctionStartTime", StartTime.ToString());
                SqlCommands.Parameters.AddWithValue("@FunctionEndTime", EndTime.ToString());
                SqlCommands.Parameters.AddWithValue("@InitialRowsAffected", leadList.Count.ToString());
                SqlCommands.Parameters.AddWithValue("@TotalRowsAffected", leadList.Count.ToString());
                con.Open();
                SqlCommands.ExecuteNonQuery();
                con.Close();
            }
        }

        /// <summary>
        /// To update MSX details in CRM
        /// </summary>
        public void RetrieveMSXRecordsFromTable()
        {
            DateTime StartTime = DateTime.UtcNow;
            int batch = 0;
            ExecuteMultipleSettings excuteMultipleSettings = new ExecuteMultipleSettings();
            excuteMultipleSettings.ContinueOnError = true;
            excuteMultipleSettings.ReturnResponses = true;
            StringBuilder DataWarnings = new StringBuilder();
            ExecuteMultipleRequest updateLeadsRequest = new ExecuteMultipleRequest();
            updateLeadsRequest.Requests = new OrganizationRequestCollection();
            updateLeadsRequest.Settings = excuteMultipleSettings;

            string query = "select * from [dbo].[SSIS_Output_MSX] (nolock) ";
            DataTable dt = _manager.RetrieveDatafromSQLDatabase(query);
            string ProcessStartTime = string.Empty;
            if (dt != null && dt.Rows.Count > 0)
            {
                foreach (DataRow row in dt.Rows)
                {
                    batch++;
                    string[] ExistingMSXSameVendorList = null;
                    string[] ExistingMSXOtherVendorList = null;
                    string MSXOppLeadID = string.Empty;
                    StringBuilder ExistingMSXActivitySameVendorWithoutID = new StringBuilder();
                    StringBuilder ExistingMSXActivityOtherVendorWithoutID = new StringBuilder();
                    string ExistingMSXActivitySameVendor = row["ExistingMSXActivitySameVendor"].ToString();
                    string ExistingMSXActivityOtherVendor = row["ExistingMSXActivityOtherVendor"].ToString();
                    string ExistingMSXActivityDetailsSameVendor = row["ExistingMSXActivityDetailsSameVendor"].ToString();
                    string ExistingMSXActivityDetailsOtherVendor = row["ExistingMSXActivityDetailsOtherVendor"].ToString();
                    //string MSXStatus = row["MSXStatus"].ToString();
                    string MSXActivity = row["MSX Activity"].ToString();
                    string LeadRefId = row["LeadRef"].ToString();
                    DateTime msxStartTime = Convert.ToDateTime(row["MSXStartDate"].ToString());
                    //string vendor = row["VendorTeamName"].ToString();

                    if (!string.IsNullOrWhiteSpace(ExistingMSXActivityDetailsSameVendor))
                    {
                        ExistingMSXSameVendorList = ExistingMSXActivityDetailsSameVendor.Split(new string[] { "\r" }, StringSplitOptions.None);

                    }
                    if (!string.IsNullOrWhiteSpace(ExistingMSXActivityDetailsOtherVendor))
                    {
                        ExistingMSXOtherVendorList = ExistingMSXActivityDetailsOtherVendor.Split(new string[] { "\r" }, StringSplitOptions.None);

                    }
                    if (ExistingMSXSameVendorList.Count() > 0)
                    {
                        foreach (var e in ExistingMSXSameVendorList)
                        {
                            if (e.Split(new string[] { "##" }, StringSplitOptions.None).Count() > 1)
                            {
                                MSXOppLeadID = MSXOppLeadID + e.Split(new string[] { "##" }, StringSplitOptions.None)[1] + ";";
                                ExistingMSXActivitySameVendorWithoutID.AppendLine(e.Split(new string[] { "##" }, StringSplitOptions.None)[0]);
                            }

                            if (e.Split(new string[] { "##" }, StringSplitOptions.None).Count() == 1)
                            {
                                ExistingMSXActivitySameVendorWithoutID.AppendLine(e);
                            }

                        }
                    }
                    if (ExistingMSXOtherVendorList.Count() > 0)
                    {
                        foreach (var e in ExistingMSXOtherVendorList)
                        {
                            if (e.Split(new string[] { "##" }, StringSplitOptions.None).Count() > 1)
                            {
                                MSXOppLeadID = MSXOppLeadID + e.Split(new string[] { "##" }, StringSplitOptions.None)[1] + ";";
                                ExistingMSXActivityOtherVendorWithoutID.AppendLine(e.Split(new string[] { "##" }, StringSplitOptions.None)[0]);
                            }
                            if (e.Split(new string[] { "##" }, StringSplitOptions.None).Count() == 1)
                            {
                                ExistingMSXActivityOtherVendorWithoutID.AppendLine(e);
                            }
                        }
                    }
                    Entity objLeadEntity = new Entity("lead");
                    objLeadEntity.Id = new Guid(LeadRefId);
                    objLeadEntity["new_existingmsxactivitydetailssamevendor"] = ExistingMSXActivitySameVendorWithoutID.ToString();
                    objLeadEntity["new_existingmsxactivitydetailsotherteam"] = ExistingMSXActivityOtherVendorWithoutID.ToString();
                    objLeadEntity["new_existingmsxactivitysamevendor"] = ExistingMSXActivitySameVendor;
                    objLeadEntity["new_existingmsxactivityotherteam"] = ExistingMSXActivityOtherVendor;
                    objLeadEntity["new_msxleadandopportunityguids"] = MSXOppLeadID.ToString();
                    objLeadEntity["new_is_msx_contacted"] = new OptionSetValue(isYes);
                    objLeadEntity["new_msxcheckstarttime"] = msxStartTime;
                    objLeadEntity["new_msxcheckendtime"] = DateTime.Now;
                    objLeadEntity["new_leadupdatedby"] = "MSXIntegration Output Job";
                    objLeadEntity["new_pendingvalidationstepsortorder"] = new OptionSetValue(Convert.ToInt32(100000005));
                    ProcessStartTime = Convert.ToString(row["ProcessStartTime"]);
                    if (MSXActivity.ToLower() == "yes")
                    { objLeadEntity["new_existingmsxactivity"] = new OptionSetValue(isYes); }
                    else { objLeadEntity["new_existingmsxactivity"] = new OptionSetValue(isNo); }

                    var updateRequest = new UpdateRequest();
                    updateRequest.Target = objLeadEntity;
                    updateLeadsRequest.Requests.Add(updateRequest);
                    if (updateLeadsRequest.Requests.Count == 1000)
                    {
                        _manager.ExecuteBulkRequest(updateLeadsRequest, "Updated MSX Details");
                        updateLeadsRequest.Requests.Clear();
                        batch = 0;
                    }
                }
                if (batch > 0)
                {
                    _manager.ExecuteBulkRequest(updateLeadsRequest, "Updated MSX Details");
                    updateLeadsRequest.Requests.Clear();
                    batch = 0;
                }
            }
            DateTime EndTime = DateTime.UtcNow;
            SqlConnection con = new SqlConnection(LeadValidationFunctions._sqlConnection);
            //AuthenticationResult authenticationResult = _manager.AADAunthenticationResult();
            con.AccessToken = Manager.getAccessToken(System.Environment.GetEnvironmentVariable("ResourceId"), System.Environment.GetEnvironmentVariable("VTCPManagedIdentity"), true).Result;
            SqlCommand SqlCommands = new SqlCommand();
            SqlCommands = new SqlCommand("AzureFunctionsDataLoad", con);
            SqlCommands.CommandType = CommandType.StoredProcedure;
            SqlCommands.Parameters.AddWithValue("@FunctionName", "MSXIntegrationOutput");
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

        #region Logic to Update ExclusionList Details
        /// <summary>
        /// To Update Is ExclusionList Field
        /// </summary>
        public void UpdateIsExclusionListField()
        {
            DateTime StartTime = DateTime.UtcNow;
            _log.LogInformation("Execution started UpdateIsExclusionListField");
            List<CsLeads> leadList = GetIAPSLeadsForIsExclusionList();

            ExecuteMultipleSettings excuteMultipleSettings = new ExecuteMultipleSettings();
            excuteMultipleSettings.ContinueOnError = true;
            excuteMultipleSettings.ReturnResponses = true;

            ExecuteMultipleRequest updateLeadsRequest = new ExecuteMultipleRequest();
            updateLeadsRequest.Requests = new OrganizationRequestCollection();
            updateLeadsRequest.Settings = excuteMultipleSettings;


            EntityCollection objEntityColl = new EntityCollection();
            int batch = 0;
            try
            {
                if (leadList.Count > 0)
                {
                    objEntityColl = GetAllExclusionListInVTCP(_orgService);
                    List<Entity> AllExclusionList = objEntityColl.Entities.ToList().OrderBy(c => c.Attributes["createdon"]).ToList();

                    foreach (var lead in leadList)
                    {
                        batch++;
                        Entity objLeadEntity = new Entity("lead");
                        objLeadEntity.Id = new Guid(lead.LeadID.ToString());

                        int Count = 0;
                        int MatchCount = 0;
                        string AssociatedExclusionList = "";
                        foreach (Entity ExclusionList in AllExclusionList)
                        {
                            Guid ExclusionListID = ExclusionList.GetAttributeValue<Guid>("new_blacklistaccountsid");
                            string AccountType = ExclusionList.Contains("new_accounttype") ? ExclusionList.GetAttributeValue<string>("new_accounttype") : string.Empty;
                            string AccountNumber = ExclusionList.Contains("new_accountnumber") ? ExclusionList.GetAttributeValue<string>("new_accountnumber") : string.Empty;
                            string ExclusionListName = ExclusionList.Contains("new_name") ? ExclusionList.GetAttributeValue<string>("new_name") : string.Empty;
                            DateTime ExclusionListStartDate = ExclusionList.Contains("new_blackliststartdate") ? ExclusionList.GetAttributeValue<DateTime>("new_blackliststartdate") : DateTime.MinValue;
                            DateTime ExclusionListEndDate = ExclusionList.Contains("new_blacklistenddate") ? ExclusionList.GetAttributeValue<DateTime>("new_blacklistenddate") : DateTime.MinValue;
                            string subsidiary = ExclusionList.Contains("new_subsidiary") ? ExclusionList.GetAttributeValue<EntityReference>("new_subsidiary").Name : string.Empty;
                            if (!string.IsNullOrWhiteSpace(subsidiary))
                            {
                                if (subsidiary != lead.Subsidiary)
                                {
                                    continue;
                                }
                            }
                            if (ExclusionListStartDate < DateTime.Now && ExclusionListEndDate > DateTime.Now)
                            {
                                MatchCount = 0;
                                if (ExclusionListName == lead.ImportedAccountName || (AccountNumber == lead.MSSalesTPID && AccountType == "TPID") || (AccountNumber == lead.MSXAccountID && AccountType == "MSX Account ID"))
                                {
                                    Count++;
                                    MatchCount++;
                                    AssociatedExclusionList += ExclusionListID.ToString() + ", ";
                                }

                                else if (!string.IsNullOrEmpty(lead.MSXAccountID) && lead.MSXAccountID.Contains(','))
                                {
                                    string[] MSXAccountIDSplitList = lead.MSXAccountID.Split(',');
                                    foreach (var MSXAccountID in MSXAccountIDSplitList)
                                    {
                                        if (MSXAccountID.Trim() == AccountNumber && AccountType == "MSX Account ID")
                                        {
                                            Count++;
                                            MatchCount++;
                                            AssociatedExclusionList += ExclusionListID.ToString() + ", ";
                                        }
                                    }
                                }
                                else if (!string.IsNullOrEmpty(lead.MSXAccountIDMatched) && lead.MSXAccountIDMatched.Trim() == AccountNumber && AccountType == "MSX Account ID")
                                {
                                    Count++;
                                    MatchCount++;
                                    AssociatedExclusionList += ExclusionListID.ToString() + ", ";
                                }

                                if (!string.IsNullOrEmpty(lead.CustomerTPIDList) && MatchCount == 0)
                                {
                                    string[] TPIDSplitList = lead.CustomerTPIDList.Replace("||", ",").Split(',');
                                    foreach (var TPID in TPIDSplitList)
                                    {
                                        if (TPID.Trim() == AccountNumber && AccountType == "TPID")
                                        {
                                            Count++;
                                            AssociatedExclusionList += ExclusionListID.ToString() + ", ";
                                        }
                                    }
                                }
                            }
                        }

                        if (Count > 0)
                        {
                            objLeadEntity["new_isblacklisted"] = new OptionSetValue(isYes);
                            objLeadEntity["new_associatedblacklistedaccounts"] = AssociatedExclusionList.TrimEnd(',', ' ');
                        }
                        else
                        {
                            objLeadEntity["new_isblacklisted"] = new OptionSetValue(isNo);
                        }
                        objLeadEntity["new_blacklistcheckcompletedtime"] = DateTime.UtcNow;
                        if (lead.IsAutoCreatedLead.ToLower() == "true" && lead.CampaignType.ToUpper() == "RENEWALS" && !string.IsNullOrWhiteSpace(lead.AgreementID))
                        {
                            objLeadEntity["new_pendingvalidationstepsortorder"] = new OptionSetValue(Convert.ToInt32(100000006));
                        }
                        else
                        {
                            objLeadEntity["new_pendingvalidationstepsortorder"] = new OptionSetValue(Convert.ToInt32(100000008));
                        }

                        var updateRequest = new UpdateRequest();
                        updateRequest.Target = objLeadEntity;
                        updateLeadsRequest.Requests.Add(updateRequest);

                        if (updateLeadsRequest.Requests.Count == 1000)
                        {
                            _manager.ExecuteBulkRequest(updateLeadsRequest, "Updated IsExclusionList Details");
                            updateLeadsRequest.Requests.Clear();
                            batch = 0;
                        }
                    }
                    if (batch > 0)
                    {
                        _manager.ExecuteBulkRequest(updateLeadsRequest, "Updated IsExclusionList Details");
                        updateLeadsRequest.Requests.Clear();
                        batch = 0;
                    }
                }
            }
            catch (Exception ex)
            {
                _log.LogError("UpdateIsExclusionListDetails", "Failed", ex);
            }
            finally
            {
                _manager.Dispose();
                DateTime EndTime = DateTime.UtcNow;
                SqlConnection con = new SqlConnection(LeadValidationFunctions._sqlConnection);
                //AuthenticationResult authenticationResult = _manager.AADAunthenticationResult();
                con.AccessToken = Manager.getAccessToken(System.Environment.GetEnvironmentVariable("ResourceId"), System.Environment.GetEnvironmentVariable("VTCPManagedIdentity"), true).Result;
                SqlCommand SqlCommands = new SqlCommand();
                SqlCommands = new SqlCommand("AzureFunctionsDataLoad", con);
                SqlCommands.CommandType = CommandType.StoredProcedure;
                SqlCommands.Parameters.AddWithValue("@FunctionName", "UpdateExclusionListDetails");
                SqlCommands.Parameters.AddWithValue("@ProcessStartTime", StartTime.ToString());
                SqlCommands.Parameters.AddWithValue("@FunctionStartTime", StartTime.ToString());
                SqlCommands.Parameters.AddWithValue("@FunctionEndTime", EndTime.ToString());
                SqlCommands.Parameters.AddWithValue("@InitialRowsAffected", leadList.Count.ToString());
                SqlCommands.Parameters.AddWithValue("@TotalRowsAffected", TotalProcessedRecords.ToString());
                con.Open();
                SqlCommands.ExecuteNonQuery();
                con.Close();
            }
        }

        /// <summary>
        /// Query Expression to pull all the ExclusionList from VTCP
        /// </summary>
        /// <param name="service"></param>
        /// <returns></returns>
        public EntityCollection GetAllExclusionListInVTCP(IOrganizationService service)
        {
            // Query using the paging cookie.
            // Define the paging attributes.

            //EntityCollection Object
            EntityCollection results = null;
            EntityCollection finalResults = new EntityCollection();

            // The number of records per page to retrieve.
            int queryCount = 2500;

            // Initialize the page number.
            int pageNumber = 1;

            // Create the query expression and add condition.
            QueryExpression pagequery = new QueryExpression();
            pagequery.EntityName = "new_blacklistaccounts";

            //pagequery.ColumnSet.AllColumns = true;
            pagequery.ColumnSet = new ColumnSet("new_blacklistaccountsid", "new_accountnumber", "new_accounttype", "new_name", "new_blackliststartdate", "new_blacklistenddate", "createdon", "new_subsidiary");

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
                results = service.RetrieveMultiple(pagequery);
                if (results.Entities != null)
                {
                    // Retrieve all records from the result set.
                    foreach (var acct in results.Entities)
                    {
                        finalResults.Entities.Add(acct);
                        //_log.LogInformation(Convert.ToString(++recordCount));
                    }
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
            }
            return finalResults;
        }

        /// <summary>
        /// Get GIAPSLeads for IsExclusionList Update
        /// </summary>
        public List<CsLeads> GetIAPSLeadsForIsExclusionList()
        {
            _log.LogInformation("Execution started: ", "GetIAPSLeadsForIsExclusionList");
            List<CsLeads> Leads = new List<CsLeads>();
            EntityCollection leadscoll = new EntityCollection();
            int statusReason = Convert.ToInt32(100000008);
            int status = Convert.ToInt32(0);
            // ExculsionList Check Pending Validation Step
            int pendingvalidationStepSortOrder = Convert.ToInt32(100000005);

            QueryExpression leadattributes = new QueryExpression()
            {
                EntityName = "lead",
                ColumnSet = new ColumnSet(true),
                Criteria = new FilterExpression(),
                TopCount = 1000
            };

            leadattributes.Criteria.AddCondition("statuscode", ConditionOperator.Equal, statusReason);
            leadattributes.Criteria.AddCondition("new_importedaccount", ConditionOperator.NotNull);
            // leadattributes.Criteria.AddCondition("new_is_msx_contacted", ConditionOperator.NotNull);
            // leadattributes.Criteria.AddCondition("new_isblacklisted", ConditionOperator.Null);
            leadattributes.Criteria.AddCondition("new_pendingvalidationstepsortorder", ConditionOperator.Equal, pendingvalidationStepSortOrder);
            leadattributes.AddOrder("prioritycode", OrderType.Descending);
            leadattributes.AddOrder("createdon", OrderType.Ascending);

            leadscoll = _orgService.RetrieveMultiple(leadattributes);

            if (leadscoll != null && leadscoll.Entities.Count > 0)
            {
                foreach (Entity entityObject in leadscoll.Entities)
                {
                    Leads.Add(new CsLeads
                    {
                        LeadID = entityObject.Contains("leadid") ? new System.Guid(entityObject["leadid"].ToString()) : Guid.Empty,
                        MSSalesTPID = entityObject.Contains("new_mssalestpid") ? entityObject["new_mssalestpid"].ToString() : string.Empty,
                        MSXAccountID = entityObject.Contains("new_msxaccountid") ? entityObject["new_msxaccountid"].ToString() : string.Empty,
                        ImportedAccountName = entityObject.Contains("new_importedaccount") ? entityObject["new_importedaccount"].ToString() : string.Empty,
                        CustomerTPIDList = entityObject.Contains("new_tpidlist") ? entityObject["new_tpidlist"].ToString() : string.Empty,
                        Subsidiary = entityObject.Contains("new_subsidiary") ? entityObject.GetAttributeValue<EntityReference>("new_subsidiary").Name : string.Empty,
                        MSXAccountIDMatched = entityObject.Contains("new_matchedmsxaccountid") ? entityObject["new_matchedmsxaccountid"].ToString() : string.Empty,
                        IsAutoCreatedLead = entityObject.Contains("new_isautocreatedlead") ? entityObject.GetAttributeValue<bool>("new_isautocreatedlead").ToString() : string.Empty,
                        CampaignType = entityObject.Contains("new_campaigntype") ? entityObject.GetAttributeValue<EntityReference>("new_campaigntype").Name : string.Empty,
                        AgreementID = entityObject.Contains("new_agreementid") ? entityObject.GetAttributeValue<string>("new_agreementid") : string.Empty

                    });
                }
            }
            return Leads;
        }
        #endregion

        #region VTCP duplicate agreement check
        public void VTCP_DuplicateAgreementCheck()
        {
            DateTime StartTime = DateTime.UtcNow;
            _log.LogInformation("Execution started VTCP_DuplicateAgreementCheck");
            List<CsLeads> leadList = GetLeadsForVTCP_DuplicateAgreementCheck();
            int batch = 0;
            List<string> DuplicatesDifferentVendor, DuplicatesSameVendor;

            ExecuteMultipleSettings excuteMultipleSettings = new ExecuteMultipleSettings();
            excuteMultipleSettings.ContinueOnError = true;
            excuteMultipleSettings.ReturnResponses = true;

            ExecuteMultipleRequest updateLeadsRequest = new ExecuteMultipleRequest();
            updateLeadsRequest.Requests = new OrganizationRequestCollection();
            updateLeadsRequest.Settings = excuteMultipleSettings;

            if (leadList?.Count > 0)
            {
                Entity objLeadEntity = null;
                UpdateRequest updateRequest = null;
                string new_duplicateagreementdetails = string.Empty;
                string MSXLeadNumber = string.Empty;

                foreach (CsLeads lead in leadList)
                {
                    batch++;
                    new_duplicateagreementdetails = string.Empty;
                    DuplicatesDifferentVendor = new List<string>();
                    DuplicatesDifferentVendor.Add("Agreement already loaded to VTCP by another vendor\r\n");
                    DuplicatesSameVendor = new List<string>();
                    DuplicatesSameVendor.Add($"Agreement already loaded to VTCP by {lead.VendorName}\r\n");

                    objLeadEntity = new Entity("lead", lead.LeadID);

                    List<CsLeads> duplicateLeads = GetDuplicateLeadsByAgreementID(lead);

                    //If count is more than 0 that means there are duplicate leads
                    if (duplicateLeads?.Count > 0)
                    {
                        foreach (CsLeads duplicateLead in duplicateLeads)
                        {
                            MSXLeadNumber = string.IsNullOrEmpty(duplicateLead.MSXUploadLeadNumber) ? "Pending MSX Upload" : "MSX Lead Number: " + duplicateLead.MSXUploadLeadNumber;

                            if (lead.VendorName == duplicateLead.VendorName)
                                DuplicatesSameVendor.Add($"- Lead {duplicateLead.LeadID} created on {duplicateLead.Createdon.ToString("yyyy-MM-dd")}. {MSXLeadNumber}\r\n");
                            else
                                DuplicatesDifferentVendor.Add($"- Lead {duplicateLead.LeadID} created on {duplicateLead.Createdon.ToString("yyyy-MM-dd")}. {MSXLeadNumber}\r\n");
                        }
                        if (DuplicatesDifferentVendor.Count > 1)
                            foreach (var text in DuplicatesDifferentVendor)
                                new_duplicateagreementdetails += text;

                        if (DuplicatesSameVendor.Count > 1)
                            foreach (var text in DuplicatesSameVendor)
                                new_duplicateagreementdetails += text;

                        if (lead.DisqualifyReasons.Contains(new OptionSetValue(100000043)) == false)
                        {
                            objLeadEntity["statuscode"] = new OptionSetValue(100000043);//Disqualified - Agreement Already in VTCP
                            objLeadEntity["statecode"] = new OptionSetValue(2);//Disqualified
                            objLeadEntity["new_validationcompletedtime"] = DateTime.UtcNow;
                        }
                        objLeadEntity["new_duplicateagreementdetails"] = new_duplicateagreementdetails;//Existing VTCP Activity
                    }

                    objLeadEntity["new_vtcpduplicatedetectioncomplete"] = true;//VTCP Duplicate Detection Complete
                    objLeadEntity["new_vtcpduplicatedetectioncompletetime"] = DateTime.UtcNow;//VTCP Duplicate Detection Complete Time
                    if (string.IsNullOrWhiteSpace(lead.MergeStatus))
                    {
                        objLeadEntity["new_pendingvalidationstepsortorder"] = new OptionSetValue(Convert.ToInt32(100000007));

                    }
                    else
                    {
                        objLeadEntity["new_pendingvalidationstepsortorder"] = new OptionSetValue(Convert.ToInt32(100000008));
                    }

                    updateRequest = new UpdateRequest();
                    updateRequest.Target = objLeadEntity;
                    updateLeadsRequest.Requests.Add(updateRequest);
                    if (updateLeadsRequest.Requests.Count == 1000)
                    {
                        _manager.ExecuteBulkRequest(updateLeadsRequest, "Updated VTCP_DuplicateAgreementCheck Details");
                        updateLeadsRequest.Requests.Clear();
                        batch = 0;
                    }
                }
                if (batch > 0)
                {
                    _manager.ExecuteBulkRequest(updateLeadsRequest, "Updated VTCP_DuplicateAgreementCheck Details");
                }
            }
            #region logs.AzureFunctions 
            DateTime EndTime = DateTime.UtcNow;
            SqlConnection con = new SqlConnection(LeadValidationFunctions._sqlConnection);
            //AuthenticationResult authenticationResult = _manager.AADAunthenticationResult();
            con.AccessToken = Manager.getAccessToken(System.Environment.GetEnvironmentVariable("ResourceId"), System.Environment.GetEnvironmentVariable("VTCPManagedIdentity"), true).Result;
            SqlCommand SqlCommands = new SqlCommand();
            SqlCommands = new SqlCommand("AzureFunctionsDataLoad", con);
            SqlCommands.CommandType = CommandType.StoredProcedure;
            SqlCommands.Parameters.AddWithValue("@FunctionName", "VTCP_DuplicateAgreementCheck");
            SqlCommands.Parameters.AddWithValue("@ProcessStartTime", StartTime.ToString());
            SqlCommands.Parameters.AddWithValue("@FunctionStartTime", StartTime.ToString());
            SqlCommands.Parameters.AddWithValue("@FunctionEndTime", EndTime.ToString());
            SqlCommands.Parameters.AddWithValue("@InitialRowsAffected", leadList.Count.ToString());
            SqlCommands.Parameters.AddWithValue("@TotalRowsAffected", TotalProcessedRecords.ToString());
            con.Open();
            SqlCommands.ExecuteNonQuery();
            con.Close();
            #endregion
        }

        private List<CsLeads> GetLeadsForVTCP_DuplicateAgreementCheck()
        {
            _log.LogInformation("Execution started: ", "GetLeadForVTCP_DuplicateAgreementCheck");
            List<CsLeads> Leads = new List<CsLeads>();
            EntityCollection leadscoll = new EntityCollection();
            // Duplicate Aggrement Check Pending Validation Step
            int pendingValidationStepSortOrder = Convert.ToInt32(100000006);

            QueryExpression leadattributes = new QueryExpression()
            {
                EntityName = "lead",
                ColumnSet = new ColumnSet(true),
                Criteria = new FilterExpression(),
                TopCount = 1000
            };

            #region Filter clause
            leadattributes.Criteria.AddCondition("statuscode", ConditionOperator.Equal, 100000008);//Validation in Progress
            //leadattributes.Criteria.AddCondition("new_isblacklisted", ConditionOperator.NotNull);
            leadattributes.AddOrder("prioritycode", OrderType.Descending);
            leadattributes.AddOrder("createdon", OrderType.Ascending);
            // leadattributes.Criteria.AddCondition("new_isautocreatedlead", ConditionOperator.Equal, true);
            leadattributes.Criteria.AddCondition("new_agreementid", ConditionOperator.NotNull);
            //leadattributes.Criteria.AddCondition("new_vtcpduplicatedetectioncomplete", ConditionOperator.NotEqual, true);//VTCP Duplicate Detection Complete
            leadattributes.Criteria.AddCondition("new_pendingvalidationstepsortorder", ConditionOperator.Equal, pendingValidationStepSortOrder);

            #endregion

            leadscoll = _orgService.RetrieveMultiple(leadattributes);

            if (leadscoll != null && leadscoll.Entities.Count > 0)
            {
                _log.LogInformation("leads count: " + leadscoll.Entities.Count);
                foreach (Entity entityObject in leadscoll.Entities)
                {
                    Leads.Add(new CsLeads
                    {
                        LeadID = entityObject.Contains("leadid") ? new System.Guid(entityObject["leadid"].ToString()) : Guid.Empty,
                        AgreementID = entityObject.Contains("new_agreementid") ? entityObject["new_agreementid"].ToString() : string.Empty,
                        MSSalesTPID = entityObject.Contains("new_mssalestpid") ? entityObject["new_mssalestpid"].ToString() : string.Empty,
                        Createdon = entityObject.Contains("createdon") ? Convert.ToDateTime(entityObject["createdon"]) : DateTime.MinValue,
                        StatusReason = entityObject.Contains("statuscode") ? entityObject.GetAttributeValue<OptionSetValue>("statuscode").Value.ToString() : string.Empty,
                        IsAutoCreatedLead = entityObject.Contains("new_isautocreatedlead") ? entityObject.GetAttributeValue<bool>("new_isautocreatedlead").ToString() : string.Empty,
                        VendorName = entityObject.Contains("new_vendorteam") ? entityObject.GetAttributeValue<EntityReference>("new_vendorteam").Name : string.Empty,
                        MSXUploadLeadNumber = entityObject.Contains("new_uploadedmsxleadnumber") ? entityObject.GetAttributeValue<string>("new_uploadedmsxleadnumber") : string.Empty,
                        DisqualifyReasons = entityObject.Contains("new_overriddendisqualificationreasons") ? entityObject.GetAttributeValue<OptionSetValueCollection>("new_overriddendisqualificationreasons") : new OptionSetValueCollection(),
                        MergeStatus = entityObject.Contains("new_mergestatus") ? entityObject.GetAttributeValue<string>("new_mergestatus") : string.Empty

                    });
                }
            }
            return Leads;
        }

        private List<CsLeads> GetDuplicateLeadsByAgreementID(CsLeads lead)
        {
            List<CsLeads> duplicateLeads = new List<CsLeads>();
            EntityCollection leadscoll = new EntityCollection();

            QueryExpression leadattributes = new QueryExpression()
            {
                EntityName = "lead",
                ColumnSet = new ColumnSet(true),
                Criteria = new FilterExpression()
            };

            FilterExpression statusReasonFilter = new FilterExpression(LogicalOperator.Or);
            statusReasonFilter.AddCondition("statuscode", ConditionOperator.Equal, 100000009);//Validated - Ready for MSX Upload
            statusReasonFilter.AddCondition("statuscode", ConditionOperator.Equal, 100000041);//Validated (With Warnings) - Review Before MSX Upload
            statusReasonFilter.AddCondition("statuscode", ConditionOperator.Equal, 100000020);//MSX Upload Complete
            statusReasonFilter.AddCondition("statuscode", ConditionOperator.Equal, 100000025);//MSX Upload Failed
            statusReasonFilter.AddCondition("statuscode", ConditionOperator.Equal, 100000027);//MSX Upload In Progress
            statusReasonFilter.AddCondition("statuscode", ConditionOperator.Equal, 100000028);//MSX Upload Rejected
            statusReasonFilter.AddCondition("statuscode", ConditionOperator.Equal, 100000030);//MSX Upload Triggered
            statusReasonFilter.AddCondition("statuscode", ConditionOperator.Equal, 100000032);//Merged with Another Lead
            leadattributes.Criteria.AddFilter(statusReasonFilter);

            leadattributes.Criteria.AddCondition("leadid", ConditionOperator.NotEqual, lead.LeadID);
            leadattributes.Criteria.AddCondition("new_isautocreatedlead", ConditionOperator.Equal, true);
            leadattributes.Criteria.AddCondition("new_agreementid", ConditionOperator.Equal, lead.AgreementID);
            leadattributes.AddOrder("createdon", OrderType.Descending);

            leadscoll = _orgService.RetrieveMultiple(leadattributes);

            if (leadscoll != null && leadscoll.Entities.Count > 0)
            {
                _log.LogInformation("duplicateLeads count: " + leadscoll.Entities.Count);
                foreach (Entity entityObject in leadscoll.Entities)
                {
                    duplicateLeads.Add(new CsLeads
                    {
                        LeadID = entityObject.Contains("leadid") ? new System.Guid(entityObject["leadid"].ToString()) : Guid.Empty,
                        AgreementID = entityObject.Contains("new_agreementid") ? entityObject["new_agreementid"].ToString() : string.Empty,
                        MSSalesTPID = entityObject.Contains("new_mssalestpid") ? entityObject["new_mssalestpid"].ToString() : string.Empty,
                        Createdon = entityObject.Contains("createdon") ? Convert.ToDateTime(entityObject["createdon"]) : DateTime.MinValue,
                        StatusReason = entityObject.Contains("statuscode") ? entityObject.GetAttributeValue<OptionSetValue>("statuscode").Value.ToString() : string.Empty,
                        MasterID = entityObject.Contains("masterid") ? entityObject.GetAttributeValue<EntityReference>("masterid").Id : Guid.Empty,
                        VendorName = entityObject.Contains("new_vendorteam") ? entityObject.GetAttributeValue<EntityReference>("new_vendorteam").Name : string.Empty,
                        MSXUploadLeadNumber = entityObject.Contains("new_uploadedmsxleadnumber") ? entityObject.GetAttributeValue<string>("new_uploadedmsxleadnumber") : string.Empty,
                        DisqualifyReasons = entityObject.Contains("new_overriddendisqualificationreasons") ? entityObject.GetAttributeValue<OptionSetValueCollection>("new_overriddendisqualificationreasons") : new OptionSetValueCollection()
                    });
                }
                if (duplicateLeads.Count > 0)
                {
                    var mergedLeads = duplicateLeads.Where(x => x.StatusReason == "100000032").ToList();//Merged with Another Lead
                    _log.LogInformation("mergedLeads count: " + mergedLeads.Count);

                    if (mergedLeads.Count > 0)
                    {
                        foreach (var mergedLead in mergedLeads)
                        {
                            QueryExpression _leadattributes = new QueryExpression()
                            {
                                EntityName = "lead",
                                ColumnSet = new ColumnSet(true),
                                Criteria = new FilterExpression()
                            };

                            _leadattributes.Criteria.AddCondition("leadid", ConditionOperator.Equal, mergedLead.MasterID);

                            var _leads = _orgService.RetrieveMultiple(_leadattributes);//retrun master lead
                            if (_leads.Entities.Count > 0)
                            {
                                var MasterLead = _leads.Entities[0];

                                var masterLeadExist = duplicateLeads.Where(x => x.LeadID != MasterLead.Id).ToList();
                                if (masterLeadExist.Count > 0)
                                {
                                    if ((MasterLead.Contains("statecode") && MasterLead.GetAttributeValue<OptionSetValue>("statecode").Value == 2)//Disqualified
                                        || (MasterLead.Contains("statuscode") && MasterLead.GetAttributeValue<OptionSetValue>("statuscode").Value == 100000044))//Validation Expired
                                    {
                                        duplicateLeads.Remove(mergedLead);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return duplicateLeads;
        }
        #endregion

        #region Logic for Merginging Leads
        /// <summary>
        /// To merge Leads with Same TPID
        /// </summary>
        /// <param name="mergeRequests"></param>
        /// <param name="AllLeadsList"></param>
        /// <param name="masterLeadsList"></param>
        public void MergeLeadsWithSameTPID()
        {
            DateTime StartTime = DateTime.UtcNow;
            int batch = 0;
            int Count = 0;
            bool IsPaused = false;

            ExecuteMultipleSettings excuteMultipleSettings = new ExecuteMultipleSettings();
            excuteMultipleSettings.ContinueOnError = true;
            excuteMultipleSettings.ReturnResponses = true;

            ExecuteMultipleRequest updateLeadsRequest = new ExecuteMultipleRequest();
            updateLeadsRequest.Requests = new OrganizationRequestCollection();
            updateLeadsRequest.Settings = excuteMultipleSettings;

            ExecuteMultipleRequest mergeRequests = new ExecuteMultipleRequest();
            mergeRequests.Requests = new OrganizationRequestCollection();
            mergeRequests.Settings = excuteMultipleSettings;

            ExecuteMultipleRequest FinalmergeRequests = new ExecuteMultipleRequest();
            FinalmergeRequests.Requests = new OrganizationRequestCollection();
            FinalmergeRequests.Settings = excuteMultipleSettings;

            _log.LogInformation("Execution started: GetIAPSLeadsForMergeProcess");

            List<CsLeads> Leads = new List<CsLeads>();
            EntityCollection leadscoll = new EntityCollection();
            int status = Convert.ToInt32(0);
            leadscoll = GetLeadsForMerging();
            int totalChildeads = 0;
            if (leadscoll != null && leadscoll.Entities.Count > 0)
            {
                foreach (Entity entityObject in leadscoll.Entities)
                {
                    Leads.Add(new CsLeads
                    {
                        LeadID = entityObject.Contains("leadid") ? new System.Guid(entityObject["leadid"].ToString()) : Guid.Empty,
                        Subsidiary = entityObject.Contains("new_subsidiary") ? entityObject.GetAttributeValue<EntityReference>("new_subsidiary").Name : string.Empty,
                        Country = entityObject.Contains("new_country") ? entityObject.GetAttributeValue<EntityReference>("new_country").Name : string.Empty,
                        Createdon = entityObject.Contains("createdon") ? entityObject.GetAttributeValue<DateTime>("createdon") : DateTime.MinValue,
                        LeadTitle = entityObject.Contains("subject") ? entityObject.GetAttributeValue<string>("subject") : string.Empty,
                        MSXAccountID = entityObject.Contains("new_msxaccountid") ? entityObject["new_msxaccountid"].ToString() : string.Empty,
                        CampaignCode = entityObject.Contains("new_campaigncode") ? entityObject["new_campaigncode"].ToString() : string.Empty,
                        MSSalesTPID = entityObject.Contains("new_mssalestpid") ? entityObject["new_mssalestpid"].ToString() : string.Empty,
                        AgreementID = entityObject.Contains("new_agreementid") ? entityObject["new_agreementid"].ToString() : string.Empty,
                        AgreementExpirationDate = entityObject.Contains("new_agreementexpirationdate") ? Convert.ToDateTime(entityObject["new_agreementexpirationdate"]) : DateTime.MinValue,
                        CustomerList = entityObject.Contains("new_customerlistname") ? entityObject.GetAttributeValue<EntityReference>("new_customerlistname").Id : Guid.Empty,
                        CampaignType = entityObject.Contains("new_campaigntype") ? entityObject.GetAttributeValue<EntityReference>("new_campaigntype").Name : string.Empty,
                        IsAutoCreatedLead = entityObject.Contains("new_isautocreatedlead") ? entityObject.GetAttributeValue<bool>("new_isautocreatedlead").ToString() : string.Empty,
                        ResellerName = entityObject.Contains("new_resellername") ? entityObject["new_resellername"].ToString() : string.Empty,
                        DistributorName = entityObject.Contains("new_distributorname") ? entityObject["new_distributorname"].ToString() : string.Empty,
                        AdvisorName = entityObject.Contains("new_advisorname") ? entityObject["new_advisorname"].ToString() : string.Empty,
                        ExpiringProductDetails = entityObject.Contains("new_expiringproductdetails") ? entityObject["new_expiringproductdetails"].ToString() : string.Empty,
                        MergedAgreementDetails = entityObject.Contains("new_mergedagreementdetails") ? entityObject["new_mergedagreementdetails"].ToString() : string.Empty,
                        MergedProductDetails = entityObject.Contains("new_mergedproductdetails") ? entityObject["new_mergedproductdetails"].ToString() : string.Empty,
                        IsMergedLead = entityObject.Contains("new_ismergedlead") ? entityObject.GetAttributeValue<bool>("new_ismergedlead").ToString() : string.Empty,
                        MergedExpiringAmount = entityObject.Contains("new_mergedexpiringamount") ? entityObject.GetAttributeValue<decimal>("new_mergedexpiringamount") : 0,
                        ExpiringAmount = entityObject.Contains("new_expiringamount") ? entityObject.GetAttributeValue<decimal>("new_expiringamount") : 0,
                        MergedMinExpirationDate = entityObject.Contains("new_mergedminexpirationdate") ? entityObject.GetAttributeValue<DateTime>("new_mergedminexpirationdate") : DateTime.MinValue,
                        MergeMultipleLeadsForSameCustomer = entityObject.Contains("new_mergemultipleleadsforsamecustomer") ? (OptionSetValue)entityObject.Attributes["new_mergemultipleleadsforsamecustomer"] : null
                    });
                }

                List<CsLeads> leadList = Leads;
                List<CsLeads> masterRenewalsLeadsList = null;

                List<CsLeads> masterRenewalsCustomerList = leadList.GroupBy(x => new { x.CustomerList }).Where(c => c.Count() >= 1).Select(y => y.FirstOrDefault()).Distinct().ToList();

                List<CsLeads> finalLeadList = new List<CsLeads>();

                foreach (var customerList in masterRenewalsCustomerList)
                {
                    masterRenewalsLeadsList = leadList.Where(c => c.CampaignType.ToUpper() == "RENEWALS" && !string.IsNullOrWhiteSpace(c.MSSalesTPID) && (c.MergeMultipleLeadsForSameCustomer == null || (c.MergeMultipleLeadsForSameCustomer != null && c.MergeMultipleLeadsForSameCustomer.Value == Convert.ToInt32(100000000))) && c.CustomerList == customerList.CustomerList).GroupBy(x => new { x.MSSalesTPID, x.CustomerList }).Where(c => c.Count() >= 1).Select(y => y.FirstOrDefault()).Distinct().ToList();

                    //Get all leads from VTCP
                    EntityCollection AllLeadCollection = GetAllLeadsInVTCP(_orgService, customerList.CustomerList);
                    List<Entity> allLeadsList = AllLeadCollection.Entities.ToList();

                    foreach (CsLeads parent in masterRenewalsLeadsList)
                    {
                        List<Entity> GetAllLeadListBaseOnMSSalesTPID = allLeadsList.Where(c => c.Contains("new_mssalestpid") && c.GetAttributeValue<string>("new_mssalestpid").ToString() == parent.MSSalesTPID).ToList();
                        //List<Entity> GetExclusionListCompletedLeadList = GetAllLeadListBaseOnMSSalesTPID.Where(c => c.Contains("new_isblacklisted")).ToList();
                        List<Entity> GetDuplicateCheckCompletedLeadList = GetAllLeadListBaseOnMSSalesTPID.Where(c => c.Contains("new_vtcpduplicatedetectioncomplete") && c.GetAttributeValue<bool>("new_vtcpduplicatedetectioncomplete") == true).ToList();
                        List<Entity> GetValidationInProgressLeadList = GetAllLeadListBaseOnMSSalesTPID.Where(c => c.GetAttributeValue<OptionSetValue>("statuscode").Value.ToString() == "100000008").ToList();
                        List<Entity> GetMergePausedLeadList = GetAllLeadListBaseOnMSSalesTPID.Where(c => c.GetAttributeValue<OptionSetValue>("statuscode").Value.ToString() == "100000039").ToList();
                        if (GetAllLeadListBaseOnMSSalesTPID != null
                            && GetValidationInProgressLeadList != null
                            && GetValidationInProgressLeadList.Count == GetAllLeadListBaseOnMSSalesTPID.Count
                            && GetDuplicateCheckCompletedLeadList.Count != GetAllLeadListBaseOnMSSalesTPID.Count)
                        {
                            continue;
                        }
                        else
                        {
                            if (GetMergePausedLeadList != null && GetMergePausedLeadList.Count > 0)
                            {
                                List<Entity> GetValidationPausedLeadList = GetAllLeadListBaseOnMSSalesTPID.Where(c => c.GetAttributeValue<OptionSetValue>("statuscode").Value.ToString() == "100000038").ToList();

                                if (GetValidationPausedLeadList != null && GetValidationPausedLeadList.Count == 0)
                                {
                                    parent.LeadID = GetAllLeadListBaseOnMSSalesTPID[0].Id;
                                    parent.LeadTitle = GetAllLeadListBaseOnMSSalesTPID[0].Contains("subject") ? GetAllLeadListBaseOnMSSalesTPID[0].GetAttributeValue<string>("subject").ToString() : string.Empty;
                                    parent.AgreementExpirationDate = GetAllLeadListBaseOnMSSalesTPID[0].Contains("new_agreementexpirationdate") ? GetAllLeadListBaseOnMSSalesTPID[0].GetAttributeValue<DateTime>("new_agreementexpirationdate") : DateTime.MinValue;
                                    parent.ExpiringAmount = GetAllLeadListBaseOnMSSalesTPID[0].Contains("new_expiringamount") ? GetAllLeadListBaseOnMSSalesTPID[0].GetAttributeValue<decimal>("new_expiringamount") : 0;
                                }
                            }
                        }

                        StringBuilder MergedProductDetails = new StringBuilder(string.Empty);
                        StringBuilder MergedContactDetails = new StringBuilder(string.Empty);
                        List<DateTime?> ExpirationDateList = new List<DateTime?>();
                        StringBuilder MergedAgreementDetails = new StringBuilder(string.Empty);
                        StringBuilder strTopic = new StringBuilder(string.Empty);
                        decimal? ExpiringAmount = decimal.MinValue;
                        string ValidationCompletedTime = string.Empty;

                        if (!string.IsNullOrWhiteSpace(parent.CampaignType) && parent.CampaignType.ToUpper() == "RENEWALS" && (parent.MergeMultipleLeadsForSameCustomer == null || (parent.MergeMultipleLeadsForSameCustomer != null && parent.MergeMultipleLeadsForSameCustomer.Value == Convert.ToInt32(100000000))))
                        {
                            mergeRequests.Requests.Clear();
                            Guid parentId = parent.LeadID;
                            string parenttpId = parent.MSSalesTPID;
                            bool msxAccountId = true;
                            IsPaused = false;
                            Guid parentML = parent.CustomerList;
                            MergedAgreementDetails = new StringBuilder(string.Empty);
                            strTopic = new StringBuilder(string.Empty);
                            strTopic.Append(parent.LeadTitle);
                            MergedProductDetails = new StringBuilder(string.Empty);
                            MergedContactDetails = new StringBuilder(string.Empty);
                            ExpirationDateList = new List<DateTime?>();
                            if (parent.AgreementExpirationDate != null && parent.AgreementExpirationDate != DateTime.MinValue)
                            {
                                ExpirationDateList.Add(parent.AgreementExpirationDate);
                            }
                            ExpiringAmount = parent.ExpiringAmount;
                            foreach (Entity child in allLeadsList)
                            {
                                Guid childId = child.GetAttributeValue<Guid>("leadid");
                                string childtpId = child.GetAttributeValue<string>("new_mssalestpid");
                                string mergeMultipleLeadsForSameCustomer = child.GetAttributeValue<OptionSetValue>("new_mergemultipleleadsforsamecustomer") != null ? child.FormattedValues["new_mergemultipleleadsforsamecustomer"] : string.Empty;

                                Guid childML = child.Contains("new_customerlistname") ? child.GetAttributeValue<EntityReference>("new_customerlistname").Id : Guid.Empty;

                                if (parentId != childId && parentML == childML && !string.IsNullOrWhiteSpace(parenttpId) && !string.IsNullOrWhiteSpace(childtpId) && parenttpId == childtpId && (string.IsNullOrWhiteSpace(mergeMultipleLeadsForSameCustomer) || mergeMultipleLeadsForSameCustomer.ToLower() == "yes"))
                                {
                                    if (child.GetAttributeValue<OptionSetValue>("statuscode").Value == Convert.ToInt32(100000038))
                                    {
                                        Entity lead = new Entity("lead");
                                        lead.Id = parent.LeadID;
                                        lead["statuscode"] = new OptionSetValue(100000039);
                                        lead["new_mergestatus"] = "";
                                        _orgService.Update(lead);
                                        batch = 0;
                                        IsPaused = true;
                                        break;
                                    }
                                    batch++;
                                    totalChildeads++;
                                    string AgreementId = child.Contains("new_agreementid") ? child.GetAttributeValue<string>("new_agreementid") : string.Empty;
                                    DateTime AgreementExpirationDate = child.Contains("new_agreementexpirationdate") ? child.GetAttributeValue<DateTime>("new_agreementexpirationdate") : DateTime.MinValue;
                                    string ResellerName = child.Contains("new_resellername") ? child.GetAttributeValue<string>("new_resellername") : string.Empty;
                                    string DistributorName = child.Contains("new_distributorname") ? child.GetAttributeValue<string>("new_distributorname") : string.Empty;
                                    string AdvisorName = child.Contains("new_advisorname") ? child.GetAttributeValue<string>("new_advisorname") : string.Empty;
                                    string IsAutoCreatedLead = child.Contains("new_isautocreatedlead") ? child.GetAttributeValue<bool>("new_isautocreatedlead").ToString() : string.Empty;
                                    string LicensingProgram = child.Contains("new_licensingprogram") ? child.GetAttributeValue<string>("new_licensingprogram") : string.Empty;
                                    string ExpiringProductDetails = child.Contains("new_expiringproductdetails") ? child.GetAttributeValue<string>("new_expiringproductdetails") : string.Empty;
                                    decimal? ChildExpiringAmount = child.Contains("new_expiringamount") ? child.GetAttributeValue<decimal?>("new_expiringamount") : 0;
                                    string firstname = child.Contains("firstname") ? child.GetAttributeValue<string>("firstname") : string.Empty;
                                    string lastname = child.Contains("lastname") ? child.GetAttributeValue<string>("lastname") : string.Empty;
                                    string jobtitle = child.Contains("jobtitle") ? child.GetAttributeValue<string>("jobtitle") : string.Empty;
                                    string emailaddress = child.Contains("emailaddress1") ? child.GetAttributeValue<string>("emailaddress1") : string.Empty;
                                    string mobilephone = child.Contains("mobilephone") ? child.GetAttributeValue<string>("mobilephone") : string.Empty;
                                    ValidationCompletedTime = child.Contains("new_validationcompletedtime") ? child.GetAttributeValue<DateTime>("new_validationcompletedtime").ToString() : string.Empty;
                                    Entity leadEntity = new Entity("lead");
                                    leadEntity["new_leadupdatedby"] = "Auto Lead Merging Process";
                                    leadEntity["new_pendingvalidationstepsortorder"] = new OptionSetValue(Convert.ToInt32(100000008));


                                    MergedAgreementDetails.Append($"AgreementID: {AgreementId}");

                                    if (AgreementExpirationDate != DateTime.MinValue && AgreementExpirationDate != null)
                                    {
                                        MergedAgreementDetails.AppendLine($" - Expiration Date: {AgreementExpirationDate.ToString("yyyy-MM-dd")}");
                                        ExpirationDateList.Add(Convert.ToDateTime(AgreementExpirationDate.ToString()));
                                    }
                                    else
                                    {
                                        MergedAgreementDetails.AppendLine(" - Expiration Date: ");
                                    }

                                    if (!string.IsNullOrEmpty(IsAutoCreatedLead) && IsAutoCreatedLead.ToLower() == "true")
                                    {
                                        ExpiringAmount += ChildExpiringAmount;

                                        //MergedAgreementDetails.AppendLine(" - Partner Details: ");

                                        if (!string.IsNullOrEmpty(ResellerName) && ResellerName.ToLower() != "n/a")
                                        {
                                            MergedAgreementDetails.AppendLine($" - Reseller: {ResellerName}");
                                        }
                                        if (!string.IsNullOrEmpty(DistributorName) && DistributorName.ToLower() != "n/a")
                                        {
                                            MergedAgreementDetails.AppendLine($" - Distributor: {DistributorName}");
                                        }

                                        if (!string.IsNullOrEmpty(AdvisorName) && AdvisorName.ToLower() != "n/a")
                                        {
                                            MergedAgreementDetails.AppendLine($" - Advisor: {AdvisorName}");
                                        }

                                        if (!string.IsNullOrEmpty(AgreementId) || !string.IsNullOrEmpty(LicensingProgram))
                                        {
                                            strTopic.Append($", {AgreementId} ({LicensingProgram})");
                                        }

                                        MergedProductDetails.Append("AgreementID: " + AgreementId);

                                        if (AgreementExpirationDate != null && AgreementExpirationDate != DateTime.MinValue)
                                        {
                                            MergedProductDetails.AppendLine($" - Expiration Date: {AgreementExpirationDate.ToString("yyyy-MM-dd")}");
                                        }
                                        else
                                        {
                                            MergedProductDetails.AppendLine(" - Expiration Date: ");

                                        }
                                        MergedProductDetails.AppendLine(ExpiringProductDetails);
                                    }

                                    MergedContactDetails.Append("AgreementID: " + AgreementId);

                                    if (AgreementExpirationDate != null && AgreementExpirationDate != DateTime.MinValue)
                                    {
                                        MergedContactDetails.AppendLine($" - Expiration Date: {AgreementExpirationDate.ToString("yyyy-MM-dd")}");
                                    }
                                    else
                                    {
                                        MergedContactDetails.AppendLine(" - Expiration Date:  ");
                                    }
                                    if (!string.IsNullOrEmpty(firstname) || !string.IsNullOrEmpty(lastname))
                                    {
                                        MergedContactDetails.Append($" - Primary Contact: {firstname} {lastname}");
                                    }
                                    if (!string.IsNullOrEmpty(emailaddress))
                                    {
                                        MergedContactDetails.Append($", Email {emailaddress}");
                                    }
                                    if (!string.IsNullOrEmpty(mobilephone))
                                    {
                                        MergedContactDetails.AppendLine($", Phone {mobilephone}");
                                    }
                                    else
                                    {
                                        MergedContactDetails.AppendLine();
                                    }

                                    MergeRequest mergeRequest = new MergeRequest();

                                    if (msxAccountId && string.IsNullOrEmpty(parent.MSXAccountID) && child.Contains("new_msxaccountid"))
                                    {
                                        leadEntity["new_msxaccountid"] = child.GetAttributeValue<string>("new_msxaccountid");
                                        msxAccountId = false;
                                    }

                                    //MergeRequest for leads to Merge
                                    mergeRequest.SubordinateId = childId;
                                    mergeRequest.Target = new EntityReference("lead", parentId);
                                    mergeRequest.UpdateContent = leadEntity;
                                    mergeRequests.Requests.Add(mergeRequest);
                                    mergeRequest.Parameters.Add("ValidationCompletedTime", ValidationCompletedTime);

                                }
                            }
                        }

                        if (batch > 0 && batch <= 1000)
                        {

                            if (mergeRequests != null && mergeRequests.Requests.Count > 0)
                            {
                                foreach (var mergechildleads in mergeRequests.Requests)
                                {
                                    Entity lead = new Entity("lead");
                                    lead.Id = new Guid(mergechildleads["SubordinateId"].ToString());
                                    if (string.IsNullOrEmpty(mergechildleads.Parameters["ValidationCompletedTime"].ToString()))
                                    {
                                        lead["new_validationcompletedtime"] = DateTime.UtcNow;
                                    }
                                    lead["new_parentlead"] = new EntityReference("lead", new Guid(((EntityReference)mergechildleads["Target"]).Id.ToString()));
                                    var updateRequest = new UpdateRequest();
                                    updateRequest.Target = lead;
                                    updateLeadsRequest.Requests.Add(updateRequest);
                                    mergechildleads.Parameters.Remove("ValidationCompletedTime");
                                }
                            }
                            ExecuteMultipleResponse responseWithResults = (ExecuteMultipleResponse)_orgService.Execute(mergeRequests);
                            _manager.ExecuteBulkRequest(updateLeadsRequest, "Leadvalidationexpiredupdate for child leads");

                            mergeRequests.Requests.Clear();
                            updateLeadsRequest.Requests.Clear();
                        }
                        else if (batch != 0 && batch > 1000)
                        {
                            foreach (var Merge in mergeRequests.Requests)
                            {
                                FinalmergeRequests.Requests.Add(Merge);
                                Count++;
                                if (Count == 1000)
                                {
                                    if (mergeRequests != null && mergeRequests.Requests.Count > 0)
                                    {
                                        foreach (var mergechildleads in mergeRequests.Requests)
                                        {
                                            Entity lead = new Entity("lead");
                                            lead.Id = new Guid(mergechildleads["SubordinateId"].ToString());
                                            if (string.IsNullOrEmpty(mergechildleads.Parameters["ValidationCompletedTime"].ToString()))
                                            {
                                                lead["new_validationcompletedtime"] = DateTime.UtcNow;
                                            }
                                            lead["new_parentlead"] = new EntityReference("lead", new Guid(((EntityReference)mergechildleads["Target"]).Id.ToString()));
                                            var updateRequest = new UpdateRequest();
                                            updateRequest.Target = lead;
                                            updateLeadsRequest.Requests.Add(updateRequest);
                                        }
                                        ExecuteMultipleResponse responseWithResults = (ExecuteMultipleResponse)_orgService.Execute(FinalmergeRequests);
                                        _manager.ExecuteBulkRequest(updateLeadsRequest, "Leadvalidationexpiredupdate for child leads");
                                    }

                                    mergeRequests.Requests.Clear();
                                    updateLeadsRequest.Requests.Clear();
                                    FinalmergeRequests.Requests.Clear();
                                    Count = 0;
                                }
                            }
                            if (Count > 0)
                            {
                                if (mergeRequests != null && mergeRequests.Requests.Count > 0)
                                {
                                    foreach (var mergechildleads in mergeRequests.Requests)
                                    {
                                        Entity lead = new Entity("lead");
                                        lead.Id = new Guid(mergechildleads["SubordinateId"].ToString());
                                        if (string.IsNullOrEmpty(mergechildleads.Parameters["ValidationCompletedTime"].ToString()))
                                        {
                                            lead["new_validationcompletedtime"] = DateTime.UtcNow;
                                        }
                                        lead["new_parentlead"] = new EntityReference("lead", new Guid(((EntityReference)mergechildleads["Target"]).Id.ToString()));
                                        var updateRequest = new UpdateRequest();
                                        updateRequest.Target = lead;
                                        updateLeadsRequest.Requests.Add(updateRequest);
                                    }
                                    ExecuteMultipleResponse responseWithResults = (ExecuteMultipleResponse)_orgService.Execute(FinalmergeRequests);
                                    _manager.ExecuteBulkRequest(updateLeadsRequest, "Leadvalidationexpiredupdate for child leads");
                                }

                                mergeRequests.Requests.Clear();
                                updateLeadsRequest.Requests.Clear();
                                FinalmergeRequests.Requests.Clear();
                                Count = 0;
                            }
                        }

                        if (!IsPaused)
                        {
                            Entity lead = new Entity("lead");
                            lead.Id = parent.LeadID;

                            if (batch > 0)
                            {
                                lead["new_mergedcontactdetails"] = MergedContactDetails.ToString();
                                lead["new_mergedexpiringamount"] = ExpiringAmount;
                                lead["new_mergedagreementdetails"] = MergedAgreementDetails.ToString();
                                lead["new_mergedproductdetails"] = MergedProductDetails.ToString();
                                lead["new_ismergedlead"] = true;
                                if (ExpirationDateList.Count > 0)
                                {
                                    lead["new_mergedminexpirationdate"] = ExpirationDateList.Min();
                                }
                                if (!string.IsNullOrEmpty(strTopic.ToString()))
                                {
                                    lead["subject"] = _manager.LimitCharacterCount(strTopic.ToString(), 200);
                                }
                            }
                            lead["new_mergestatus"] = "Merge Complete";
                            lead["new_mergeprocesscompletedtime"] = DateTime.UtcNow;
                            lead["statuscode"] = new OptionSetValue(100000008);
                            lead["new_pendingvalidationstepsortorder"] = new OptionSetValue(Convert.ToInt32(100000008));
                            _orgService.Update(lead);
                            IsPaused = false;
                            batch = 0;

                        }
                    }

                    UpdateAutoLeadStatus(customerList.CustomerList);
                }
            }
            DateTime EndTime = DateTime.UtcNow;
            SqlConnection con = new SqlConnection(LeadValidationFunctions._sqlConnection);
            //AuthenticationResult authenticationResult = _manager.AADAunthenticationResult();
            con.AccessToken = Manager.getAccessToken(System.Environment.GetEnvironmentVariable("ResourceId"), System.Environment.GetEnvironmentVariable("VTCPManagedIdentity"), true).Result;
            SqlCommand SqlCommands = new SqlCommand();
            SqlCommands = new SqlCommand("AzureFunctionsDataLoad", con);
            SqlCommands.CommandType = CommandType.StoredProcedure;
            SqlCommands.Parameters.AddWithValue("@FunctionName", "MergeAutoCreatedLeads");
            SqlCommands.Parameters.AddWithValue("@ProcessStartTime", StartTime.ToString());
            SqlCommands.Parameters.AddWithValue("@FunctionStartTime", StartTime.ToString());
            SqlCommands.Parameters.AddWithValue("@FunctionEndTime", EndTime.ToString());
            SqlCommands.Parameters.AddWithValue("@InitialRowsAffected", Leads.Count.ToString());
            SqlCommands.Parameters.AddWithValue("@TotalRowsAffected", (Leads.Count + totalChildeads).ToString());
            con.Open();
            SqlCommands.ExecuteNonQuery();
            con.Close();
        }

        public void UpdateAutoLeadStatus(Guid CustomerList)
        {
            EntityCollection leadscoll = new EntityCollection();
            int status = Convert.ToInt32(0);

            QueryExpression AutoCreatedLeads = new QueryExpression()
            {
                EntityName = "lead",
                ColumnSet = new ColumnSet(true),
                Criteria = new FilterExpression()
            };
            AutoCreatedLeads.Criteria.AddCondition("statecode", ConditionOperator.Equal, status);
            AutoCreatedLeads.Criteria.AddCondition("new_customerlistname", ConditionOperator.Equal, CustomerList);
            AutoCreatedLeads.Criteria.AddCondition("new_mergestatus", ConditionOperator.Null);

            leadscoll = _orgService.RetrieveMultiple(AutoCreatedLeads);

            if (leadscoll != null && leadscoll.Entities.Count == 0)
            {
                QueryExpression AutoLeadCreation = new QueryExpression()
                {
                    EntityName = "new_autoleadcreationrequest",
                    ColumnSet = new ColumnSet(true),
                    Criteria = new FilterExpression()
                };
                AutoLeadCreation.Criteria.AddCondition("new_marketinglist", ConditionOperator.Equal, CustomerList);
                leadscoll = _orgService.RetrieveMultiple(AutoLeadCreation);

                Entity AutoLeadRequest = new Entity("new_autoleadcreationrequest");
                AutoLeadRequest.Id = leadscoll.Entities[0].Id;
                AutoLeadRequest["statuscode"] = new OptionSetValue(100000003);
                _orgService.Update(AutoLeadRequest);
            }
        }

        public EntityCollection GetLeadsForMerging()
        {
            int statusReason = Convert.ToInt32(100000008);
            int status = Convert.ToInt32(0);
            int statusPaused = Convert.ToInt32(100000039);
            // Aggreement Merge Pending Validation Step
            int pendingValidationStepSortOrder = Convert.ToInt32(100000007);

            EntityCollection LeadsForMerging = new EntityCollection();
            EntityCollection PausedLeadsForMerging = new EntityCollection();
            QueryExpression leadattributes = new QueryExpression()
            {
                EntityName = "lead",
                ColumnSet = new ColumnSet(true),
                Criteria = new FilterExpression(),
                TopCount = 250
            };
            leadattributes.Criteria.AddCondition("statuscode", ConditionOperator.Equal, statusReason);
            //leadattributes.Criteria.AddCondition("new_mergestatus", ConditionOperator.Null);
            //leadattributes.Criteria.AddCondition("new_isautocreatedlead", ConditionOperator.Equal, true);
            //leadattributes.Criteria.AddCondition("new_vtcpduplicatedetectioncomplete", ConditionOperator.Equal, true);
            leadattributes.Criteria.AddCondition("new_pendingvalidationstepsortorder", ConditionOperator.Equal, pendingValidationStepSortOrder);


            //Link Query to pull Lead Validation and Merging in Progress Status Autolead
            LinkEntity leAutoLeadEntityType = new LinkEntity()
            {
                LinkFromEntityName = "lead",
                LinkToEntityName = "new_autoleadcreationrequest",
                LinkFromAttributeName = "new_customerlistname",
                LinkToAttributeName = "new_marketinglist",
                JoinOperator = JoinOperator.Inner,
                Columns = new ColumnSet(true),
                LinkCriteria = new FilterExpression(LogicalOperator.Or)
                {
                    Conditions = { new ConditionExpression("statuscode", ConditionOperator.Equal, Convert.ToInt32(100000002)),
                     new ConditionExpression("statuscode", ConditionOperator.Equal, Convert.ToInt32(100000003))}
                }
            };

            OrderExpression order = new OrderExpression();
            order.AttributeName = "new_agreementprofilesortorder";
            order.OrderType = OrderType.Ascending;

            OrderExpression orderExpDate = new OrderExpression();
            orderExpDate.AttributeName = "new_agreementexpirationdate";
            orderExpDate.OrderType = OrderType.Ascending;

            OrderExpression orderExpAmount = new OrderExpression();
            orderExpAmount.AttributeName = "new_expiringamount";
            orderExpAmount.OrderType = OrderType.Descending;

            leadattributes.Orders.Add(order);
            leadattributes.Orders.Add(orderExpDate);
            leadattributes.Orders.Add(orderExpAmount);
            leadattributes.LinkEntities.Add(leAutoLeadEntityType);
            LeadsForMerging = _orgService.RetrieveMultiple(leadattributes);

            //if (LeadsForMerging.Entities.Count < 250)
            //{
            //    QueryExpression leadattributespaused = new QueryExpression()
            //    {
            //        EntityName = "lead",
            //        ColumnSet = new ColumnSet(true),
            //        Criteria = new FilterExpression(),
            //        TopCount = 250 - LeadsForMerging.Entities.Count
            //    };
            //    leadattributespaused.Criteria.AddCondition("statuscode", ConditionOperator.Equal, statusPaused);
            //    leadattributespaused.Criteria.AddCondition("statecode", ConditionOperator.Equal, status);
            //    leadattributespaused.Criteria.AddCondition("new_importedaccount", ConditionOperator.NotNull);
            //    leadattributespaused.Criteria.AddCondition("new_is_msx_contacted", ConditionOperator.NotNull);
            //    leadattributespaused.Criteria.AddCondition("new_ismandatory", ConditionOperator.Equal, isYes);
            //    leadattributespaused.Criteria.AddCondition("new_subsidiary", ConditionOperator.NotNull);
            //    leadattributespaused.Criteria.AddCondition("new_is_lir_contacted", ConditionOperator.NotNull);
            //    leadattributespaused.Criteria.AddCondition("new_isclascontacted", ConditionOperator.NotNull);
            //    leadattributespaused.Criteria.AddCondition("new_isblacklisted", ConditionOperator.NotNull);
            //    leadattributespaused.Criteria.AddCondition("new_mergestatus", ConditionOperator.Null);
            //    leadattributespaused.Criteria.AddCondition("new_isautocreatedlead", ConditionOperator.Equal, true);
            //    leadattributespaused.Criteria.AddCondition("modifiedon", ConditionOperator.OlderThanXHours, 4);
            //    leadattributespaused.Criteria.AddFilter(MalFilter);
            //    leadattributespaused.Orders.Add(order);
            //    leadattributespaused.Orders.Add(orderExpDate);
            //    leadattributespaused.Orders.Add(orderExpAmount);
            //    leadattributespaused.LinkEntities.Add(leAutoLeadEntityType);
            //    PausedLeadsForMerging = _orgService.RetrieveMultiple(leadattributespaused);
            //}
            //if (PausedLeadsForMerging.Entities.Count > 0)
            //{
            //    foreach (Entity lead in PausedLeadsForMerging.Entities)
            //    {
            //        LeadsForMerging.Entities.Add(lead);
            //    }
            //}
            return LeadsForMerging;
        }
        #endregion

        #region Status, Status reason for leads

        /// <summary>
        /// To retrieve Leads for Status update
        /// </summary>
        /// <returns></returns>
        public EntityCollection GetIAPSLeadsForStatusChange()
        {
            _log.LogInformation("Execution started: ", "GetIAPSLeadsForStatusChange");

            EntityCollection leadscoll = new EntityCollection();
            int statusReason = Convert.ToInt32(100000008);
            int status = Convert.ToInt32(0);
            int topCount = 1000;
            // Lead Status change Pending Validation Step
            int pendingValidationStepSortOrder = Convert.ToInt32(100000008);

            QueryExpression leadattributes = new QueryExpression()
            {
                EntityName = "lead",
                ColumnSet = new ColumnSet(true),
                Criteria = new FilterExpression(),
                TopCount = topCount
            };
            leadattributes.AddOrder("prioritycode", OrderType.Descending);
            leadattributes.AddOrder("createdon", OrderType.Ascending);
            FilterExpression filterlead = new FilterExpression(LogicalOperator.And);
            // filterlead.AddCondition("statuscode", ConditionOperator.Equal, statusReason);
            // filterlead.AddCondition("new_isblacklisted", ConditionOperator.NotNull);
            filterlead.AddCondition("new_pendingvalidationstepsortorder", ConditionOperator.Equal, pendingValidationStepSortOrder);
            //FilterExpression AutoleadRequest = new FilterExpression(LogicalOperator.Or);

            //FilterExpression NotAutolead = new FilterExpression(LogicalOperator.And);
            //NotAutolead.AddCondition("new_isautocreatedlead", ConditionOperator.Equal, false);

            //FilterExpression Autolead = new FilterExpression(LogicalOperator.And);
            //Autolead.AddCondition("new_isautocreatedlead", ConditionOperator.Equal, true);
            //Autolead.AddCondition("new_mergestatus", ConditionOperator.NotNull);
            //Autolead.AddCondition("new_vtcpduplicatedetectioncomplete", ConditionOperator.Equal, true);

            FilterExpression filterStatus = new FilterExpression(LogicalOperator.Or);
            filterStatus.AddCondition("statuscode", ConditionOperator.Equal, statusReason);
            filterStatus.AddCondition("statuscode", ConditionOperator.Equal, Convert.ToInt32(100000033));

            //AutoleadRequest.AddFilter(Autolead);
            //AutoleadRequest.AddFilter(NotAutolead);
            //filterlead.AddFilter(AutoleadRequest);
            filterlead.AddFilter(filterStatus);

            leadattributes.Criteria.AddFilter(filterlead);
            leadscoll = _orgService.RetrieveMultiple(leadattributes);
            return leadscoll;
        }


        /// <summary>
        /// To update Leads with Status, Status reason, Stage
        /// </summary>
        public void UpdateLeadsWithStatus()
        {
            DateTime StartTime = DateTime.UtcNow;
            int batch = 0;
            EntityCollection leadscoll = GetIAPSLeadsForStatusChange();
            List<CsLeads> Leads = new List<CsLeads>();

            ExecuteMultipleSettings excuteMultipleSettings = new ExecuteMultipleSettings();
            excuteMultipleSettings.ContinueOnError = true;
            excuteMultipleSettings.ReturnResponses = true;

            ExecuteMultipleRequest updateLeadsRequest = new ExecuteMultipleRequest();
            updateLeadsRequest.Requests = new OrganizationRequestCollection();
            updateLeadsRequest.Settings = excuteMultipleSettings;
            Guid LeadID = Guid.Empty;
            OptionSetValue Is_LIR_Contacted = null;
            OptionSetValue Is_MSX_Contacted = null;
            OptionSetValue Is_MAL = null;
            OptionSetValue Is_ExclusionList = null;
            OptionSetValue PreApproveForMSXUpload = null;
            string Area = string.Empty;
            string Region = string.Empty;
            string Subsidiary = string.Empty;
            string Salesmotion = string.Empty;
            string Vendor = string.Empty;
            string MSXStatus = string.Empty;
            string CLAS_VLAllocation = string.Empty;
            string CLAS_MalDomain = string.Empty;
            string CLAS_MalExactNameMatch = string.Empty;
            string CLAS_SubSegment = string.Empty;
            string CLAS_Nro = string.Empty;
            string CLAS_TPNameWaste = string.Empty;
            string PrimaryProduct = string.Empty;
            string TopUnmanaged_CSM = string.Empty;
            string CLAS_IsABMAccount = string.Empty;
            string CampaignType = string.Empty;
            string ExistingMSXActivitySameVendor = string.Empty;
            string ExistingMSXActivityOtherTeam = string.Empty;
            string Program = string.Empty;
            string CLAS_IsNonProfit = string.Empty;
            int OpenLeadsSameVendor = 0;
            int OpenLeadsOtherVendor = 0;
            int ActiveOpportunitiesSameVendor = 0;
            int ActiveOpportunitiesOtherVendor = 0;
            int AgingDSQLeadsSameVendor = 0;
            int ExpiredClosedOppSameVendor = 0;
            int AgingDSQLeadsOtherVendor = 0;
            int ExpiredClosedOppOtherVendor = 0;
            string MSXAccountID = string.Empty;
            string CampaignCode = string.Empty;
            string Email = string.Empty;
            string Phone = string.Empty;
            string ImportedAccount = string.Empty;
            string City = string.Empty;
            string MSXLeadOwnerAlias = string.Empty;
            string DefaultTeamSubsidiaryOwner = string.Empty;
            string DefaultTeamOwner = string.Empty;
            string CampaignName = string.Empty;
            string LicenseProgram = string.Empty;
            string IsAutoLead = string.Empty;
            string duplicateagreementdetails = string.Empty;
            StringBuilder duplicateagreementdetails_Warning = null;
            bool hasValidPhoneOrEmail = false;
            bool hasValidCampaignCode = false;
            bool hasValidMSXAlias = false;
            bool hasEmail = false;
            bool hasValidEmail = false;
            bool hasAllowedEmail = false;
            bool hasPhone = false;
            bool hasValidPhone = false;
            bool isPhoneZeroes = false;
            bool ExistingMSX = false;
            const string defaultCampaign = "<Local Allocadia ID>";
            OptionSetValueCollection DisqualifyReasons = new OptionSetValueCollection();
            string StatusReason = string.Empty;
            string ValidationCompletedTime = string.Empty;
            OptionSetValue OverrideDisqualification = null;
            EntityCollection CustomDsqColl = new EntityCollection();
            OptionSetValueCollection DisqualificationReasonsForOverride = new OptionSetValueCollection();
            Guid CampaginNameId = Guid.Empty;
            QueryExpression CustomDisqualification = new QueryExpression
            {
                EntityName = "new_customleaddisqualificationconfiguration",
                ColumnSet = new ColumnSet(true),
                Criteria = new FilterExpression()

            };
            CustomDisqualification.Criteria.AddCondition("statecode", ConditionOperator.Equal, 0);
            CustomDisqualification.Criteria.AddCondition("statuscode", ConditionOperator.Equal, 1);
            CustomDsqColl = _orgService.RetrieveMultiple(CustomDisqualification);
            List<DsqLeads> DsqLead = new List<DsqLeads>();
            if (CustomDsqColl != null && CustomDsqColl.Entities.Count > 0)
            {
                foreach (var dsq in CustomDsqColl.Entities)
                {
                    DsqLead.Add(new DsqLeads
                    {
                        DsqVendor = dsq.Contains("new_vendorname") ? dsq.GetAttributeValue<EntityReference>("new_vendorname").Name : null,
                        DsqArea = dsq.Contains("new_area") ? dsq.GetAttributeValue<EntityReference>("new_area").Name : null,
                        DsqColumn = dsq.Contains("new_columnname") ? dsq["new_columnname"].ToString() : string.Empty,
                        DsqOperator = dsq.Contains("new_columnoperator") ? dsq.FormattedValues["new_columnoperator"] : string.Empty,
                        DsqOverride = dsq.Contains("new_canbeoverridden") ? dsq.FormattedValues["new_canbeoverridden"].ToString() : string.Empty,
                        DsqReason = dsq.Contains("new_disqualificationreasondetails") ? dsq["new_disqualificationreasondetails"].ToString() : string.Empty,
                        DsqRegion = dsq.Contains("new_region") ? dsq.GetAttributeValue<EntityReference>("new_region").Name : null,
                        DsqSalesmotion = dsq.Contains("new_salesmotion") ? dsq.GetAttributeValue<EntityReference>("new_salesmotion").Name : null,
                        DsqSubsidiary = dsq.Contains("new_subsidiary") ? dsq.GetAttributeValue<EntityReference>("new_subsidiary").Name : null,
                        DsqValue = dsq.Contains("new_columnvalue") ? dsq["new_columnvalue"].ToString() : string.Empty,
                        DsqId = dsq.Id.ToString()


                    });
                }
            }
            if (leadscoll != null && leadscoll.Entities.Count > 0)
            {
                foreach (Entity entityObject in leadscoll.Entities)
                {
                    duplicateagreementdetails_Warning = new StringBuilder();
                    hasValidPhoneOrEmail = false;
                    hasValidCampaignCode = false;
                    hasValidMSXAlias = false;
                    hasEmail = false;
                    hasValidEmail = false;
                    hasAllowedEmail = false;
                    hasPhone = false;
                    hasValidPhone = false;
                    isPhoneZeroes = false;
                    ExistingMSX = false;

                    StringBuilder DataWarnings = new StringBuilder();
                    List<DsqLeads> finalDsq = new List<DsqLeads>();
                    List<DsqLeads> dsqfilter = new List<DsqLeads>();
                    string DsqReasonDetails = string.Empty;
                    string DsqIds = string.Empty;
                    LeadID = entityObject.Contains("leadid") ? new System.Guid(entityObject["leadid"].ToString()) : Guid.Empty;
                    Is_LIR_Contacted = entityObject.Contains("new_is_lir_contacted") ? (OptionSetValue)entityObject.Attributes["new_is_lir_contacted"] : null;
                    Is_MSX_Contacted = entityObject.Contains("new_is_msx_contacted") ? (OptionSetValue)entityObject.Attributes["new_is_msx_contacted"] : null;
                    Is_MAL = entityObject.Contains("new_is_mal") ? (OptionSetValue)entityObject.Attributes["new_is_mal"] : null;
                    Is_ExclusionList = entityObject.Contains("new_isblacklisted") ? (OptionSetValue)entityObject.Attributes["new_isblacklisted"] : null;
                    MSXStatus = entityObject.Contains("new_msxopportunitystatus") ? entityObject["new_msxopportunitystatus"].ToString() : string.Empty;
                    CLAS_VLAllocation = entityObject.Contains("new_clas_vlallocation") ? entityObject["new_clas_vlallocation"].ToString() : string.Empty;
                    CLAS_MalDomain = entityObject.Contains("new_clas_maldomain") ? entityObject["new_clas_maldomain"].ToString() : string.Empty;
                    CLAS_MalExactNameMatch = entityObject.Contains("new_clas_malexactnamematch") ? entityObject["new_clas_malexactnamematch"].ToString() : string.Empty;
                    CLAS_SubSegment = entityObject.Contains("new_clas_subsegment") ? entityObject["new_clas_subsegment"].ToString() : string.Empty;
                    CLAS_Nro = entityObject.Contains("new_clas_nro") ? entityObject["new_clas_nro"].ToString() : string.Empty;
                    CLAS_TPNameWaste = entityObject.Contains("new_clas_tpnamewaste") ? entityObject["new_clas_tpnamewaste"].ToString() : string.Empty;
                    PreApproveForMSXUpload = entityObject.Contains("new_preapproveformsxupload") ? (OptionSetValue)entityObject.Attributes["new_preapproveformsxupload"] : null;
                    PrimaryProduct = entityObject.Contains("new_primaryproductcampaign") ? entityObject["new_primaryproductcampaign"].ToString() : string.Empty;
                    TopUnmanaged_CSM = entityObject.Contains("new_clas_topunmanagedcsm") ? entityObject["new_clas_topunmanagedcsm"].ToString() : string.Empty;
                    Vendor = entityObject.Contains("new_vendorteam") ? entityObject.GetAttributeValue<EntityReference>("new_vendorteam").Name : null;
                    Area = entityObject.Contains("new_area") ? entityObject.GetAttributeValue<EntityReference>("new_area").Name : null;
                    Region = entityObject.Contains("new_region") ? entityObject.GetAttributeValue<EntityReference>("new_region").Name : null;
                    Subsidiary = entityObject.Contains("new_subsidiary") ? entityObject.GetAttributeValue<EntityReference>("new_subsidiary").Name : null;
                    Salesmotion = entityObject.Contains("new_salesmotion") ? entityObject.GetAttributeValue<EntityReference>("new_salesmotion").Name : null;
                    CLAS_IsABMAccount = entityObject.Contains("new_clas_topunmanageddsr") ? entityObject["new_clas_topunmanageddsr"].ToString() : string.Empty;
                    CampaignType = entityObject.Contains("new_campaigntype") ? entityObject.GetAttributeValue<EntityReference>("new_campaigntype").Name : null;
                    ExistingMSXActivityOtherTeam = entityObject.Contains("new_existingmsxactivityotherteam") ? entityObject.GetAttributeValue<string>("new_existingmsxactivityotherteam").ToString() : string.Empty;
                    ExistingMSXActivitySameVendor = entityObject.Contains("new_existingmsxactivitysamevendor") ? entityObject.GetAttributeValue<string>("new_existingmsxactivitysamevendor").ToString() : string.Empty;
                    MSXAccountID = entityObject.Contains("new_matchedmsxaccountid") ? entityObject.GetAttributeValue<string>("new_matchedmsxaccountid").ToString() : string.Empty;
                    CampaignCode = entityObject.Contains("new_campaigncode") ? entityObject.GetAttributeValue<string>("new_campaigncode").ToString() : string.Empty;
                    CampaginNameId = entityObject.Contains("campaignid") ? entityObject.GetAttributeValue<EntityReference>("campaignid").Id : Guid.Empty;
                    Email = entityObject.Contains("emailaddress1") ? entityObject.GetAttributeValue<string>("emailaddress1").ToString() : string.Empty;
                    Phone = entityObject.Contains("mobilephone") ? entityObject.GetAttributeValue<string>("mobilephone").ToString() : string.Empty;
                    ImportedAccount = entityObject.Contains("new_importedaccount") ? entityObject.GetAttributeValue<string>("new_importedaccount").ToString() : string.Empty;
                    City = entityObject.Contains("address1_city") ? entityObject.GetAttributeValue<string>("address1_city").ToString() : string.Empty;
                    MSXLeadOwnerAlias = entityObject.Contains("new_msxleadowneralias") ? entityObject.GetAttributeValue<string>("new_msxleadowneralias").ToString() : string.Empty;
                    DefaultTeamSubsidiaryOwner = entityObject.Contains("new_defaultteamsubsidiaryown") ? entityObject.GetAttributeValue<string>("new_defaultteamsubsidiaryown").ToString() : string.Empty;
                    DefaultTeamOwner = entityObject.Contains("new_defaultteamowner") ? entityObject.GetAttributeValue<string>("new_defaultteamowner").ToString() : string.Empty;
                    StatusReason = entityObject.Contains("statuscode") ? entityObject.FormattedValues["statuscode"] : string.Empty;
                    CampaignName = entityObject.Contains("campaignid") ? entityObject.GetAttributeValue<EntityReference>("campaignid").Name : string.Empty;
                    LicenseProgram = entityObject.Contains("new_licensingprogram") ? entityObject.GetAttributeValue<string>("new_licensingprogram") : string.Empty;
                    IsAutoLead = entityObject.Contains("new_isautocreatedlead") ? entityObject.GetAttributeValue<bool>("new_isautocreatedlead").ToString() : string.Empty;
                    duplicateagreementdetails = entityObject.Contains("new_duplicateagreementdetails") ? entityObject.GetAttributeValue<string>("new_duplicateagreementdetails") : string.Empty;
                    ValidationCompletedTime = entityObject.Contains("new_validationcompletedtime") ? entityObject.GetAttributeValue<DateTime>("new_validationcompletedtime").ToString() : string.Empty;
                    OverrideDisqualification = entityObject.Contains("new_overridedisqualification") ? entityObject.GetAttributeValue<OptionSetValue>("new_overridedisqualification") : new OptionSetValue();
                    Program = entityObject.Contains("new_program") ? entityObject.GetAttributeValue<EntityReference>("new_program").Name : string.Empty;
                    CLAS_IsNonProfit = entityObject.Contains("new_clas_isnonprofit") ? entityObject.GetAttributeValue<string>("new_clas_isnonprofit") : string.Empty;

                    DisqualificationReasonsForOverride = _orgService.Retrieve("campaign", CampaginNameId, new ColumnSet("new_disqualificationreasonsforoverride")).GetAttributeValue<OptionSetValueCollection>("new_disqualificationreasonsforoverride");
                    DisqualifyReasons.Clear();
                    if (entityObject.Contains("new_overriddendisqualificationreasons"))
                    {
                        DisqualifyReasons.AddRange(entityObject.GetAttributeValue<OptionSetValueCollection>("new_overriddendisqualificationreasons"));
                    }
                    if (OverrideDisqualification != null && OverrideDisqualification.Value == 100000000 && DisqualificationReasonsForOverride != null)
                        DisqualifyReasons.AddRange(DisqualificationReasonsForOverride);

                    if (!string.IsNullOrWhiteSpace(duplicateagreementdetails))
                    {
                        if (duplicateagreementdetails.Contains("Agreement already loaded to VTCP by another vendor"))
                            duplicateagreementdetails_Warning.AppendLine("- Another vendor team has already created a lead for this agreement.");
                        if (duplicateagreementdetails.Contains($"Agreement already loaded to VTCP by {Vendor}"))
                            duplicateagreementdetails_Warning.AppendLine($"- {Vendor} team has already created a lead for this agreement.");
                    }
                    if (!string.IsNullOrWhiteSpace(ExistingMSXActivityOtherTeam))
                    {
                        string[] ExistingMSXActivityOtherTeamList = ExistingMSXActivityOtherTeam.Split('#');

                        OpenLeadsOtherVendor = Convert.ToInt32(Regex.Replace(ExistingMSXActivityOtherTeamList[1].Split(':')[1], "[^0-9.]", ""));
                        AgingDSQLeadsOtherVendor = Convert.ToInt32(Regex.Replace(ExistingMSXActivityOtherTeamList[2].Split(':')[1], "[^0-9.]", ""));
                        ActiveOpportunitiesOtherVendor = Convert.ToInt32(Regex.Replace(ExistingMSXActivityOtherTeamList[3].Split(':')[1], "[^0-9.]", ""));
                        ExpiredClosedOppOtherVendor = Convert.ToInt32(Regex.Replace(ExistingMSXActivityOtherTeamList[4].Split(':')[1], "[^0-9.]", ""));

                    }

                    if (!string.IsNullOrWhiteSpace(ExistingMSXActivitySameVendor))
                    {
                        string[] ExistingMSXActivitySameVendorList = ExistingMSXActivitySameVendor.Split('#');

                        OpenLeadsSameVendor = Convert.ToInt32(Regex.Replace(ExistingMSXActivitySameVendorList[1].Split(':')[1], "[^0-9.]", ""));
                        AgingDSQLeadsSameVendor = Convert.ToInt32(Regex.Replace(ExistingMSXActivitySameVendorList[2].Split(':')[1], "[^0-9.]", ""));
                        ActiveOpportunitiesSameVendor = Convert.ToInt32(Regex.Replace(ExistingMSXActivitySameVendorList[3].Split(':')[1], "[^0-9.]", ""));
                        ExpiredClosedOppSameVendor = Convert.ToInt32(Regex.Replace(ExistingMSXActivitySameVendorList[4].Split(':')[1], "[^0-9.]", ""));
                    }



                    foreach (var dsqlead in DsqLead)
                    {
                        if ((entityObject.GetAttributeValue<EntityReference>("new_vendorteam").Name == dsqlead.DsqVendor || dsqlead.DsqVendor == null) && (entityObject.GetAttributeValue<EntityReference>("new_area").Name == dsqlead.DsqArea || dsqlead.DsqArea == null) && (entityObject.GetAttributeValue<EntityReference>("new_region").Name == dsqlead.DsqRegion || dsqlead.DsqRegion == null) && (entityObject.GetAttributeValue<EntityReference>("new_salesmotion").Name == dsqlead.DsqSalesmotion || dsqlead.DsqSalesmotion == null) && (entityObject.GetAttributeValue<EntityReference>("new_subsidiary").Name == dsqlead.DsqSubsidiary || dsqlead.DsqSubsidiary == null))
                        {
                            dsqfilter.Add(dsqlead);
                        }
                    }
                    foreach (var leads in dsqfilter)
                    {
                        if (entityObject.Contains(leads.DsqColumn))
                        {
                            if (leads.DsqOperator == "Equals")
                            {
                                if (entityObject[leads.DsqColumn].ToString().ToLower() == leads.DsqValue.ToLower())
                                {
                                    finalDsq.Add(leads);
                                }
                            }
                            else if (leads.DsqOperator == "Not Equals")
                            {
                                if (entityObject[leads.DsqColumn].ToString().ToLower() != leads.DsqValue.ToLower())
                                {
                                    finalDsq.Add(leads);
                                }
                            }
                            else if (leads.DsqOperator == "Contains")
                            {
                                if (entityObject[leads.DsqColumn].ToString().ToLower().Contains(leads.DsqValue.ToLower()))
                                {
                                    finalDsq.Add(leads);
                                }
                            }
                            else if (leads.DsqOperator == "Does Not Contain")
                            {
                                if (!entityObject[leads.DsqColumn].ToString().ToLower().Contains(leads.DsqValue.ToLower()))
                                {
                                    finalDsq.Add(leads);
                                }
                            }
                            else if (leads.DsqOperator == "Less Than")
                            {

                                bool leadresult = Int64.TryParse(entityObject[leads.DsqColumn].ToString(), out long leadnumber);
                                bool dsqresult = Int64.TryParse(leads.DsqValue.ToString(), out long dsqnumber);
                                if (leadresult && dsqresult)
                                {
                                    if (leadnumber < dsqnumber)
                                    {
                                        finalDsq.Add(leads);
                                    }
                                }
                            }
                            else if (leads.DsqOperator == "Greater Than")
                            {
                                bool leadresult = Int64.TryParse(entityObject[leads.DsqColumn].ToString(), out long leadnumber);
                                bool dsqresult = Int64.TryParse(leads.DsqValue.ToString(), out long dsqnumber);
                                if (leadresult && dsqresult)
                                {
                                    if (leadnumber > dsqnumber)
                                    {
                                        finalDsq.Add(leads);
                                    }
                                }
                            }
                        }
                    }
                    Entity objLeadEntity = new Entity("lead");
                    objLeadEntity.Id = LeadID;
                    batch++;
                    if (finalDsq.Count > 0)
                    {
                        foreach (var final in finalDsq)
                        {
                            if (!string.IsNullOrEmpty(DsqReasonDetails))
                            {
                                DsqReasonDetails += string.Concat("; ", final.DsqReason);
                                DsqIds += string.Concat("; ", final.DsqId);
                            }
                            else
                            {
                                DsqReasonDetails += final.DsqReason;
                                DsqIds += final.DsqId;
                            }
                        }
                        objLeadEntity["new_iscustomdisqualified"] = true;
                        objLeadEntity["new_custom_dsqdetails"] = DsqReasonDetails;
                        objLeadEntity["new_customdisqualificationreasonids"] = DsqIds;
                    }
                    if (Program != null && Program.ToUpper() == "TSI" && !CLAS_IsNonProfit.ToLower().Contains("approved") && !DisqualifyReasons.Contains(new OptionSetValue(100000046)))
                    {
                        objLeadEntity["statecode"] = new OptionSetValue(statusDisqualified);
                        objLeadEntity["statuscode"] = new OptionSetValue(Convert.ToInt32(100000046));
                    }

                    else if (Is_ExclusionList != null && Is_ExclusionList.Value == isYes && !DisqualifyReasons.Contains(new OptionSetValue(100000017)))
                    {
                        objLeadEntity["statecode"] = new OptionSetValue(statusDisqualified);
                        objLeadEntity["statuscode"] = new OptionSetValue(Convert.ToInt32(100000017));
                    }
                    else if ((CampaignName.ToUpper() != "EA RENEWALS" || IsAutoLead.ToUpper() == "FALSE") && !string.IsNullOrWhiteSpace(CLAS_VLAllocation) && CLAS_VLAllocation.ToUpper() == "YES" && !DisqualifyReasons.Contains(new OptionSetValue(100000022)))//If type VL Allocation
                    {
                        objLeadEntity["statecode"] = new OptionSetValue(statusDisqualified);
                        objLeadEntity["statuscode"] = new OptionSetValue(Convert.ToInt32(100000022));
                    }
                    else if ((CampaignName.ToUpper() != "EA RENEWALS" || IsAutoLead.ToUpper() == "FALSE") && ((Is_MAL != null && Is_MAL.Value == Convert.ToInt32(100000003))
                          || (!string.IsNullOrWhiteSpace(CLAS_SubSegment) && !CLAS_SubSegment.ToUpper().Contains("SMB") && !CLAS_SubSegment.ToUpper().Contains("SM&C SCALE - CORPORATE"))) && !DisqualifyReasons.Contains(new OptionSetValue(100000018)))                      //If type Non-SMB
                    {
                        objLeadEntity["statecode"] = new OptionSetValue(statusDisqualified);
                        objLeadEntity["statuscode"] = new OptionSetValue(Convert.ToInt32(100000018));
                    }
                    else if ((CampaignName.ToUpper() != "EA RENEWALS" || IsAutoLead.ToUpper() == "FALSE") && ((Is_MAL != null && Is_MAL.Value == Convert.ToInt32(100000000))
                       || (!string.IsNullOrWhiteSpace(CLAS_MalDomain) && CLAS_MalDomain.ToUpper() == "YES")
                       || (!string.IsNullOrWhiteSpace(CLAS_MalExactNameMatch) && CLAS_MalExactNameMatch.ToUpper() == "YES")) && !DisqualifyReasons.Contains(new OptionSetValue(100000016)))    //If type MAL 
                    {
                        objLeadEntity["statecode"] = new OptionSetValue(statusDisqualified);
                        objLeadEntity["statuscode"] = new OptionSetValue(Convert.ToInt32(100000016));
                    }
                    else if (!string.IsNullOrEmpty(TopUnmanaged_CSM) && TopUnmanaged_CSM.ToLower() == "yes" && !string.IsNullOrEmpty(PrimaryProduct) && PrimaryProduct.ToLower().Contains("azure") && !DisqualifyReasons.Contains(new OptionSetValue(100000034)))
                    {
                        objLeadEntity["statecode"] = new OptionSetValue(statusDisqualified);
                        objLeadEntity["statuscode"] = new OptionSetValue(Convert.ToInt32(100000034));
                    }
                    //else if (!string.IsNullOrWhiteSpace(CLAS_IsABMAccount) && CLAS_IsABMAccount.ToUpper() == "YES" && !string.IsNullOrWhiteSpace(CampaignType) && CampaignType.ToUpper() != "RENEWALS" && !DisqualifyReasons.Contains(new OptionSetValue(100000040)))
                    //{
                    //    objLeadEntity["statecode"] = new OptionSetValue(statusDisqualified);
                    //    objLeadEntity["statuscode"] = new OptionSetValue(Convert.ToInt32(100000040));
                    //}
                    else if (!string.IsNullOrWhiteSpace(CLAS_Nro) && CLAS_Nro.ToUpper() == "YES" && !DisqualifyReasons.Contains(new OptionSetValue(100000023)))
                    {
                        objLeadEntity["statecode"] = new OptionSetValue(statusDisqualified);
                        objLeadEntity["statuscode"] = new OptionSetValue(Convert.ToInt32(100000023));
                    }
                    else if ((CampaignName.ToUpper() != "EA RENEWALS" || IsAutoLead.ToUpper() == "FALSE") && !string.IsNullOrWhiteSpace(CLAS_TPNameWaste) && CLAS_TPNameWaste.ToUpper() == "YES" && !DisqualifyReasons.Contains(new OptionSetValue(100000024)))
                    {
                        objLeadEntity["statecode"] = new OptionSetValue(statusDisqualified);
                        objLeadEntity["statuscode"] = new OptionSetValue(Convert.ToInt32(100000024));
                    }
                    else if ((OpenLeadsOtherVendor > 0 || ActiveOpportunitiesOtherVendor > 0) && !DisqualifyReasons.Contains(new OptionSetValue(100000042)))
                    {
                        objLeadEntity["statecode"] = new OptionSetValue(statusDisqualified);
                        objLeadEntity["statuscode"] = new OptionSetValue(Convert.ToInt32(100000042));
                    }
                    else if (finalDsq != null && finalDsq.Count >= 1 && !DisqualifyReasons.Contains(new OptionSetValue(100000036)))
                    {
                        objLeadEntity["statecode"] = new OptionSetValue(statusDisqualified);
                        objLeadEntity["statuscode"] = new OptionSetValue(Convert.ToInt32(100000036));
                        objLeadEntity["new_customdisqualificationreasondetails"] = DsqReasonDetails;
                    }

                    if ((!string.IsNullOrWhiteSpace(MSXAccountID)))
                    {
                        hasValidPhoneOrEmail = true;
                    }
                    else
                    {
                        if (!string.IsNullOrWhiteSpace(Email))
                        {
                            hasEmail = true;
                            if (Email.Contains("@"))
                            {
                                string[] fullEmail = Email.Split('@');
                                string emailAlias = (fullEmail != null) ? fullEmail[0] : string.Empty;

                                if (fullEmail != null && fullEmail[1].Contains('.'))
                                {
                                    string[] fulldomain = fullEmail[1].Split('.');
                                    string domain = fulldomain[0];
                                    string domainEnd = fulldomain[1];

                                    if (emailAlias.Length >= 2 && domain.Length >= 2 && domainEnd.Length >= 2)
                                    {
                                        hasValidEmail = true;
                                        if (hasValidEmail && !Email.Contains("@microsoft.com") && !Email.Contains("@test.com"))
                                        {
                                            hasAllowedEmail = true;
                                        }
                                    }
                                }
                            }
                        }

                        int charCount = 0;
                        int nonZeroCount = 0;
                        int listOfCharCount = 0;
                        List<char> listOfChar = new List<char>();
                        listOfChar.Add('(');
                        listOfChar.Add(')');
                        listOfChar.Add('+');
                        listOfChar.Add('-');
                        listOfChar.Add('.');
                        listOfChar.Add('/');
                        listOfChar.Add('x');
                        listOfChar.Add('0');
                        listOfChar.Add('1');
                        listOfChar.Add('2');
                        listOfChar.Add('3');
                        listOfChar.Add('4');
                        listOfChar.Add('5');
                        listOfChar.Add('6');
                        listOfChar.Add('7');
                        listOfChar.Add('8');
                        listOfChar.Add('9');

                        if (!string.IsNullOrWhiteSpace(Phone))
                        {
                            Phone = Phone.ToLower();
                            Phone = Phone.Replace(" ", string.Empty);
                            Phone = Phone.Replace("ext", string.Empty);

                            hasPhone = true;
                            isPhoneZeroes = Regex.Match(Phone, @"^0+$").Success;
                            foreach (char a in Phone)
                            {
                                charCount++;
                                if (Phone.Length >= 5 && !isPhoneZeroes)
                                {
                                    if (a != '0')
                                    {
                                        nonZeroCount++;
                                    }
                                    if (listOfChar.Contains(a))
                                    {
                                        listOfCharCount++;
                                    }

                                    if (Phone.Length == charCount && charCount == listOfCharCount)
                                    {
                                        if (nonZeroCount >= 3)
                                        {
                                            hasValidPhone = true;
                                        }
                                    }
                                }
                            }
                        }

                        if ((hasEmail && hasValidEmail && hasAllowedEmail) || (hasPhone && hasValidPhone))
                        {
                            hasValidPhoneOrEmail = true;
                        }
                    }

                    if (!string.IsNullOrWhiteSpace(CampaignCode) && !defaultCampaign.Equals(CampaignCode))
                    {
                        hasValidCampaignCode = true;
                    }

                    if (OpenLeadsSameVendor > 0 || OpenLeadsOtherVendor > 0 || ActiveOpportunitiesSameVendor > 0 || ActiveOpportunitiesOtherVendor > 0 || AgingDSQLeadsSameVendor > 0 || AgingDSQLeadsOtherVendor > 0 || ExpiredClosedOppSameVendor > 0 || ExpiredClosedOppOtherVendor > 0)
                    {
                        ExistingMSX = true;
                    }
                    string valueLead = "Lead";
                    string valueLeads = "Leads";
                    string valueOpp = "Opportunity";
                    string valueOpps = "Opportunities";
                    string Lead = string.Empty;
                    string Opp = string.Empty;
                    if (!string.IsNullOrWhiteSpace(MSXLeadOwnerAlias) || !string.IsNullOrWhiteSpace(DefaultTeamSubsidiaryOwner) || !string.IsNullOrWhiteSpace(DefaultTeamOwner))
                    {
                        hasValidMSXAlias = true;
                    }
                    if (!hasValidPhoneOrEmail || !hasValidCampaignCode || !hasValidMSXAlias || ExistingMSX || !string.IsNullOrWhiteSpace(duplicateagreementdetails_Warning?.ToString()))
                    {
                        if (!hasValidMSXAlias)
                        {
                            DataWarnings = DataWarnings.AppendLine("- MSX Lead Owner Alias is required");
                        }
                        if (!string.IsNullOrWhiteSpace(duplicateagreementdetails_Warning?.ToString()))
                            DataWarnings = DataWarnings.AppendLine(duplicateagreementdetails_Warning.ToString());

                        if (OpenLeadsSameVendor > 0 || ActiveOpportunitiesSameVendor > 0)
                        {
                            if (OpenLeadsSameVendor == 1)
                            {
                                Lead = valueLead;
                            }
                            else
                            {
                                Lead = valueLeads;
                            }
                            if (ActiveOpportunitiesSameVendor == 1)
                            {
                                Opp = valueOpp;
                            }
                            else
                            {
                                Opp = valueOpps;
                            }
                            DataWarnings = DataWarnings.AppendLine("- " + Vendor + " already has active leads and / or opportunities in MSX for this customer (" + OpenLeadsSameVendor.ToString() + " Open " + Lead + ", " + ActiveOpportunitiesSameVendor.ToString() + " Active " + Opp + ")");
                        }
                        if (AgingDSQLeadsSameVendor > 0 || ExpiredClosedOppSameVendor > 0)
                        {
                            if (AgingDSQLeadsSameVendor == 1)
                            {
                                Lead = valueLead;
                            }
                            else
                            {
                                Lead = valueLeads;
                            }
                            if (ExpiredClosedOppSameVendor == 1)
                            {
                                Opp = valueOpp;
                            }
                            else
                            {
                                Opp = valueOpps;
                            }
                            DataWarnings = DataWarnings.AppendLine("- " + Vendor + " has some prior activity in MSX for this customer (" + AgingDSQLeadsSameVendor.ToString() + " Aging / Disqualified " + Lead + ", " + ExpiredClosedOppSameVendor.ToString() + " Expired / Closed / Invalid " + Opp + ")");
                        }
                        if (OpenLeadsOtherVendor > 0 || ActiveOpportunitiesOtherVendor > 0)
                        {
                            if (OpenLeadsOtherVendor == 1)
                            {
                                Lead = valueLead;
                            }
                            else
                            {
                                Lead = valueLeads;
                            }
                            if (ActiveOpportunitiesOtherVendor == 1)
                            {
                                Opp = valueOpp;
                            }
                            else
                            {
                                Opp = valueOpps;
                            }
                            DataWarnings = DataWarnings.AppendLine("- Other teams / vendors already have active leads and / or opportunities in MSX for this customer (" + OpenLeadsOtherVendor.ToString() + " Open " + Lead + ", " + ActiveOpportunitiesOtherVendor.ToString() + " Active " + Opp + ")");
                        }
                        if (AgingDSQLeadsOtherVendor > 0 || ExpiredClosedOppOtherVendor > 0)
                        {
                            if (AgingDSQLeadsOtherVendor == 1)
                            {
                                Lead = valueLead;
                            }
                            else
                            {
                                Lead = valueLeads;
                            }
                            if (ExpiredClosedOppOtherVendor == 1)
                            {
                                Opp = valueOpp;
                            }
                            else
                            {
                                Opp = valueOpps;
                            }
                            DataWarnings = DataWarnings.AppendLine("- Other teams / vendors have some prior activity in MSX for this customer (" + AgingDSQLeadsOtherVendor.ToString() + " Aging / Disqualified " + Lead + ", " + ExpiredClosedOppOtherVendor.ToString() + " Expired / Closed / Invalid " + Opp + ")");
                        }
                        if (!hasValidPhoneOrEmail)
                        {
                            if (!hasPhone && !hasEmail)
                            {
                                DataWarnings = DataWarnings.AppendLine("- If MSX Account Number is not known, then one of either EmailAddress or PhoneNumber is recommended");
                            }
                            else if (hasEmail && (!hasAllowedEmail || !hasValidEmail))
                            {
                                DataWarnings = DataWarnings.AppendLine("- Email address is not valid");
                            }
                            else if (hasPhone && !hasValidPhone)
                            {
                                DataWarnings = DataWarnings.AppendLine("- Phone number is not valid");
                            }
                        }
                        if (!hasValidCampaignCode)
                        {
                            DataWarnings = DataWarnings.AppendLine("- Invalid Allocadia ID provided in CampaignCode field");
                        }
                        if (!objLeadEntity.Contains("statuscode") && !DisqualifyReasons.Contains(new OptionSetValue(100000041)))
                        {
                            if (IsLeadValidationExpired(ref objLeadEntity, ValidationCompletedTime) == false)
                            {
                                objLeadEntity["statecode"] = new OptionSetValue(Convert.ToInt32(0));
                                objLeadEntity["statuscode"] = new OptionSetValue(Convert.ToInt32(100000041));
                                objLeadEntity["new_msxuploaddatawarnings"] = DataWarnings.ToString();
                            }
                        }

                    }
                    if (!objLeadEntity.Contains("statuscode"))
                    {
                        if (IsLeadValidationExpired(ref objLeadEntity, ValidationCompletedTime) == false)
                        {
                            objLeadEntity["statecode"] = new OptionSetValue(Convert.ToInt32(0));
                            objLeadEntity["statuscode"] = new OptionSetValue(Convert.ToInt32(100000009));
                            if (PreApproveForMSXUpload != null && PreApproveForMSXUpload.Value == Convert.ToInt32(100000000))
                            {
                                objLeadEntity["new_approveleadformsxupload"] = true;
                            }
                        }
                    }
                    if (PreApproveForMSXUpload == null || (PreApproveForMSXUpload != null && PreApproveForMSXUpload.Value != Convert.ToInt32(100000000)))
                    {
                        objLeadEntity["new_approveleadformsxupload"] = false;
                    }
                    if (StatusReason == "Revalidation In Progress")
                    {
                        objLeadEntity["new_overridedatetime"] = DateTime.UtcNow;
                    }
                    else
                    {
                        if (string.IsNullOrEmpty(ValidationCompletedTime))
                        {
                            objLeadEntity["new_validationcompletedtime"] = DateTime.UtcNow;
                        }
                    }

                    objLeadEntity["new_leadupdatedby"] = "Update status Job";
                    objLeadEntity["new_pendingvalidationstepsortorder"] = new OptionSetValue(Convert.ToInt32(100000009));
                    var updateRequest = new UpdateRequest();
                    updateRequest.Target = objLeadEntity;
                    updateLeadsRequest.Requests.Add(updateRequest);

                    if (updateLeadsRequest.Requests.Count == 1000)
                    {
                        _log.LogInformation(batch.ToString());
                        _manager.ExecuteBulkRequest(updateLeadsRequest, "Updated Status Details");
                        updateLeadsRequest.Requests.Clear();
                        batch = 0;
                    }
                }
                if (batch > 0)
                {
                    _manager.ExecuteBulkRequest(updateLeadsRequest, "Updated Status Details");
                    updateLeadsRequest.Requests.Clear();
                    batch = 0;
                }

            }
            DateTime EndTime = DateTime.UtcNow;
            SqlConnection con = new SqlConnection(LeadValidationFunctions._sqlConnection);
            //AuthenticationResult authenticationResult = _manager.AADAunthenticationResult();
            con.AccessToken = Manager.getAccessToken(System.Environment.GetEnvironmentVariable("ResourceId"), System.Environment.GetEnvironmentVariable("VTCPManagedIdentity"), true).Result;
            SqlCommand SqlCommands = new SqlCommand();
            SqlCommands = new SqlCommand("AzureFunctionsDataLoad", con);
            SqlCommands.CommandType = CommandType.StoredProcedure;
            SqlCommands.Parameters.AddWithValue("@FunctionName", "UpdateLeadStage");
            SqlCommands.Parameters.AddWithValue("@ProcessStartTime", StartTime.ToString());
            SqlCommands.Parameters.AddWithValue("@FunctionStartTime", StartTime.ToString());
            SqlCommands.Parameters.AddWithValue("@FunctionEndTime", EndTime.ToString());
            SqlCommands.Parameters.AddWithValue("@InitialRowsAffected", leadscoll.Entities.Count.ToString());
            SqlCommands.Parameters.AddWithValue("@TotalRowsAffected", TotalProcessedRecords.ToString());
            con.Open();
            SqlCommands.ExecuteNonQuery();
            con.Close();

        }
        bool IsLeadValidationExpired(DateTime ValidationCompletedTime_t2)
        {
            bool expired = true;
            DateTime pastDate_t1 = DateTime.UtcNow.AddDays(-30).Date;

            int result = DateTime.Compare(pastDate_t1, ValidationCompletedTime_t2);
            if (result < 0)//Less than zero = t1 is earlier than t2
                expired = false;

            return expired;
        }

        bool IsLeadValidationExpired(ref Entity objLeadEntity, string ValidationCompletedTime)
        {
            bool expired = false;
            if (string.IsNullOrEmpty(ValidationCompletedTime))
            {
                objLeadEntity["new_validationcompletedtime"] = DateTime.UtcNow;
            }
            else
            {
                if (IsLeadValidationExpired(Convert.ToDateTime(ValidationCompletedTime)))
                {
                    objLeadEntity["statecode"] = new OptionSetValue(Convert.ToInt32(0));//Open
                    objLeadEntity["statuscode"] = new OptionSetValue(Convert.ToInt32(100000044));//Validation Expired
                    expired = true;
                }
            }
            return expired;
        }

        public class DsqLeads
        {
            public string DsqArea;
            public string DsqRegion;

            public string DsqSalesmotion;
            public string DsqVendor;
            public string DsqSubsidiary;
            public string DsqReason;
            public string DsqColumn;
            public string DsqOperator;
            public string DsqOverride;
            public string DsqValue;
            public string DsqId;
        }

        #endregion

        #region Upload Leads to MSX

        /// <summary>
        /// Method to Post leads to MSX 
        /// </summary>
        /// <param name="body"></param>
        /// <returns></returns>
        public Tuple<bool, string, string> PostToAlertAPI(JObject body, CsLeads lead)
        {
            bool MSXLeadStatus = false;
            string responseMessage = string.Empty;
            string failureMessage = string.Empty;
            try
            {
                string accessToken = Manager.getAccessToken(Convert.ToString(System.Environment.GetEnvironmentVariable("MsxResource")), _vtcpManagedIdentity).Result;
                HttpClient httpClient = new HttpClient();
                httpClient.BaseAddress = new System.Uri(Convert.ToString(System.Environment.GetEnvironmentVariable("MsxApiBaseUrl")));
                httpClient.DefaultRequestHeaders.Accept.Add(new System.Net.Http.Headers.MediaTypeWithQualityHeaderValue("application/json"));
                httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
                httpClient.DefaultRequestHeaders.Add("Ocp-Apim-Subscription-Key", Convert.ToString(System.Environment.GetEnvironmentVariable("MsxSubscriptionKey")));
                HttpContent content = new StringContent(body.ToString(), UTF8Encoding.UTF8, "application/json");
                HttpResponseMessage message = httpClient.PostAsync(Convert.ToString(System.Environment.GetEnvironmentVariable("MsxApiUrl")), content).Result;
                _log.LogInformation(message?.StatusCode.ToString());
                string result = message.Content.ReadAsStringAsync().Result;
                if (!string.IsNullOrEmpty(result))
                {
                    _log.LogInformation(result.ToString());
                    if (message?.StatusCode.ToString() == "OK")
                    {
                        JObject res = JObject.Parse(result);
                        string success = res.SelectToken("success").ToString();
                        responseMessage = res.SelectToken("Message").ToString();
                        string correlationId = res.SelectToken("CorrelationID").ToString();
                        if (success.ToLower() == "true")
                        {
                            MSXLeadStatus = true;
                        }
                    }
                    else
                    {

                        failureMessage = message?.StatusCode.ToString();
                    }
                }
            }
            catch (Exception ex)
            {
                _log.LogError(ex.Message);
                //throw ex;
            }
            return new Tuple<bool, string, string>(MSXLeadStatus, responseMessage, failureMessage);
        }

        /// <summary>
        /// Method to Update Payload with the GIAPS Lead
        /// </summary>
        /// <param name="payload"></param>
        /// <param name="finalQuery"></param>
        /// <param name="lead"></param>
        public Tuple<bool, String, string> UpdatePayLoadWithQueryResult(JObject parsedData, CsLeads lead)
        {
            StringBuilder need = new StringBuilder();
            StringBuilder notes = new StringBuilder();

            //Parse JSON Payload
            var rawPayload = parsedData.SelectToken("Input");
            var messageHeader = rawPayload.SelectToken("MessageHeader");
            var messageBody = rawPayload.SelectToken("MessageBody");

            //Update Message Header
            messageHeader["MessageSentTime"] = Convert.ToString(System.DateTime.UtcNow);
            messageHeader["CorrelationID"] = lead.LeadID;

            //Update Message Body
            messageBody["CountryOrRegion"] = lead.Country;
            messageBody["FirstName"] = lead.Firstname;
            messageBody["LastName"] = lead.Lastname;
            messageBody["EmailAddress"] = lead.EmailAddress;
            messageBody["BusinessPhone"] = lead.BusinessPhone;
            messageBody["JobTitle"] = lead.JobTitle;
            messageBody["LeadSource"] = lead.Leadsource;
            messageBody["LeadSourceSubType"] = lead.LeadsourceSubType;
            messageBody["ServicesPrimaryProduct"] = lead.PrimaryProduct;
            if (!string.IsNullOrWhiteSpace(lead.MSXLeadOwner))
            {
                messageBody["LeadOwner"] = lead.MSXLeadOwner;
            }
            else if (!string.IsNullOrWhiteSpace(lead.DefaultTeamSubsidiaryOwner))
            {
                messageBody["LeadOwner"] = lead.DefaultTeamSubsidiaryOwner;
            }
            else
            {
                messageBody["LeadOwner"] = lead.DefaultTeamOwner;
            }
            messageBody["LeadTitle"] = _manager.LimitCharacterCount(lead.LeadTitle, 200);
            messageBody["OrganizationName"] = lead.ImportedAccountName;
            messageBody["AddressLine1"] = lead.Address;
            messageBody["City"] = lead.City;
            messageBody["StateOrProvince"] = lead.State;
            messageBody["PostalCode"] = lead.PostalCode;
            messageBody["WebSiteURL"] = lead.WebsiteURL;

            decimal expAmount = decimal.MinValue;
            int propensityValue = 0;
            decimal prioritizationScore = decimal.MinValue;
            string appid = string.Empty;
            appid = Environment.GetEnvironmentVariable("AppId", EnvironmentVariableTarget.Process);

            expAmount = Convert.ToDecimal(lead.ExpiringAmount);

            if (lead.CLAS_Propensity.ToLower() == "act now")
            {
                propensityValue = 5;
            }
            else if (lead.CLAS_Propensity.ToLower() == "evaluate")
            {
                propensityValue = 4;
            }
            else if (lead.CLAS_Propensity.ToLower() == "nurture")
            {
                propensityValue = 3;
            }
            else if (lead.CLAS_Propensity.ToLower() == "educate")
            {
                propensityValue = 2;
            }
            else if (lead.CLAS_Propensity.ToLower() == "n/a" || lead.CLAS_Propensity.ToLower() == "unknown" || string.IsNullOrEmpty(lead.CLAS_Propensity))
            {
                propensityValue = 1;
            }
            prioritizationScore = (expAmount / 1000) * propensityValue;

            if (lead.NymeriaRanking != decimal.MinValue)
            {
                messageBody["PrioritizationScore"] = lead.NymeriaRanking;
                prioritizationScore = lead.NymeriaRanking;
            }

            if (messageBody["PrioritizationScore"] == null)
            {
                ((JObject)messageBody).Add("PrioritizationScore", 0);
            }

            if (prioritizationScore != 0)
            {
                messageBody["PrioritizationScore"] = Math.Round(prioritizationScore, 2);
            }
            else
            {
                ((JObject)messageBody).Property("PrioritizationScore").Remove();
            }
            if (!string.IsNullOrWhiteSpace(lead.NymeriaPriority))
            {
                messageBody["LeadRating"] = lead.NymeriaPriority;
            }
            string VLAllocation = string.Empty;
            if (lead.CLAS_VLAllocation.ToUpper() == "YES")
            {
                VLAllocation = "(Potential VL Allocation TPID)";
            }
            if (!string.IsNullOrWhiteSpace(lead.CustomerTPIDList))
            {
                need = need.AppendLine($"Customer TPIDs: {lead.CustomerTPIDList} {VLAllocation}");
            }
            else if (!string.IsNullOrWhiteSpace(lead.MSSalesTPID))
            {
                need = need.AppendLine($"Customer TPID: {lead.MSSalesTPID} {VLAllocation}");
            }
            if (!string.IsNullOrWhiteSpace(lead.CLAS_SmcType))
            {
                need = need.AppendLine($"SMC Type: {lead.CLAS_SmcType}");
            }
            if (!string.IsNullOrWhiteSpace(lead.CLAS_PropensityDetails))
            {
                need = need.AppendLine(lead.CLAS_PropensityDetails.Replace("N/A", "Unknown"));
            }
            if (!string.IsNullOrWhiteSpace(lead.CLAS_ProductOwnershipDetails))
            {
                need = need.AppendLine(lead.CLAS_ProductOwnershipDetails);
            }
            if (!string.IsNullOrWhiteSpace(lead.Need))
            {
                need = need.AppendLine(lead.Need);
            }
            //if (!string.IsNullOrWhiteSpace(lead.CampaignType) && lead.CampaignType == "Renewals" && !string.IsNullOrEmpty(lead.ExpiringProductDetails))
            //{
            //    string needText = "See Notes for details of expiring products per agreement (Note added by VTCP)";
            //    need = need.Append(needText);
            //}

            StringBuilder customerComments = new StringBuilder();
            if (!string.IsNullOrWhiteSpace(lead.CustomerTPIDList))
            {
                customerComments = customerComments.AppendLine($"Customer TPIDs: {lead.CustomerTPIDList} {VLAllocation}");
            }
            else if (!string.IsNullOrWhiteSpace(lead.MSSalesTPID))
            {
                customerComments = customerComments.AppendLine($"Customer TPID: {lead.MSSalesTPID} {VLAllocation}");
            }
            if (!string.IsNullOrWhiteSpace(lead.CLAS_SmcType))
            {
                customerComments = customerComments.AppendLine($"SMC Type: {lead.CLAS_SmcType}");
            }
            if (!string.IsNullOrWhiteSpace(lead.AdditionalContactDetails))
            {
                customerComments = customerComments.AppendLine("Additional Contact Details / Additional Comments: ");
                customerComments = customerComments.AppendLine($"{lead.AdditionalContactDetails}");
            }
            customerComments = customerComments.AppendLine();
            customerComments = customerComments.Append($"Created in VTCP by {lead.VendorOwner} on {lead.Createdon.ToString("yyyy-MM-dd")}. Original VTCP Lead ID {lead.LeadID}");

            messageBody["CustomerComments"] = customerComments.ToString();
            messageBody["Need"] = Convert.ToString(need);

            notes = notes.AppendLine("<div data-wrapper='true'><div>");
            if (lead.CallPrepSheet != null && lead.CallPrepSheet.Value == 100000000)
            {
                notes.AppendLine($"<strong><a href = {lead.CallPrepSheetLink} > View Call Prep Sheet (Powered By AI)</a></strong><br/><br/>");
            }
            notes = notes.AppendLine("<strong>Additional Details From VTCP Validation & Enrichment </strong><br /> " +
            $"<a href='https://{_instancename}.crm.dynamics.com/main.aspx?appid={appid}&pagetype=entityrecord&etn=lead&id={lead.LeadID}'>Original VTCP Record</a> - VTCP validation and enrichment process completed on {lead.ValidationCompletedTime.ToString("yyyy-MM-dd HH:mm:ss UTC")}");

            if (!string.IsNullOrWhiteSpace(lead.CampaignType) && lead.CampaignType == "Renewals" && !string.IsNullOrEmpty(lead.ExpiringProductDetails))
            {
                notes = notes.Append("<br/>");
                notes = notes.AppendLine("<strong>Products on Expiring Agreements: </strong>");
                notes = notes.Append("AgreementID: " + lead.AgreementID);
                if (lead.AgreementExpirationDate != null && lead.AgreementExpirationDate != DateTime.MinValue)
                {
                    notes = notes.AppendLine($" - Expiration Date: {lead.AgreementExpirationDate.Value.ToString("yyyy-MM-dd")}");
                }
                else
                {
                    notes = notes.AppendLine(" - Expiration Date: ");
                }
                notes = notes.AppendLine(lead.ExpiringProductDetails);
                if (lead.IsMergedLead.ToLower() == "true")
                {
                    notes = notes.AppendLine(lead.MergedProductDetails);
                }
            }
            if (lead.CampaignType == "Renewals" && ((!string.IsNullOrEmpty(lead.ResellerName) && lead.ResellerName != "N/A") || (!string.IsNullOrEmpty(lead.DistributorName) && lead.DistributorName != "N/A") || (!string.IsNullOrEmpty(lead.AdvisorName) && lead.AdvisorName != "N/A")))
            {
                notes = notes.AppendLine("<br/><strong>Incumbent Partners on Expiring Agreements: </strong>");
                notes = notes.Append($"AgreementID: {lead.AgreementID}");

                if (lead.AgreementExpirationDate != DateTime.MinValue && lead.AgreementExpirationDate != null)
                {
                    notes = notes.AppendLine($" - Expiration Date: {lead.AgreementExpirationDate.Value.ToString("yyyy-MM-dd")}");
                }
                else
                {
                    notes = notes.AppendLine(" - Expiration Date: ");
                }

                if (!string.IsNullOrWhiteSpace(lead.ResellerName) && lead.ResellerName != "N/A")
                {
                    notes = notes.AppendLine($" - Reseller: {lead.ResellerName}");
                }
                if (!string.IsNullOrWhiteSpace(lead.DistributorName) && lead.DistributorName != "N/A")
                {
                    notes = notes.AppendLine($" - Distributor: {lead.DistributorName}");
                }
                if (!string.IsNullOrWhiteSpace(lead.AdvisorName) && lead.AdvisorName != "N/A")
                {
                    notes = notes.AppendLine($" - Advisor: {lead.AdvisorName}");
                }

                if (lead.IsMergedLead.ToLower() == "true" && !string.IsNullOrEmpty(lead.MergedAgreementDetails))
                {
                    notes = notes.AppendLine(lead.MergedAgreementDetails);
                }
            }


            if (!string.IsNullOrWhiteSpace(lead.MSXLeadOppID))
            {
                string[] MSXIDList = lead.MSXLeadOppID.Split(new string[] { ";" }, StringSplitOptions.None);
                int i = 0;
                if (!string.IsNullOrWhiteSpace(lead.ExistingMSXActivitySameVendor))
                {
                    foreach (var e in lead.ExistingMSXActivitySameVendor.Split(new string[] { "\r\n" }, StringSplitOptions.None))
                    {
                        if (e.Split(new string[] { "- ", " (" }, StringSplitOptions.None).Count() > 1)
                        {
                            if (MSXIDList[i].Contains("Opp"))
                            {
                                lead.ExistingMSXActivitySameVendor = lead.ExistingMSXActivitySameVendor.Replace(e.Split(new string[] { "- ", " (" }, StringSplitOptions.None)[1], $"<a href = 'https://microsoftsales.crm.dynamics.com/main.aspx?appid=fe0c3504-3700-e911-a849-000d3a10b7cc&forceUCI=1&pagetype=entityrecord&etn=opportunity&id={MSXIDList[i].Split('#')[0]}' >{e.Split(new string[] { "- ", " (" }, StringSplitOptions.None)[1]}</a>");
                            }
                            else
                            {
                                lead.ExistingMSXActivitySameVendor = lead.ExistingMSXActivitySameVendor.Replace(e.Split(new string[] { "- ", " (" }, StringSplitOptions.None)[1], $"<a href = 'https://microsoftsales.crm.dynamics.com/main.aspx?appid=fe0c3504-3700-e911-a849-000d3a10b7cc&forceUCI=1&pagetype=entityrecord&etn=lead&id={MSXIDList[i].Split('#')[0]}' >{e.Split(new string[] { "- ", " (" }, StringSplitOptions.None)[1]}</a>");
                            }
                            i++;
                        }
                    }
                    foreach (var e in lead.ExistingMSXActivityOtherTeam.Split(new string[] { "\r\n" }, StringSplitOptions.None))
                    {
                        if (e.Split(new string[] { "- ", " (" }, StringSplitOptions.None).Count() > 1)
                        {
                            if (MSXIDList[i].Contains("Opp"))
                            {
                                lead.ExistingMSXActivityOtherTeam = lead.ExistingMSXActivityOtherTeam.Replace(e.Split(new string[] { "- ", " (" }, StringSplitOptions.None)[1], $"<a href = 'https://microsoftsales.crm.dynamics.com/main.aspx?appid=fe0c3504-3700-e911-a849-000d3a10b7cc&forceUCI=1&pagetype=entityrecord&etn=opportunity&id={MSXIDList[i].Split('#')[0]}' >{e.Split(new string[] { "- ", " (" }, StringSplitOptions.None)[1]}</a>");
                            }
                            else
                            {

                                lead.ExistingMSXActivityOtherTeam = lead.ExistingMSXActivityOtherTeam.Replace(e.Split(new string[] { "- ", " (" }, StringSplitOptions.None)[1], $"<a href = 'https://microsoftsales.crm.dynamics.com/main.aspx?appid=fe0c3504-3700-e911-a849-000d3a10b7cc&forceUCI=1&pagetype=entityrecord&etn=lead&id={MSXIDList[i].Split('#')[0]}' >{e.Split(new string[] { "- ", " (" }, StringSplitOptions.None)[1]}</a>");
                            }
                            i++;
                        }
                    }
                }

            }

            if (!string.IsNullOrEmpty(lead.ExistingMSXActivitySameVendor))
            {
                notes.AppendLine();
                notes.AppendLine($"<strong>Existing MSX Leads and Opportunities from {lead.VendorOwner}:</strong>");
                notes.AppendLine($"{lead.ExistingMSXActivitySameVendor}");
                notes.ToString();
            }
            if (!string.IsNullOrEmpty(lead.ExistingMSXActivityOtherTeam))
            {
                notes.AppendLine();
                notes.AppendLine("<strong>Existing MSX Leads and Opportunities from Other Teams:</strong>");
                notes.AppendLine($"{lead.ExistingMSXActivityOtherTeam}");
                notes.ToString();
            }
            notes.AppendLine("</div></div>");
            notes.Replace("\r\n", "<br/>");
            notes.Replace("\r", "<br/>");
            notes.Replace("\n", "<br/>");
            notes.Replace(" -", "&nbsp;-");
            messageBody["Notes"] = notes.ToString();

            if (messageBody["DueDate"] == null)
            {
                ((JObject)messageBody).Add("DueDate", "");
            }

            string SubscriptionId = string.Empty;
            if (!string.IsNullOrEmpty(lead.MergedAgreementDetails))
            {
                string[] AgreementIDs = lead.MergedAgreementDetails.Split(new[] { "AgreementID:" }, StringSplitOptions.RemoveEmptyEntries);

                SubscriptionId = lead.AgreementID + ",";
                foreach (var AgreementID in AgreementIDs)
                {
                    var ChildAggrements = AgreementID.Split(new[] { "-" }, StringSplitOptions.RemoveEmptyEntries);
                    if (!string.IsNullOrEmpty(ChildAggrements.ToString()) && ChildAggrements[0] != null)
                    {
                        var lenghtofAggrements = SubscriptionId + ChildAggrements[0].ToString().Replace("\r\n", "").Trim();
                        if (lenghtofAggrements.Length <= 100)
                        {
                            SubscriptionId += ChildAggrements[0].ToString().Replace("\r\n", "").Trim() + ",";
                        }
                        else
                        {
                            break;
                        }
                    }
                }
                SubscriptionId = SubscriptionId.TrimEnd(',', ' ');
            }
            else
            {
                SubscriptionId = lead.AgreementID;
            }

            if (messageBody["SubscriptionId"] == null)
            {
                ((JObject)messageBody).Add("SubscriptionId", "");
            }

            messageBody["SubscriptionId"] = SubscriptionId;

            if (!string.IsNullOrWhiteSpace(lead.CampaignType) && lead.CampaignType.ToLower() == "renewals")
            {
                if (lead.IsMergedLead.ToLower() == "true" && lead.MergedMinExpirationDate != null && lead.MergedMinExpirationDate != DateTime.MinValue)
                {
                    messageBody["DueDate"] = lead.MergedMinExpirationDate;
                }
                else if (lead.IsMergedLead.ToLower() != "true" && lead.AgreementExpirationDate != null && lead.AgreementExpirationDate != DateTime.MinValue)
                {
                    messageBody["DueDate"] = lead.AgreementExpirationDate;
                }
                else
                {
                    ((JObject)messageBody).Property("DueDate").Remove();
                }
            }
            else
            {
                ((JObject)messageBody).Property("DueDate").Remove();
            }

            if (messageBody["EstimatedValue"] == null)
            {
                ((JObject)messageBody).Add("EstimatedValue", 0);
            }
            if (lead.CampaignType == "Renewals" && (lead.MergedExpiringAmount != 0 || lead.ExpiringAmount != 0))
            {
                if (lead.IsMergedLead.ToLower() == "true" && lead.MergedExpiringAmount != 0)
                {
                    messageBody["EstimatedValue"] = lead.MergedExpiringAmount;
                }
                else if (lead.ExpiringAmount != 0)
                {
                    messageBody["EstimatedValue"] = lead.ExpiringAmount;
                }
                else
                {
                    ((JObject)messageBody).Property("EstimatedValue").Remove();

                }
            }
            else
            {
                ((JObject)messageBody).Property("EstimatedValue").Remove();
            }

            messageBody["SalesAccountNumber"] = lead.MSXAccountID;
            messageBody["TrackingCode"] = lead.CampaignCode;
            messageBody["SourceSystemLeadID"] = lead.LeadID;
            messageBody["AllocatedTPID"] = lead.MSSalesTPID;
            //messageBody["AdditionalInformationFromSource"] = "Test";

            rawPayload["MessageBody"] = messageBody;
            rawPayload["MessageHeader"] = messageHeader;

            _log.LogInformation(parsedData.ToString());
            Tuple<bool, string, string> objResponse = PostToAlertAPI(parsedData, lead);
            Console.WriteLine("Alert function response is : " + objResponse);

            return objResponse;
        }

        /// <summary>
        /// Method to read JSON payload from Blob
        /// </summary>
        /// <returns></returns>
        public static JObject ReadBlobFiles()
        {
            string blobUrl = _blobConnection;
            string managedIdentityClientId = _vtcpManagedIdentity; 

            //// Create an instance of DefaultAzureCredential with managed identity options
            var credentialOptions = new DefaultAzureCredentialOptions
            {
                ManagedIdentityClientId = managedIdentityClientId
            };
            var credential = new DefaultAzureCredential(credentialOptions);

            var blobServiceClient = new BlobServiceClient(new Uri(blobUrl), credential);
            var containerName = "msxpayload";
            var blobName = "MSXPayload.json";
            var blobClient = blobServiceClient.GetBlobContainerClient(containerName).GetBlobClient(blobName);
            var blobContent =  blobClient.OpenReadAsync().GetAwaiter().GetResult();
            string content; 
            using (var reader = new StreamReader(blobContent))
            {
                 content = reader.ReadToEndAsync().GetAwaiter().GetResult();
             
            }
            JObject payload = JObject.Parse(content);
            return payload;
        }

        /// <summary>
        /// Method to upload leads to MSX
        /// </summary>
        public void LeadUploadToMSX()
        {
            DateTime StartTime = DateTime.UtcNow;
            var payload = ReadBlobFiles();
            Tuple<bool, string, string> msxResponse = new Tuple<bool, string, string>(false, string.Empty, string.Empty);
            _log.LogInformation("Execution started: Upload to MSX");

            int batch = 0;
            ExecuteMultipleSettings excuteMultipleSettings = new ExecuteMultipleSettings();
            excuteMultipleSettings.ContinueOnError = true;
            excuteMultipleSettings.ReturnResponses = true;

            ExecuteMultipleRequest updateLeadsRequest = new ExecuteMultipleRequest();
            updateLeadsRequest.Requests = new OrganizationRequestCollection();
            updateLeadsRequest.Settings = excuteMultipleSettings;

            ExecuteMultipleRequest mergeRequests = new ExecuteMultipleRequest();
            mergeRequests.Requests = new OrganizationRequestCollection();
            mergeRequests.Settings = excuteMultipleSettings;

            //Get leads for MSX Upload
            List<CsLeads> leadList = GetIAPSLeadsForMSXUpload();

            if (leadList != null && leadList.Count > 0)
            {
                foreach (var lead in leadList)
                {
                    batch++;
                    Entity objUpdateLeadEntity = new Entity("lead");
                    objUpdateLeadEntity.Id = lead.LeadID;
                    objUpdateLeadEntity["new_msxuploadtriggertime"] = DateTime.UtcNow; ;
                    objUpdateLeadEntity["statecode"] = new OptionSetValue(Convert.ToInt32(0));
                    objUpdateLeadEntity["statuscode"] = new OptionSetValue(Convert.ToInt32(100000030)); //MSX Upload Triggered
                    objUpdateLeadEntity["new_leadupdatedby"] = "MSX Upload Job Part 1";
                    objUpdateLeadEntity["new_msxuploadstatusdetail"] = "MSX Upload Triggered";
                    var updateRequest = new UpdateRequest();
                    updateRequest.Target = objUpdateLeadEntity;
                    updateLeadsRequest.Requests.Add(updateRequest);
                    if (updateLeadsRequest.Requests.Count == leadList.Count)
                    {
                        _manager.ExecuteBulkRequest(updateLeadsRequest, "Updated MSX Trigger Details");
                        updateLeadsRequest.Requests.Clear();
                        batch = 0;
                    }
                }

                foreach (var lead in leadList)
                {
                    //Wait time of 10 seconds
                    //Thread.Sleep(10000);
                    msxResponse = UpdatePayLoadWithQueryResult((JObject)payload, lead);
                    Entity objLeadEntity = new Entity("lead");
                    objLeadEntity.Id = lead.LeadID;
                    if (msxResponse.Item1)
                    {
                        objLeadEntity["new_msxuploadstarttime"] = DateTime.UtcNow;
                        objLeadEntity["statecode"] = new OptionSetValue(Convert.ToInt32(0));
                        objLeadEntity["statuscode"] = new OptionSetValue(Convert.ToInt32(100000027));
                        objLeadEntity["new_msxuploadstatusdetail"] = "MSX Upload Request In Progress";
                        objLeadEntity["new_leadupdatedby"] = "MSX Upload Job Part 2";
                        _log.LogInformation("Lead Upload succeeded" + ' ' + lead.LeadID);
                    }
                    else
                    {
                        objLeadEntity["statecode"] = new OptionSetValue(Convert.ToInt32(0));
                        objLeadEntity["statuscode"] = new OptionSetValue(Convert.ToInt32(100000025));
                        if (msxResponse != null && !string.IsNullOrWhiteSpace(msxResponse.Item2.ToString()))
                        {
                            objLeadEntity["new_msxuploadstatusdetail"] = msxResponse.Item2.ToString();
                        }
                        else
                        {
                            objLeadEntity["new_msxuploadstatusdetail"] = "MSX API Not Responding - Retry in Progress";
                        }
                        if (msxResponse != null && !string.IsNullOrWhiteSpace(msxResponse.Item3.ToString()))
                        {
                            objLeadEntity["new_msxuploadfailuremessage"] = msxResponse.Item3.ToString();
                        }
                        objLeadEntity["new_leadupdatedby"] = "MSX Upload Job Part 2";
                        objLeadEntity["new_approveleadformsxupload"] = false;
                        _log.LogInformation("Lead Upload Failed" + ' ' + lead.LeadID);
                    }
                    _orgService.Update(objLeadEntity);
                    _log.LogInformation("Lead Updated Successfully");
                }
            }
            DateTime EndTime = DateTime.UtcNow;
            SqlConnection con = new SqlConnection(LeadValidationFunctions._sqlConnection);
            //AuthenticationResult authenticationResult = _manager.AADAunthenticationResult();
            con.AccessToken = Manager.getAccessToken(System.Environment.GetEnvironmentVariable("ResourceId"), System.Environment.GetEnvironmentVariable("VTCPManagedIdentity"), true).Result;
            SqlCommand SqlCommands = new SqlCommand();
            SqlCommands = new SqlCommand("AzureFunctionsDataLoad", con);
            SqlCommands.CommandType = CommandType.StoredProcedure;
            SqlCommands.Parameters.AddWithValue("@FunctionName", "MSXLeadUpload");
            SqlCommands.Parameters.AddWithValue("@ProcessStartTime", StartTime.ToString());
            SqlCommands.Parameters.AddWithValue("@FunctionStartTime", StartTime.ToString());
            SqlCommands.Parameters.AddWithValue("@FunctionEndTime", EndTime.ToString());
            SqlCommands.Parameters.AddWithValue("@InitialRowsAffected", leadList.Count.ToString());
            SqlCommands.Parameters.AddWithValue("@TotalRowsAffected", leadList.Count.ToString());
            con.Open();
            SqlCommands.ExecuteNonQuery();
            con.Close();
        }

        /// <summary>
        /// Query Expression to pull all the leads in Validation stage from VTCP
        /// </summary>
        /// <param name="service"></param>
        /// <returns></returns>
        public EntityCollection GetAllLeadsInVTCP(IOrganizationService service, Guid id)
        {
            // Query using the paging cookie.
            // Define the paging attributes.

            //EntityCollection Object
            EntityCollection results = null;
            EntityCollection finalResults = new EntityCollection();

            // The number of records per page to retrieve.
            int queryCount = 2500;

            // Initialize the page number.
            int pageNumber = 1;

            // Initialize the number of records.
            //int recordCount = 0;

            // Define the condition expression for retrieving records.

            FilterExpression filterStatus = new FilterExpression(LogicalOperator.And);
            filterStatus.AddCondition("statecode", ConditionOperator.Equal, Convert.ToInt32(0));
            filterStatus.AddCondition("new_customerlistname", ConditionOperator.Equal, id);


            // Define the order expression to retrieve the records.
            OrderExpression order = new OrderExpression();
            order.AttributeName = "new_agreementprofilesortorder";
            order.OrderType = OrderType.Ascending;

            OrderExpression orderexpdate = new OrderExpression();
            orderexpdate.AttributeName = "new_agreementexpirationdate";
            orderexpdate.OrderType = OrderType.Ascending;

            OrderExpression orderexpamount = new OrderExpression();
            orderexpamount.AttributeName = "new_expiringamount";
            orderexpamount.OrderType = OrderType.Descending;

            //Link Query to pull only Renewals Leads
            LinkEntity leCampaignType = new LinkEntity()
            {
                LinkFromEntityName = "lead",
                LinkToEntityName = "new_campaigntype",
                LinkFromAttributeName = "new_campaigntype",
                LinkToAttributeName = "new_campaigntypeid",
                JoinOperator = JoinOperator.Inner,
                Columns = new ColumnSet(true),
                LinkCriteria = new FilterExpression(LogicalOperator.And)
                {
                    Conditions = { new ConditionExpression("new_name", ConditionOperator.Equal, "Renewals") }
                }
            };

            // Create the query expression and add condition.
            QueryExpression pagequery = new QueryExpression();
            pagequery.EntityName = "lead";
            pagequery.LinkEntities.Add(leCampaignType);
            pagequery.Criteria.AddFilter(filterStatus);
            pagequery.Orders.Add(order);
            pagequery.Orders.Add(orderexpdate);
            pagequery.Orders.Add(orderexpamount);
            pagequery.ColumnSet = new ColumnSet("leadid", "createdon", "new_msxaccountid", "new_agreementid", "new_agreementexpirationdate", "firstname", "lastname", "jobtitle", "mobilephone", "new_mssalestpid", "new_customerlistname", "emailaddress1", "new_resellername", "new_distributorname", "new_advisorname", "new_licensingprogram", "new_expiringamount", "new_expiringproductdetails", "new_isautocreatedlead", "new_mergemultipleleadsforsamecustomer", "statuscode", "subject", "new_isblacklisted", "new_vtcpduplicatedetectioncomplete", "new_validationcompletedtime");

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
                results = service.RetrieveMultiple(pagequery);
                if (results.Entities != null)
                {
                    // Retrieve all records from the result set.
                    foreach (var acct in results.Entities)
                    {
                        finalResults.Entities.Add(acct);
                        //_log.LogInformation(Convert.ToString(++recordCount));
                    }
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
            }
            return finalResults;
        }


        /// <summary>
        /// GetIAPSLeads records For MSX Upload
        /// </summary>
        public List<CsLeads> GetIAPSLeadsForMSXUpload()
        {
            _log.LogInformation("Execution started: GetIAPSLeadsForMSXUpload");
            List<CsLeads> Leads = new List<CsLeads>();
            EntityCollection leadscoll = new EntityCollection();
            EntityCollection leadscollFailed = new EntityCollection();
            int statusReason = Convert.ToInt32(100000026);
            int status = Convert.ToInt32(0);
            int statusReasonFailed = Convert.ToInt32(100000025);
            int topCount = Convert.ToInt32(System.Environment.GetEnvironmentVariable("MSXLeadCount"));

            QueryExpression leadattributes = new QueryExpression()
            {
                EntityName = "lead",
                ColumnSet = new ColumnSet(true),
                Criteria = new FilterExpression(),
                TopCount = topCount
            };
            leadattributes.Criteria.AddCondition("statuscode", ConditionOperator.Equal, statusReason);
            //leadattributes.Criteria.AddCondition("leadid", ConditionOperator.Equal, "af875818-1724-eb11-a813-000d3a5accc0");
            leadattributes.AddOrder("prioritycode", OrderType.Descending);
            leadattributes.AddOrder("new_msxuploadqueuedtime", OrderType.Ascending);
            leadscoll = _orgService.RetrieveMultiple(leadattributes);
            //if (leadscoll != null && leadscoll.Entities.Count < topCount)
            //{
            //    QueryExpression leadattributesFailed = new QueryExpression():[-
            //    {
            //        EntityName = "lead",
            //        ColumnSet = new ColumnSet(true),
            //        Criteria = new FilterExpression(),
            //        TopCount = topCount - leadscoll.Entities.Count
            //    };
            //    FilterExpression FilterOr = new FilterExpression(LogicalOperator.Or);
            //    FilterOr.AddCondition("new_msxuploadretryattempts", ConditionOperator.LessEqual, 5);
            //    FilterOr.AddCondition("new_msxuploadretryattempts", ConditionOperator.Null);
            //    leadattributesFailed.Criteria.AddCondition("statuscode", ConditionOperator.Equal, statusReasonFailed);
            //    leadattributesFailed.Criteria.AddCondition("statecode", ConditionOperator.Equal, status);
            //    leadattributesFailed.Criteria.AddCondition("new_msxuploadstatusdetail", ConditionOperator.Like, "%Retry in Progress%");
            //    leadattributesFailed.Criteria.AddCondition("modifiedon", ConditionOperator.OlderThanXHours, 2);
            //    leadattributesFailed.Criteria.AddCondition("new_importedaccount", ConditionOperator.NotNull);
            //    leadattributesFailed.Criteria.AddCondition("new_is_msx_contacted", ConditionOperator.NotNull);
            //    leadattributesFailed.Criteria.AddCondition("new_accountmatchcomplete", ConditionOperator.Equal, isYes);
            //    leadattributesFailed.Criteria.AddCondition("new_ismandatory", ConditionOperator.Equal, isYes);
            //    leadattributesFailed.Criteria.AddCondition("new_subsidiary", ConditionOperator.NotNull);
            //    leadattributesFailed.Criteria.AddCondition("new_is_lir_contacted", ConditionOperator.NotNull);
            //    leadattributesFailed.Criteria.AddCondition("new_isclascontacted", ConditionOperator.NotNull);
            //    leadattributesFailed.Criteria.AddFilter(FilterOr);
            //    leadscollFailed = _orgService.RetrieveMultiple(leadattributesFailed);
            //}
            if (leadscollFailed != null && leadscollFailed.Entities.Count > 0)
            {
                foreach (var lead in leadscollFailed.Entities)
                {
                    Entity leadEntity = new Entity("lead");
                    leadEntity.Id = lead.Id;
                    int? msxAttempts = lead.GetAttributeValue<int?>("new_msxuploadretryattempts");
                    if (msxAttempts != null && msxAttempts == 5)
                    {
                        leadEntity["new_approveleadformsxupload"] = false;
                        leadEntity["new_msxuploadstatusdetail"] = "MSX Upload Failed – Please review the details of this lead and try again later";
                    }
                    else
                    {
                        leadscoll.Entities.Add(lead);
                        if (msxAttempts == null)
                        {
                            msxAttempts = 0;
                        }
                        leadEntity["new_msxuploadretryattempts"] = msxAttempts + 1;
                    }
                    _orgService.Update(leadEntity);
                }
            }
            if (leadscoll != null && leadscoll.Entities.Count > 0)
            {
                foreach (Entity entityObject in leadscoll.Entities)
                {
                    Leads.Add(new CsLeads
                    {
                        LeadID = entityObject.Contains("leadid") ? new System.Guid(entityObject["leadid"].ToString()) : Guid.Empty,
                        Subsidiary = entityObject.Contains("new_subsidiary") ? entityObject.GetAttributeValue<EntityReference>("new_subsidiary").Name : string.Empty,
                        Country = entityObject.Contains("new_country") ? entityObject.GetAttributeValue<EntityReference>("new_country").Name : string.Empty,
                        Firstname = entityObject.Contains("firstname") ? entityObject.GetAttributeValue<string>("firstname") : string.Empty,
                        Lastname = entityObject.Contains("lastname") ? entityObject.GetAttributeValue<string>("lastname") : string.Empty,
                        EmailAddress = entityObject.Contains("emailaddress1") ? entityObject.GetAttributeValue<string>("emailaddress1") : string.Empty,
                        BusinessPhone = entityObject.Contains("mobilephone") ? entityObject.GetAttributeValue<string>("mobilephone") : string.Empty,
                        JobTitle = entityObject.Contains("jobtitle") ? entityObject.GetAttributeValue<string>("jobtitle") : string.Empty,
                        Createdby = entityObject.Contains("createdby") ? entityObject.GetAttributeValue<EntityReference>("createdby").Name : string.Empty,
                        Createdon = entityObject.Contains("createdon") ? entityObject.GetAttributeValue<DateTime>("createdon") : DateTime.MinValue,
                        LeadTitle = entityObject.Contains("subject") ? entityObject.GetAttributeValue<string>("subject") : string.Empty,
                        ImportedAccountName = entityObject.Contains("new_importedaccount") ? entityObject["new_importedaccount"].ToString() : string.Empty,
                        Address = entityObject.Contains("address1_line1") ? entityObject["address1_line1"].ToString() : string.Empty,
                        City = entityObject.Contains("address1_city") ? entityObject["address1_city"].ToString() : string.Empty,
                        State = entityObject.Contains("address1_stateorprovince") ? entityObject["address1_stateorprovince"].ToString() : string.Empty,
                        PostalCode = entityObject.Contains("address1_postalcode") ? entityObject["address1_postalcode"].ToString() : string.Empty,
                        WebsiteURL = entityObject.Contains("websiteurl") ? entityObject["websiteurl"].ToString() : string.Empty,
                        MSXAccountID = entityObject.Contains("new_matchedmsxaccountid") ? entityObject["new_matchedmsxaccountid"].ToString() : string.Empty,
                        CampaignCode = entityObject.Contains("new_campaigncode") ? entityObject["new_campaigncode"].ToString() : string.Empty,
                        MSSalesTPID = entityObject.Contains("new_mssalestpid") ? entityObject["new_mssalestpid"].ToString() : string.Empty,
                        AgreementID = entityObject.Contains("new_agreementid") ? entityObject["new_agreementid"].ToString() : string.Empty,
                        AgreementExpirationDate = entityObject.Contains("new_agreementexpirationdate") ? Convert.ToDateTime(entityObject["new_agreementexpirationdate"]) : DateTime.MinValue,
                        MSXLeadOwner = entityObject.Contains("new_msxleadowneralias") ? entityObject["new_msxleadowneralias"].ToString() : string.Empty,
                        DefaultTeamOwner = entityObject.Contains("new_defaultteamowner") ? entityObject["new_defaultteamowner"].ToString() : string.Empty,
                        DefaultTeamSubsidiaryOwner = entityObject.Contains("new_defaultteamsubsidiaryown") ? entityObject.GetAttributeValue<string>("new_defaultteamsubsidiaryown") : string.Empty,
                        CustomerList = entityObject.Contains("new_customerlistname") ? entityObject.GetAttributeValue<EntityReference>("new_customerlistname").Id : Guid.Empty,
                        CampaignType = entityObject.Contains("new_campaigntype") ? entityObject.GetAttributeValue<EntityReference>("new_campaigntype").Name : string.Empty,
                        IsAutoCreatedLead = entityObject.Contains("new_isautocreatedlead") ? entityObject.GetAttributeValue<bool>("new_isautocreatedlead").ToString() : string.Empty,
                        ResellerName = entityObject.Contains("new_resellername") ? entityObject["new_resellername"].ToString() : string.Empty,
                        DistributorName = entityObject.Contains("new_distributorname") ? entityObject["new_distributorname"].ToString() : string.Empty,
                        AdvisorName = entityObject.Contains("new_advisorname") ? entityObject["new_advisorname"].ToString() : string.Empty,
                        ExpiringProductDetails = entityObject.Contains("new_expiringproductdetails") ? entityObject["new_expiringproductdetails"].ToString() : string.Empty,
                        MergedAgreementDetails = entityObject.Contains("new_mergedagreementdetails") ? entityObject["new_mergedagreementdetails"].ToString() : string.Empty,
                        MergedProductDetails = entityObject.Contains("new_mergedproductdetails") ? entityObject["new_mergedproductdetails"].ToString() : string.Empty,
                        IsMergedLead = entityObject.Contains("new_ismergedlead") ? entityObject.GetAttributeValue<bool>("new_ismergedlead").ToString() : string.Empty,
                        MergedExpiringAmount = entityObject.Contains("new_mergedexpiringamount") ? entityObject.GetAttributeValue<decimal>("new_mergedexpiringamount") : 0,
                        ExpiringAmount = entityObject.Contains("new_expiringamount") ? entityObject.GetAttributeValue<decimal>("new_expiringamount") : 0,
                        MergedMinExpirationDate = entityObject.Contains("new_mergedminexpirationdate") ? entityObject.GetAttributeValue<DateTime>("new_mergedminexpirationdate") : DateTime.MinValue,
                        MergeMultipleLeadsForSameCustomer = entityObject.Contains("new_mergemultipleleadsforsamecustomer") ? (OptionSetValue)entityObject.Attributes["new_mergemultipleleadsforsamecustomer"] : null,
                        CustomerTPIDList = entityObject.Contains("new_tpidlist") ? entityObject["new_tpidlist"].ToString() : string.Empty,
                        CLAS_SmcType = entityObject.Contains("new_classmctype") ? entityObject.GetAttributeValue<string>("new_classmctype") : string.Empty,
                        CLAS_PropensityDetails = entityObject.Contains("new_claspropensitydetails") ? entityObject.GetAttributeValue<string>("new_claspropensitydetails") : string.Empty,
                        CLAS_ProductOwnershipDetails = entityObject.Contains("new_clasproductownershipdetails") ? entityObject.GetAttributeValue<string>("new_clasproductownershipdetails") : string.Empty,
                        Need = entityObject.Contains("new_need") ? entityObject.GetAttributeValue<string>("new_need") : string.Empty,
                        VendorOwner = entityObject.GetAttributeValue<EntityReference>("ownerid").Name,
                        CLAS_Propensity = entityObject.Contains("new_claspropensityhighlevel") ? entityObject.GetAttributeValue<string>("new_claspropensityhighlevel") : string.Empty,
                        ExistingMSXActivitySameVendor = entityObject.Contains("new_existingmsxactivitydetailssamevendor") ? entityObject.GetAttributeValue<string>("new_existingmsxactivitydetailssamevendor").ToString() : string.Empty,
                        ExistingMSXActivityOtherTeam = entityObject.Contains("new_existingmsxactivitydetailsotherteam") ? entityObject.GetAttributeValue<string>("new_existingmsxactivitydetailsotherteam").ToString() : string.Empty,
                        MSXLeadOppID = entityObject.Contains("new_msxleadandopportunityguids") ? entityObject.GetAttributeValue<string>("new_msxleadandopportunityguids").ToString() : string.Empty,
                        CLAS_VLAllocation = entityObject.Contains("new_clas_vlallocation") ? entityObject.GetAttributeValue<string>("new_clas_vlallocation") : string.Empty,
                        AdditionalContactDetails = entityObject.Contains("new_additionalcontacts") ? entityObject.GetAttributeValue<string>("new_additionalcontacts").ToString() : string.Empty,
                        ValidationCompletedTime = entityObject.Contains("new_validationcompletedtime") ? entityObject.GetAttributeValue<DateTime>("new_validationcompletedtime") : DateTime.MinValue,
                        Leadsource = entityObject.Contains("leadsourcecode") ? entityObject.FormattedValues["leadsourcecode"] : null,
                        LeadsourceSubType = entityObject.Contains("new_leadsubsource") ? entityObject["new_leadsubsource"].ToString() : string.Empty,
                        PrimaryProduct = entityObject.Contains("new_primaryproductcampaign") ? entityObject.FormattedValues["new_primaryproductcampaign"].ToString() : string.Empty,
                        NymeriaRanking = entityObject.Contains("new_nymeriaranking") ? entityObject.GetAttributeValue<decimal>("new_nymeriaranking") : decimal.MinValue,
                        CLAS_VT_Priority = entityObject.Contains("new_clas_vt_priority") ? entityObject.GetAttributeValue<string>("new_clas_vt_priority").ToString() : string.Empty,
                        NymeriaPriority = entityObject.Contains("new_nymeriapriority") ?
                          entityObject.GetAttributeValue<string>("new_nymeriapriority").ToString() :
                          string.Empty,
                        CallPrepSheet = entityObject.Contains("new_callprepsheetavailable") ?
                          entityObject.GetAttributeValue<OptionSetValue>("new_callprepsheetavailable") :
                         null,
                        CallPrepSheetLink = entityObject.Contains("new_callprepsheetlink") ?
                          entityObject.GetAttributeValue<string>("new_callprepsheetlink").ToString() :
                          string.Empty
                    }); ;
                }
            }
            return Leads;
        }

        #endregion

        #region MSX Acknowlegement

        /// <summary>
        /// Method to update MSX Lead in VTCP
        /// </summary>
        /// <param name="VTCPLeadId"></param>
        /// <param name="StatusCode"></param>
        /// <param name="MSXLeadId"></param>
        public void UpdateMSXLeadIDToVTCP(string VTCPLeadId, bool StatusCode, string MSXLeadId, string FailureType, string ErrorMessage)
        {
            //Wait time of 10 seconds
            Thread.Sleep(2000);
            if (!string.IsNullOrWhiteSpace(VTCPLeadId) && StatusCode)
            {
                Entity objLeadEntity = new Entity("lead");
                objLeadEntity.Id = new Guid(VTCPLeadId);
                objLeadEntity["new_uploadedmsxleadnumber"] = MSXLeadId;
                objLeadEntity["statecode"] = new OptionSetValue(Convert.ToInt32(1));
                objLeadEntity["statuscode"] = new OptionSetValue(Convert.ToInt32(100000020));
                objLeadEntity["new_msxuploadcompletetime"] = System.DateTime.UtcNow;
                objLeadEntity["new_msxuploadstatusdetail"] = "MSX Upload Completed Successfully";
                objLeadEntity["new_lastmsxuploadattempttime"] = DateTime.UtcNow;
                objLeadEntity["new_leadupdatedby"] = "MSXAcknowledge Job";
                _log.LogInformation("Lead Acknowledgement Succeeded " + ' ' + VTCPLeadId);
                _orgService.Update(objLeadEntity);
            }
            else
            {
                Entity objLeadEntity = new Entity("lead");
                objLeadEntity.Id = new Guid(VTCPLeadId);
                objLeadEntity["statecode"] = new OptionSetValue(Convert.ToInt32(0));
                objLeadEntity["statuscode"] = new OptionSetValue(Convert.ToInt32(100000025));
                objLeadEntity["new_lastmsxuploadattempttime"] = DateTime.UtcNow;
                objLeadEntity["new_msxuploadstatusdetail"] = "MSX Upload Failed - Retry In Progress";
                objLeadEntity["new_leadupdatedby"] = "MSXAcknowledge Job";
                objLeadEntity["new_msxuploadfailuremessage"] = "Failure Type: " + FailureType + "; Error Message: " + ErrorMessage;
                if (ErrorMessage.Contains("Failed to Resolve Mandatory fields (LeadOwner)"))
                {
                    objLeadEntity["new_msxuploadstatusdetail"] = "Invalid value in Lead Owner field";
                    objLeadEntity["new_approveleadformsxupload"] = false;
                }
                _log.LogInformation("Lead Acknowledgement Failed" + ' ' + VTCPLeadId);
                _orgService.Update(objLeadEntity);
            }
        }

        #endregion

    }
}
