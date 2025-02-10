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
using System.Numerics;
using System.ServiceModel;

using System.Text;
using System.Threading;
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
using static Helper.Manager;


using Microsoft.Xrm.Sdk.Metadata;
using Label = Microsoft.Xrm.Sdk.Label;

using System.Xml.Linq;

namespace Helper
{
    public class LeadCleanupFunctions
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
        public LeadCleanupFunctions(string instancename, string clientId, string secret, string msxclientId, string msxsecret, string sqlconnection, string ssasServer, string blobConnection,string vtcpManagedIdentity, ILogger log)
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
        #region Logic for Reset MSX Leads
        /// <summary>
        /// To Insert Leads in CLAS Input table for Processing
        /// </summary>
        public void InsertResetMSXleadIntoTable()
        {
            DateTime StartTime = DateTime.UtcNow;
            Stopwatch timer = new Stopwatch();
            timer.Start();
            _log.LogInformation("Execution started: ", "InsertResetMSXleadIntoTable");
            List<CsLeads> leadList = GetIAPSResetMSXLeads();
            DataTable dt = new DataTable();
            DataTable _dtResetMSXLeadsDetails = new DataTable();
            try
            {
                if (leadList.Count > 0)
                {
                    _dtResetMSXLeadsDetails.Columns.Add("Leadid", typeof(string));
                    _dtResetMSXLeadsDetails.Columns.Add("MSXUploadTriggerTime", typeof(DateTime));
                    _dtResetMSXLeadsDetails.Columns.Add("ProcessStartTime", typeof(string));
                    _dtResetMSXLeadsDetails.Columns.Add("MSXUploadLeadNumber", typeof(string));
                    _dtResetMSXLeadsDetails.Columns.Add("MSXUploadRetryAttempts", typeof(int));
                    _dtResetMSXLeadsDetails.Columns.Add("StatusReason", typeof(string));
                    _dtResetMSXLeadsDetails.Columns.Add("MSXUploadStatusDetail", typeof(string));
                    foreach (var lead in leadList)
                    {

                        DataRow drResetMSXLeads = _dtResetMSXLeadsDetails.NewRow();

                        drResetMSXLeads["Leadid"] = lead.LeadID;
                        drResetMSXLeads["MSXUploadTriggerTime"] = lead.MSXUploadTriggerTime;
                        drResetMSXLeads["ProcessStartTime"] = StartTime.ToString();
                        drResetMSXLeads["MSXUploadLeadNumber"] = lead.MSXUploadLeadNumber;
                        drResetMSXLeads["MSXUploadRetryAttempts"] = lead.MSXUploadRetryAttempts;
                        drResetMSXLeads["StatusReason"] = lead.StatusReason;
                        drResetMSXLeads["MSXUploadStatusDetail"] = lead.MSXUploadStatusDetail;
                        _dtResetMSXLeadsDetails.Rows.Add(drResetMSXLeads);

                    }

                    if (_dtResetMSXLeadsDetails != null && _dtResetMSXLeadsDetails.Rows.Count > 0)
                    {
                        string tableName = "[dbo].[SSIS_Staging_ResetMSXLeadStatus]";
                        _manager.InsertDataIntoSQLDatabase(tableName, _dtResetMSXLeadsDetails);
                    }
                }
            }
            catch (Exception ex)
            {
                _log.LogError("InsertResetMSXLeadStatusDetails", "Failed", ex);
            }
            finally
            {
                _manager.Dispose();
                _log.LogInformation(timer.Elapsed.TotalSeconds.ToString());
                timer.Stop();
                DateTime EndTime = DateTime.UtcNow;
                SqlConnection con = new SqlConnection(LeadCleanupFunctions._sqlConnection);
                //AuthenticationResult authenticationResult = _manager.AADAunthenticationResult();
                con.AccessToken = Manager.getAccessToken(System.Environment.GetEnvironmentVariable("ResourceId"), System.Environment.GetEnvironmentVariable("VTCPManagedIdentity"), true).Result;
                SqlCommand SqlCommands = new SqlCommand();
                SqlCommands = new SqlCommand("AzureFunctionsDataLoad", con);
                SqlCommands.CommandType = CommandType.StoredProcedure;
                SqlCommands.Parameters.AddWithValue("@FunctionName", "ResetMSXLeadStatusInput");
                SqlCommands.Parameters.AddWithValue("@ProcessStartTime", StartTime.ToString());
                SqlCommands.Parameters.AddWithValue("@FunctionStartTime", StartTime.ToString());
                SqlCommands.Parameters.AddWithValue("@FunctionEndTime", EndTime.ToString());
                SqlCommands.Parameters.AddWithValue("@InitialRowsAffected", leadList.Count.ToString());
                if (_dtResetMSXLeadsDetails != null)
                    SqlCommands.Parameters.AddWithValue("@TotalRowsAffected", _dtResetMSXLeadsDetails.Rows.Count.ToString());
                else
                    SqlCommands.Parameters.AddWithValue("@TotalRowsAffected", "0");
                con.Open();
                SqlCommands.ExecuteNonQuery();
                con.Close();
            }
        }

        /// <summary>
        /// To Retrieve ResetMSXLeads from CRM
        /// </summary>
        /// <returns></returns>
        public List<CsLeads> GetIAPSResetMSXLeads()
        {
            _log.LogInformation("Execution started: ", "GetIAPSResetMSXLeads");
            List<CsLeads> Leads = new List<CsLeads>();
            EntityCollection leadscollFailed = new EntityCollection();
            int status = Convert.ToInt32(0);
            int topCount = 1000;

            QueryExpression leadattributesFailed = new QueryExpression()
            {
                EntityName = "lead",
                ColumnSet = new ColumnSet(true),
                Criteria = new FilterExpression(),
                TopCount = topCount
            };

            FilterExpression FilterAnd1 = new FilterExpression(LogicalOperator.And);
            FilterAnd1.AddCondition("new_msxuploadtriggertime", ConditionOperator.LastXDays, 30);
            FilterAnd1.AddCondition("new_msxuploadtriggertime", ConditionOperator.OlderThanXHours, 4);
            FilterAnd1.AddCondition("modifiedon", ConditionOperator.OlderThanXHours, 2);

            FilterExpression FilterOr1 = new FilterExpression(LogicalOperator.And);
            FilterOr1.AddCondition("statuscode", ConditionOperator.Equal, "100000027");
            FilterOr1.AddCondition("new_uploadedmsxleadnumber", ConditionOperator.Null);

            FilterExpression FilterOr2 = new FilterExpression(LogicalOperator.And);

            FilterExpression FilterOr21 = new FilterExpression(LogicalOperator.And);
            FilterOr21.AddCondition("statuscode", ConditionOperator.Equal, "100000025");

            FilterExpression FilterOr22 = new FilterExpression(LogicalOperator.Or);
            FilterOr22.AddCondition("new_msxuploadstatusdetail", ConditionOperator.Like, "%Retry in Progress%");
            FilterOr22.AddCondition("new_msxuploadstatusdetail", ConditionOperator.Like, "%Checking For Lead in MSX%");
            FilterOr2.AddFilter(FilterOr21);
            FilterOr2.AddFilter(FilterOr22);

            FilterExpression FilterOr3 = new FilterExpression(LogicalOperator.And);
            FilterOr3.AddCondition("statuscode", ConditionOperator.Equal, "100000030");
            FilterOr3.AddCondition("new_uploadedmsxleadnumber", ConditionOperator.Null);

            FilterExpression FinalFilter1 = new FilterExpression(LogicalOperator.Or);
            FinalFilter1.AddFilter(FilterOr1);
            FinalFilter1.AddFilter(FilterOr2);
            FinalFilter1.AddFilter(FilterOr3);

            FilterExpression FinalFilterLastmonth = new FilterExpression(LogicalOperator.And);
            FinalFilterLastmonth.AddFilter(FinalFilter1);
            FinalFilterLastmonth.AddFilter(FilterAnd1);

            FilterExpression FilterUploadedMSXLeadNumberExists = new FilterExpression(LogicalOperator.And);
            FilterUploadedMSXLeadNumberExists.AddCondition("statuscode", ConditionOperator.Equal, "100000027");
            FilterUploadedMSXLeadNumberExists.AddCondition("new_uploadedmsxleadnumber", ConditionOperator.NotNull);

            FilterExpression FinalFilter = new FilterExpression(LogicalOperator.Or);
            FinalFilter.AddFilter(FilterUploadedMSXLeadNumberExists);
            FinalFilter.AddFilter(FinalFilterLastmonth);

            leadattributesFailed.Criteria.AddFilter(FinalFilter);

            leadscollFailed = _orgService.RetrieveMultiple(leadattributesFailed);


            if (leadscollFailed != null && leadscollFailed.Entities.Count > 0)
            {
                foreach (Entity entityObject in leadscollFailed.Entities)
                {
                    Leads.Add(new CsLeads
                    {
                        LeadID = entityObject.Contains("leadid") ? new System.Guid(entityObject["leadid"].ToString()) : Guid.Empty,
                        MSXUploadTriggerTime = entityObject.Contains("new_msxuploadtriggertime") ? Convert.ToDateTime(entityObject["new_msxuploadtriggertime"]) : DateTime.MinValue,
                        MSXUploadLeadNumber = entityObject.Contains("new_uploadedmsxleadnumber") ? entityObject["new_uploadedmsxleadnumber"].ToString() : null,
                        StatusReason = entityObject.Contains("statuscode") ? entityObject.FormattedValues["statuscode"] : string.Empty,
                        MSXUploadRetryAttempts = entityObject.Contains("new_msxuploadretryattempts") ? entityObject.GetAttributeValue<int>("new_msxuploadretryattempts") : 0,
                        MSXUploadStatusDetail = entityObject.Contains("new_msxuploadstatusdetail") ? entityObject.GetAttributeValue<string>("new_msxuploadstatusdetail") : string.Empty
                    });
                }
            }
            return Leads;
        }

        /// <summary>
        /// To Update ResetMSXStatusLead output to CRM
        /// </summary>
        public void RetrieveResetMSXStatusFromTable()
        {
            DateTime StartTime = DateTime.UtcNow;
            int batch = 0;
            ExecuteMultipleSettings excuteMultipleSettings = new ExecuteMultipleSettings();
            excuteMultipleSettings.ContinueOnError = true;
            excuteMultipleSettings.ReturnResponses = true;

            ExecuteMultipleRequest updateLeadsRequest = new ExecuteMultipleRequest();
            updateLeadsRequest.Requests = new OrganizationRequestCollection();
            updateLeadsRequest.Settings = excuteMultipleSettings;

            string query = "select o.*,s.MSXUploadRetryAttempts from [dbo].[SSIS_Output_ResetMSXLeadStatus]  o (nolock)  join [dbo].[SSIS_Staging_ResetMSXLeadStatus] s (nolock)  on o.LeadId=s.LeadId ";
            DataTable dt = _manager.RetrieveDatafromSQLDatabase(query);
            string ProcessStartTime = string.Empty;
            if (dt != null && dt.Rows.Count > 0)
            {
                foreach (DataRow row in dt.Rows)
                {
                    batch++;
                    ProcessStartTime = Convert.ToString(row["ProcessStartTime"]);
                    string LeadId = row["LeadId"].ToString();
                    string LeadStatus = row["Leadstatus"].ToString();
                    string MSXLeadNumber = row["MSXLeadNumber"].ToString();
                    int MSXUploadRetryAttempts = Convert.ToInt32(row["MSXUploadRetryAttempts"]);
                    DateTime? MSXCreatedDate = !(row["MSXCreatedDate"] is DBNull) ? Convert.ToDateTime(row["MSXCreatedDate"]) : DateTime.MinValue;

                    Entity objLeadEntity = new Entity("lead");
                    objLeadEntity.Id = new Guid(LeadId);

                    _log.LogInformation("Lead Upload status" + ' ' + LeadStatus);
                    if (!string.IsNullOrEmpty(LeadStatus))
                    {
                        if (LeadStatus.ToLower() == "lead imported successfully")
                        {
                            objLeadEntity["new_uploadedmsxleadnumber"] = MSXLeadNumber;
                            objLeadEntity["statecode"] = new OptionSetValue(Convert.ToInt32(1));
                            objLeadEntity["statuscode"] = new OptionSetValue(Convert.ToInt32(100000020));
                            objLeadEntity["new_msxuploadcompletetime"] = MSXCreatedDate == DateTime.MinValue ? null : MSXCreatedDate;
                            objLeadEntity["new_msxuploadstatusdetail"] = "MSX Upload Completed – Valid Lead Found in MSX";
                            objLeadEntity["new_lastmsxuploadattempttime"] = MSXCreatedDate == DateTime.MinValue ? null : MSXCreatedDate;
                        }
                        else if (LeadStatus.ToLower() == "lead partially imported")
                        {
                            objLeadEntity["new_uploadedmsxleadnumber"] = MSXLeadNumber;
                            objLeadEntity["statecode"] = new OptionSetValue(Convert.ToInt32(1));
                            objLeadEntity["statuscode"] = new OptionSetValue(Convert.ToInt32(100000045));
                            objLeadEntity["new_msxuploadcompletetime"] = MSXCreatedDate == DateTime.MinValue ? null : MSXCreatedDate;
                            objLeadEntity["new_msxuploadstatusdetail"] = "Lead Created in MSX Without Supporting Notes";
                            objLeadEntity["new_lastmsxuploadattempttime"] = MSXCreatedDate == DateTime.MinValue ? null : MSXCreatedDate;
                        }
                        else if (LeadStatus.ToLower() == "pending fresh msx data")
                        {
                            objLeadEntity["new_msxuploadstatusdetail"] = "Checking For Lead in MSX";
                        }
                        else if (LeadStatus.ToLower() == "msx upload in progress")
                        {
                            objLeadEntity["statecode"] = new OptionSetValue(Convert.ToInt32(1));
                            objLeadEntity["statuscode"] = new OptionSetValue(Convert.ToInt32(100000020));
                            objLeadEntity["new_msxuploadstatusdetail"] = "MSX Upload Completed – Status Reset";
                        }
                        else
                        {
                            objLeadEntity["statecode"] = new OptionSetValue(Convert.ToInt32(0));
                            objLeadEntity["statuscode"] = new OptionSetValue(Convert.ToInt32(100000026));
                            objLeadEntity["new_msxuploadstatusdetail"] = null;
                            objLeadEntity["new_msxuploadstarttime"] = null;
                            objLeadEntity["new_msxuploadtriggertime"] = null;
                            objLeadEntity["new_msxuploadretryattempts"] = MSXUploadRetryAttempts + 1;
                        }
                        objLeadEntity["new_leadupdatedby"] = "ResetMSXLeadStatus Job";
                    }
                    var updateRequest = new UpdateRequest();
                    updateRequest.Target = objLeadEntity;
                    updateLeadsRequest.Requests.Add(updateRequest);
                    if (updateLeadsRequest.Requests.Count == 1000)
                    {
                        _manager.ExecuteBulkRequest(updateLeadsRequest, "Updated ResetMSXLeadStatus Output");
                        updateLeadsRequest.Requests.Clear();
                        batch = 0;
                    }
                }
                if (batch > 0)
                {
                    _manager.ExecuteBulkRequest(updateLeadsRequest, "Updated ResetMSXLeadStatus Output");
                    updateLeadsRequest.Requests.Clear();
                    batch = 0;
                }
            }
            DateTime EndTime = DateTime.UtcNow;
            SqlConnection con = new SqlConnection(LeadCleanupFunctions._sqlConnection);
            //AuthenticationResult authenticationResult = _manager.AADAunthenticationResult();
            con.AccessToken = Manager.getAccessToken(System.Environment.GetEnvironmentVariable("ResourceId"), System.Environment.GetEnvironmentVariable("VTCPManagedIdentity"), true).Result;
            SqlCommand SqlCommands = new SqlCommand();
            SqlCommands = new SqlCommand("AzureFunctionsDataLoad", con);
            SqlCommands.CommandType = CommandType.StoredProcedure;
            SqlCommands.Parameters.AddWithValue("@FunctionName", "ResetMSXLeadStatusOutput");
            SqlCommands.Parameters.AddWithValue("@ProcessStartTime", ProcessStartTime);
            SqlCommands.Parameters.AddWithValue("@FunctionStartTime", StartTime.ToString());
            SqlCommands.Parameters.AddWithValue("@FunctionEndTime", EndTime.ToString());
            SqlCommands.Parameters.AddWithValue("@InitialRowsAffected", dt.Rows.Count.ToString());
            SqlCommands.Parameters.AddWithValue("@TotalRowsAffected", TotalProcessedRecords.ToString());
            con.Open();
            SqlCommands.ExecuteNonQuery();
            con.Close();
        }
        public class LeadAnnotation
        {
            [JsonProperty("@odata.etag")]
            public string odataetag { get; set; }
            public string notetext { get; set; }
            public string _objectid_value { get; set; }
            public string annotationid { get; set; }
            public string objecttypecode { get; set; }
        }

        public class Result
        {
            [JsonProperty("@odata.context")]
            public string odatacontext { get; set; }
            public List<Value> value { get; set; }
        }

        public class Value
        {
            [JsonProperty("@odata.etag")]
            public string odataetag { get; set; }
            public string msp_leadnumber { get; set; }
            public string createdon { get; set; }
            public string leadid { get; set; }
            public List<LeadAnnotation> Lead_Annotation { get; set; }

            [JsonProperty("Lead_Annotation@odata.nextLink")]
            public string Lead_AnnotationodatanextLink { get; set; }
        }
        #endregion

        #region Logic for Contact Details Removal
        /// <summary>
        /// To Update Contact details
        /// </summary>
        public void UpdateContactDetails()
        {
            DateTime StartTime = DateTime.UtcNow;
            _log.LogInformation("Execution started Update Lead Contact Details");
            List<CsLeads> leadContactList = GetIAPSLeadContacts();

            ExecuteMultipleSettings excuteMultipleSettings = new ExecuteMultipleSettings();
            excuteMultipleSettings.ContinueOnError = true;
            excuteMultipleSettings.ReturnResponses = true;

            ExecuteMultipleRequest updateLeadsRequest = new ExecuteMultipleRequest();
            updateLeadsRequest.Requests = new OrganizationRequestCollection();
            updateLeadsRequest.Settings = excuteMultipleSettings;


            int batch = 0;
            try
            {
                if (leadContactList.Count > 0)
                {
                    foreach (var contact in leadContactList)
                    {
                        batch++;
                        Entity objLeadEntity = new Entity("lead");
                        objLeadEntity.Id = new Guid(contact.LeadID.ToString());
                        if (!string.IsNullOrWhiteSpace(contact.Firstname))
                            objLeadEntity["firstname"] = "xxxxxxxxxx";
                        if (!string.IsNullOrWhiteSpace(contact.Lastname))
                            objLeadEntity["lastname"] = "xxxxxxxxxx";
                        if (!string.IsNullOrWhiteSpace(contact.BusinessPhone))
                            objLeadEntity["mobilephone"] = "xxxxxxxxxx";
                        if (!string.IsNullOrWhiteSpace(contact.EmailAddress))
                            objLeadEntity["emailaddress1"] = "xxxxxxxxxx";
                        if (!string.IsNullOrWhiteSpace(contact.JobTitle))
                            objLeadEntity["jobtitle"] = "xxxxxxxxxx";
                        if (!string.IsNullOrWhiteSpace(contact.AdditionalContactDetails))
                            objLeadEntity["new_additionalcontacts"] = "xxxxxxxxxx";
                        if (!string.IsNullOrWhiteSpace(contact.MergedContactDetails))
                            objLeadEntity["new_mergedcontactdetails"] = "xxxxxxxxxx";
                        objLeadEntity["new_contactdetailsremoveddatetime"] = DateTime.UtcNow;
                        objLeadEntity["new_leadupdatedby"] = "Removed Contact Details Function";


                        var updateRequest = new UpdateRequest();
                        updateRequest.Target = objLeadEntity;
                        updateLeadsRequest.Requests.Add(updateRequest);

                        if (updateLeadsRequest.Requests.Count == 1000)
                        {
                            _manager.ExecuteBulkRequest(updateLeadsRequest, " Lead Contact Details Updated ");
                            updateLeadsRequest.Requests.Clear();
                            batch = 0;
                        }
                    }
                    if (batch > 0)
                    {
                        _manager.ExecuteBulkRequest(updateLeadsRequest, "Lead Contact Details Updated");
                        updateLeadsRequest.Requests.Clear();
                        batch = 0;
                    }
                }
            }
            catch (Exception ex)
            {
                _log.LogError("Updated Contact Details", "Failed", ex);
            }
            finally
            {
                _manager.Dispose();
                DateTime EndTime = DateTime.UtcNow;
                SqlConnection con = new SqlConnection(LeadCleanupFunctions._sqlConnection);
                //AuthenticationResult authenticationResult = _manager.AADAunthenticationResult();
                con.AccessToken = Manager.getAccessToken(System.Environment.GetEnvironmentVariable("ResourceId"), System.Environment.GetEnvironmentVariable("VTCPManagedIdentity"), true).Result;
                SqlCommand SqlCommands = new SqlCommand();
                SqlCommands = new SqlCommand("AzureFunctionsDataLoad", con);
                SqlCommands.CommandType = CommandType.StoredProcedure;
                SqlCommands.Parameters.AddWithValue("@FunctionName", "RemoveContactDetails");
                SqlCommands.Parameters.AddWithValue("@ProcessStartTime", StartTime.ToString());
                SqlCommands.Parameters.AddWithValue("@FunctionStartTime", StartTime.ToString());
                SqlCommands.Parameters.AddWithValue("@FunctionEndTime", EndTime.ToString());
                SqlCommands.Parameters.AddWithValue("@InitialRowsAffected", leadContactList.Count.ToString());
                SqlCommands.Parameters.AddWithValue("@TotalRowsAffected", TotalProcessedRecords.ToString());
                con.Open();
                SqlCommands.ExecuteNonQuery();
                con.Close();
            }
        }


        /// <summary>
        /// To Update Contact details
        /// </summary>.
        public List<CsLeads> GetIAPSLeadContacts()
        {
            _log.LogInformation("Execution started: ", "GetIAPSLeadContacts");
            List<CsLeads> Contacts = new List<CsLeads>();
            EntityCollection leadsContactcoll = new EntityCollection();

            QueryExpression leadContactattributesQuery = new QueryExpression()
            {
                EntityName = "lead",
                ColumnSet = new ColumnSet(true),
                Criteria = new FilterExpression(),
                TopCount = 1000

            };
            FilterExpression leadContactAttributesFilter = new FilterExpression(LogicalOperator.Or);
            // FirstName Filter
            FilterExpression leadContactFirstNameFilter = new FilterExpression(LogicalOperator.And);
            leadContactFirstNameFilter.AddCondition("firstname", ConditionOperator.NotNull);
            leadContactFirstNameFilter.AddCondition("firstname", ConditionOperator.NotEqual, " ");
            leadContactFirstNameFilter.AddCondition("firstname", ConditionOperator.NotEqual, "xxxxxxxxxx");
            leadContactAttributesFilter.AddFilter(leadContactFirstNameFilter);
            // LastName Filter
            FilterExpression leadContactLastNameFilter = new FilterExpression(LogicalOperator.And);
            leadContactLastNameFilter.AddCondition("lastname", ConditionOperator.NotNull);
            leadContactLastNameFilter.AddCondition("lastname", ConditionOperator.NotEqual, " ");
            leadContactLastNameFilter.AddCondition("lastname", ConditionOperator.NotEqual, "xxxxxxxxxx");
            leadContactAttributesFilter.AddFilter(leadContactLastNameFilter);
            // MobilePhone Filter
            FilterExpression leadContactMobilePhoneFilter = new FilterExpression(LogicalOperator.And);
            leadContactMobilePhoneFilter.AddCondition("mobilephone", ConditionOperator.NotNull);
            leadContactMobilePhoneFilter.AddCondition("mobilephone", ConditionOperator.NotEqual, " ");
            leadContactMobilePhoneFilter.AddCondition("mobilephone", ConditionOperator.NotEqual, "xxxxxxxxxx");
            leadContactAttributesFilter.AddFilter(leadContactMobilePhoneFilter);
            // EmailAddress Filter
            FilterExpression leadContactEmailAddressFilter = new FilterExpression(LogicalOperator.And);
            leadContactEmailAddressFilter.AddCondition("emailaddress1", ConditionOperator.NotNull);
            leadContactEmailAddressFilter.AddCondition("emailaddress1", ConditionOperator.NotEqual, " ");
            leadContactEmailAddressFilter.AddCondition("emailaddress1", ConditionOperator.NotEqual, "xxxxxxxxxx");
            leadContactAttributesFilter.AddFilter(leadContactEmailAddressFilter);
            // JobTitle Filter
            FilterExpression leadContactJobTitleFilter = new FilterExpression(LogicalOperator.And);
            leadContactJobTitleFilter.AddCondition("jobtitle", ConditionOperator.NotNull);
            leadContactJobTitleFilter.AddCondition("jobtitle", ConditionOperator.NotEqual, " ");
            leadContactJobTitleFilter.AddCondition("jobtitle", ConditionOperator.NotEqual, "xxxxxxxxxx");
            leadContactAttributesFilter.AddFilter(leadContactJobTitleFilter);
            //AdditionalContacts Filter
            FilterExpression leadContactAdditionalContanctsFilter = new FilterExpression(LogicalOperator.And);
            leadContactAdditionalContanctsFilter.AddCondition("new_additionalcontacts", ConditionOperator.NotNull);
            leadContactAdditionalContanctsFilter.AddCondition("new_additionalcontacts", ConditionOperator.NotEqual, " ");
            leadContactAdditionalContanctsFilter.AddCondition("new_additionalcontacts", ConditionOperator.NotEqual, "xxxxxxxxxx");
            leadContactAttributesFilter.AddFilter(leadContactAdditionalContanctsFilter);
            // MergedContactDetails  Filter
            FilterExpression leadContactMergedContanctsDetailsFilter = new FilterExpression(LogicalOperator.And);
            leadContactMergedContanctsDetailsFilter.AddCondition("new_mergedcontactdetails", ConditionOperator.NotNull);
            leadContactMergedContanctsDetailsFilter.AddCondition("new_mergedcontactdetails", ConditionOperator.NotEqual, " ");
            leadContactMergedContanctsDetailsFilter.AddCondition("new_mergedcontactdetails", ConditionOperator.NotEqual, "xxxxxxxxxx");
            leadContactAttributesFilter.AddFilter(leadContactMergedContanctsDetailsFilter);

            FilterExpression leadcreatedOnFilter = new FilterExpression(LogicalOperator.Or);
            leadcreatedOnFilter.AddCondition("new_validationcompletedtime", ConditionOperator.OlderThanXDays, 60);
            leadcreatedOnFilter.AddCondition("createdon", ConditionOperator.OlderThanXDays, 90);
            leadContactattributesQuery.Criteria.AddFilter(leadcreatedOnFilter);

            leadContactattributesQuery.Criteria.AddFilter(leadContactAttributesFilter);

            OrderExpression orderValidationTime = new OrderExpression();
            orderValidationTime.AttributeName = "new_validationcompletedtime";
            orderValidationTime.OrderType = OrderType.Ascending;

            OrderExpression orderCreatedOn = new OrderExpression();
            orderCreatedOn.AttributeName = "createdon";
            orderCreatedOn.OrderType = OrderType.Ascending;
            leadContactattributesQuery.Orders.Add(orderCreatedOn);
            leadContactattributesQuery.Orders.Add(orderValidationTime);


            leadsContactcoll = _orgService.RetrieveMultiple(leadContactattributesQuery);

            if (leadsContactcoll != null && leadsContactcoll.Entities.Count > 0)
            {
                foreach (Entity entityObject in leadsContactcoll.Entities)
                {

                    Contacts.Add(new CsLeads
                    {
                        LeadID = entityObject.Contains("leadid") ? new System.Guid(entityObject["leadid"].ToString()) : Guid.Empty,
                        Firstname = entityObject.Contains("firstname") ? entityObject["firstname"].ToString() : string.Empty,
                        Lastname = entityObject.Contains("lastname") ? entityObject["lastname"].ToString() : string.Empty,
                        BusinessPhone = entityObject.Contains("mobilephone") ? entityObject["mobilephone"].ToString() : string.Empty,
                        EmailAddress = entityObject.Contains("emailaddress1") ? entityObject["emailaddress1"].ToString() : string.Empty,
                        JobTitle = entityObject.Contains("jobtitle") ? entityObject["jobtitle"].ToString() : string.Empty,
                        AdditionalContactDetails = entityObject.Contains("new_additionalcontacts") ? entityObject["new_additionalcontacts"].ToString() : string.Empty,
                        MergedContactDetails = entityObject.Contains("new_mergedcontactdetails") ? entityObject["new_mergedcontactdetails"].ToString() : string.Empty
                    });
                }
            }
            return Contacts;
        }

        #endregion

        #region Lead Validation Expired Check
        public void LeadValidationExpiredCheck()
        {
            DateTime StartTime = DateTime.UtcNow;
            Stopwatch timer = new Stopwatch();
            timer.Start();
            _log.LogInformation("Execution started: ", "LeadValidationExpiredCheck");
            List<CsLeads> leads = GetLeadsForValidationExpiredCheck();
            int batch = 0;

            ExecuteMultipleSettings excuteMultipleSettings = new ExecuteMultipleSettings();
            excuteMultipleSettings.ContinueOnError = true;
            excuteMultipleSettings.ReturnResponses = true;

            ExecuteMultipleRequest updateLeadsRequest = new ExecuteMultipleRequest();
            updateLeadsRequest.Requests = new OrganizationRequestCollection();
            updateLeadsRequest.Settings = excuteMultipleSettings;

            if (leads.Count > 0)
            {
                Entity objLeadEntity = null;
                UpdateRequest updateRequest = null;
                string new_validationexpiredtime = string.Empty;

                foreach (CsLeads lead in leads)
                {
                    batch++;
                    new_validationexpiredtime = string.Empty;
                    objLeadEntity = new Entity("lead", lead.LeadID);

                    objLeadEntity["statuscode"] = new OptionSetValue(Convert.ToInt32(100000044));//Expi
                    objLeadEntity["new_validationexpiredtime"] = DateTime.UtcNow;
                    objLeadEntity["new_leadupdatedby"] = "LeadValidationExpiredCheck";

                    updateRequest = new UpdateRequest();
                    updateRequest.Target = objLeadEntity;
                    updateLeadsRequest.Requests.Add(updateRequest);
                }


                if (updateLeadsRequest.Requests.Count == 1000)
                {
                    _manager.ExecuteBulkRequest(updateLeadsRequest, "Updated LeadValidationExpiredCheck Details");
                    updateLeadsRequest.Requests.Clear();
                    batch = 0;
                }
            }
            if (batch > 0)
            {
                _manager.ExecuteBulkRequest(updateLeadsRequest, "Updated LeadValidationExpiredCheck Details");
                updateLeadsRequest.Requests.Clear();
                batch = 0;
            }

            _log.LogInformation(timer.Elapsed.TotalSeconds.ToString());
            timer.Stop();
            DateTime EndTime = DateTime.UtcNow;
            SqlConnection con = new SqlConnection(LeadCleanupFunctions._sqlConnection);
            //AuthenticationResult authenticationResult = _manager.AADAunthenticationResult();
            con.AccessToken = Manager.getAccessToken(System.Environment.GetEnvironmentVariable("ResourceId"), System.Environment.GetEnvironmentVariable("VTCPManagedIdentity"), true).Result;
            SqlCommand SqlCommands = new SqlCommand();
            SqlCommands = new SqlCommand("AzureFunctionsDataLoad", con);
            SqlCommands.CommandType = CommandType.StoredProcedure;
            SqlCommands.Parameters.AddWithValue("@FunctionName", "LeadValidationExpiredCheck");
            SqlCommands.Parameters.AddWithValue("@ProcessStartTime", StartTime.ToString());
            SqlCommands.Parameters.AddWithValue("@FunctionStartTime", StartTime.ToString());
            SqlCommands.Parameters.AddWithValue("@FunctionEndTime", EndTime.ToString());
            SqlCommands.Parameters.AddWithValue("@InitialRowsAffected", leads.Count.ToString());
            SqlCommands.Parameters.AddWithValue("@TotalRowsAffected", leads.Count.ToString());
            con.Open();
            SqlCommands.ExecuteNonQuery();
            con.Close();
        }

        private List<CsLeads> GetLeadsForValidationExpiredCheck()
        {
            _log.LogInformation("Execution started: ", "GetLeadsForValidationExpiredCheck");
            List<CsLeads> Leads = new List<CsLeads>();
            EntityCollection leadscoll = new EntityCollection();

            QueryExpression leadattributes = new QueryExpression()
            {
                EntityName = "lead",
                ColumnSet = new ColumnSet(true),
                Criteria = new FilterExpression(),
                TopCount = 1000
            };

            #region Filter clause
            FilterExpression statusReasonFilter = new FilterExpression(LogicalOperator.Or);
            statusReasonFilter.AddCondition("statuscode", ConditionOperator.Equal, 100000009);//Validated - Ready for MSX Upload
            statusReasonFilter.AddCondition("statuscode", ConditionOperator.Equal, 100000041);//Validated (With Warnings) - Review Before MSX Upload            
            statusReasonFilter.AddCondition("statuscode", ConditionOperator.Equal, 100000028);//MSX Upload Rejected
            statusReasonFilter.AddCondition("statuscode", ConditionOperator.Equal, 100000025); //MSX Upload Failed
            statusReasonFilter.AddCondition("statuscode", ConditionOperator.Equal, 100000027);//MSX Upload In Progress
            statusReasonFilter.AddCondition("statuscode", ConditionOperator.Equal, 100000030);//MSX Upload Triggered
            leadattributes.Criteria.AddFilter(statusReasonFilter);

            DateTime pastDate = DateTime.UtcNow.AddDays(-31).Date;
            leadattributes.Criteria.AddCondition("new_validationcompletedtime", ConditionOperator.OnOrBefore, pastDate);
            leadattributes.AddOrder("new_validationcompletedtime", OrderType.Ascending);
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
                        ValidationCompletedTime = entityObject.Contains("new_validationcompletedtime") ? entityObject.GetAttributeValue<DateTime>("new_validationcompletedtime") : DateTime.MinValue,
                        StatusReason = entityObject.Contains("statuscode") ? entityObject.GetAttributeValue<OptionSetValue>("statuscode").Value.ToString() : string.Empty
                    });
                }
            }
            return Leads;
        }

        #endregion

        public void UpdateAutoNumberLeads()
        {
            try
            {
                DateTime StartTime = DateTime.UtcNow;
                Stopwatch timer = new Stopwatch();
                int leadNumber = 0;
                timer.Start();
                _log.LogInformation("Execution started: ", "UpdateAutoNumberLeads");
                List<CsLeads> largestLeadNumber = GetLeadstoUpdateAutoNumber(true);
                List<CsLeads> leads = GetLeadstoUpdateAutoNumber(false);
                int batch = 0;
                if (largestLeadNumber.Count > 0)
                {
                    leadNumber =Convert.ToInt32( largestLeadNumber[0].LeadNumber.Split('-')[1]);
                }
                ExecuteMultipleSettings excuteMultipleSettings = new ExecuteMultipleSettings();
                excuteMultipleSettings.ContinueOnError = true;
                excuteMultipleSettings.ReturnResponses = true;

                ExecuteMultipleRequest updateLeadsRequest = new ExecuteMultipleRequest();
                updateLeadsRequest.Requests = new OrganizationRequestCollection();
                updateLeadsRequest.Settings = excuteMultipleSettings;

                if (leads.Count > 0)
                {
                    foreach (CsLeads lead in leads)
                    {
                        batch++;
                        Entity objLeadEntity = null;
                        UpdateRequest updateRequest = new UpdateRequest();
                        objLeadEntity = new Entity("lead", lead.LeadID);
                        objLeadEntity["new_leadnumber"] = "VTL-" + (++leadNumber).ToString("D9");
                        updateRequest.Target = objLeadEntity;
                        updateLeadsRequest.Requests.Add(updateRequest);
                    }


                    if (updateLeadsRequest.Requests.Count == 1000)
                    {
                        _manager.ExecuteBulkRequest(updateLeadsRequest, "Updated Auto Lead Number Details");
                        updateLeadsRequest.Requests.Clear();
                        batch = 0;
                    }
                }
                if (batch > 0)
                {
                    _manager.ExecuteBulkRequest(updateLeadsRequest, "Updated Auto Lead Number Details");
                    updateLeadsRequest.Requests.Clear();
                    batch = 0;
                }

                _log.LogInformation(timer.Elapsed.TotalSeconds.ToString());
                timer.Stop();
                DateTime EndTime = DateTime.UtcNow;
                SqlConnection con = new SqlConnection(LeadCleanupFunctions._sqlConnection);
                //AuthenticationResult authenticationResult = _manager.AADAunthenticationResult();
                con.AccessToken = Manager.getAccessToken(System.Environment.GetEnvironmentVariable("ResourceId"), System.Environment.GetEnvironmentVariable("VTCPManagedIdentity"), true).Result;
                SqlCommand SqlCommands = new SqlCommand();
                SqlCommands = new SqlCommand("AzureFunctionsDataLoad", con);
                SqlCommands.CommandType = CommandType.StoredProcedure;
                SqlCommands.Parameters.AddWithValue("@FunctionName", "UpdateAutoNumberLeads");
                SqlCommands.Parameters.AddWithValue("@ProcessStartTime", StartTime.ToString());
                SqlCommands.Parameters.AddWithValue("@FunctionStartTime", StartTime.ToString());
                SqlCommands.Parameters.AddWithValue("@FunctionEndTime", EndTime.ToString());
                SqlCommands.Parameters.AddWithValue("@InitialRowsAffected", leads.Count.ToString());
                SqlCommands.Parameters.AddWithValue("@TotalRowsAffected", leads.Count.ToString());
                con.Open();
                SqlCommands.ExecuteNonQuery();
                con.Close();
            }
            catch(Exception ex)
            {
                _log.LogInformation(ex.Message);
            }
           
        }

        public List<CsLeads> GetLeadstoUpdateAutoNumber(bool leadNumber)
        {
            _log.LogInformation("Execution started: ", "GetLeadstoUpdateAutoNumber");
            List<CsLeads> Leads = new List<CsLeads>();
            EntityCollection leadscoll = new EntityCollection();
            string xml=string.Empty;
            QueryExpression leadAttributes = new QueryExpression()
            {
                EntityName = "lead",
                ColumnSet = new ColumnSet(true),
                Criteria = new FilterExpression()
                
            };

            #region Filter clause
            FilterExpression leadFilter = new FilterExpression();
            if (leadNumber)
            {
                xml= @" <fetch version=""1.0"" output-format=""xml-platform"" mapping=""logical"" distinct=""false"" count=""1"">
                <entity name=""lead"">
                <attribute name=""new_leadnumber"" />
                <filter type=""and"">
                <condition attribute=""new_leadnumber"" operator=""not-null"" />
                </filter>
                <order attribute=""new_leadnumber"" descending=""true"" />
                </entity>
                </fetch>";
                leadscoll = _orgService.RetrieveMultiple(new FetchExpression(xml));
            }
            else 
            {
                leadAttributes.TopCount = 1000;
                leadFilter.AddCondition("new_leadnumber", ConditionOperator.Null);
                leadFilter.AddCondition("statuscode", ConditionOperator.NotEqual, 100000037);
                leadAttributes.AddOrder("createdon", OrderType.Ascending);
                leadAttributes.Criteria.AddFilter(leadFilter);
                leadscoll = _orgService.RetrieveMultiple(leadAttributes);
            }
         
            #endregion

            if (leadscoll != null && leadscoll.Entities.Count > 0)
            {
                _log.LogInformation("leads count: " + leadscoll.Entities.Count);
                foreach (Entity entityObject in leadscoll.Entities)
                {
                    Leads.Add(new CsLeads
                    {
                        LeadID = entityObject.Contains("leadid") ? new System.Guid(entityObject["leadid"].ToString()) : Guid.Empty,
                        LeadNumber = entityObject.Contains("new_leadnumber") ? entityObject.GetAttributeValue<string>("new_leadnumber") : string.Empty,
                        Createdon=entityObject.Contains("createdon")?entityObject.GetAttributeValue<DateTime>("createdon"):DateTime.MinValue
                    });
                }
            }
            return Leads;
        }
        #region Lead Duplicate Lead Number Removal
        public void RemoveDuplicateLeaNumber()
        {
            DateTime StartTime = DateTime.UtcNow;
            int batch = 0;
            ExecuteMultipleSettings excuteMultipleSettings = new ExecuteMultipleSettings();
            excuteMultipleSettings.ContinueOnError = true;
            excuteMultipleSettings.ReturnResponses = true;

            ExecuteMultipleRequest updateLeadsRequest = new ExecuteMultipleRequest();
            updateLeadsRequest.Requests = new OrganizationRequestCollection();
            updateLeadsRequest.Settings = excuteMultipleSettings;

            string query = "Select Top 1000 * from SSIS_DuplicateLeadNumber_Output";
            DataTable dt = _manager.RetrieveDatafromSQLDatabase(query);
            string ProcessStartTime = string.Empty;

            if (dt != null && dt.Rows.Count > 0)
            {
                foreach (DataRow row in dt.Rows)
                {
                    batch++;

                    string leadId = row["leadid"].ToString();
                    Entity objLeadEntity = new Entity("lead");
                    objLeadEntity.Id = new Guid(leadId);
                    objLeadEntity["new_leadnumber"] = null;
                    
                    var updateRequest = new UpdateRequest();
                    updateRequest.Target = objLeadEntity;
                    updateLeadsRequest.Requests.Add(updateRequest);
                    if (updateLeadsRequest.Requests.Count == 1000)
                    {
                        _manager.ExecuteBulkRequest(updateLeadsRequest, "Removed Lead Number");
                        updateLeadsRequest.Requests.Clear();
                        batch = 0;
                    }
                }
                if (batch > 0)
                {
                    _manager.ExecuteBulkRequest(updateLeadsRequest, "Removed Lead Number");
                    updateLeadsRequest.Requests.Clear();
                    batch = 0;
                }

            }
            DateTime EndTime = DateTime.UtcNow;
            SqlConnection con = new SqlConnection(LeadCleanupFunctions._sqlConnection);
            //AuthenticationResult authenticationResult = _manager.AADAunthenticationResult();
            con.AccessToken = Manager.getAccessToken(System.Environment.GetEnvironmentVariable("ResourceId"), System.Environment.GetEnvironmentVariable("VTCPManagedIdentity"), true).Result;
            SqlCommand SqlCommands = new SqlCommand();
            SqlCommands = new SqlCommand("AzureFunctionsDataLoad", con);
            SqlCommands.CommandType = CommandType.StoredProcedure;
            SqlCommands.Parameters.AddWithValue("@FunctionName", "RemoveDuplicateLeaNumber");
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

    }
}
