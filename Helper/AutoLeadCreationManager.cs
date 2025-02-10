using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.Extensions.Logging;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Sdk.WebServiceClient;
using Microsoft.Xrm.Tooling.Connector;

using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;

using System.Net;
using System.Numerics;
using System.ServiceModel;


using System.Threading;
using System.Threading.Tasks;

using Newtonsoft.Json.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.IO;

using Newtonsoft.Json;


using System.Configuration;
using System.Text.RegularExpressions;
using Microsoft.Xrm.Sdk.Metadata;
using System.Drawing;


namespace Helper
{
    public class AutoLeadCreationManager
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
        private static string _nymeriaSchema;
        private static string _nymeriaSqlConnection;
        private static int isYes = Convert.ToInt32(100000000);
        private static int isNo = Convert.ToInt32(100000001);
        private static int isNotFound = Convert.ToInt32(100000002);
        private static int statusDisqualified = Convert.ToInt32(2);
        private static DateTime startDatetime;
        //private const int MaxLength = 4000;
        private const string MalAccountIdMatch = "Matched By VTCP System";
        Manager _manager;

        public AutoLeadCreationManager(string instancename, string clientId, string secret, string msxclientId, string msxsecret, string sqlconnection, string ssasServer, string blobConnection, ILogger log, string NymeriaSchema, string NymeriaSqlConnection, string vtcpManagedIdentity)
        {
            _instancename = instancename;
            _clientId = clientId;
            _secret = secret;
            _msxclientId = msxclientId;
            _msxsecret = msxsecret;
            _sqlConnection = sqlconnection;
            _ssasServer = ssasServer;
            _blobConnection = blobConnection;
            _log = log;
            _nymeriaSchema = NymeriaSchema;
            _nymeriaSqlConnection = NymeriaSqlConnection;
            _vtcpManagedIdentity = vtcpManagedIdentity;
            _manager = new Manager(_instancename, _clientId, _secret, _msxclientId, _msxsecret, _sqlConnection, _ssasServer, _blobConnection, _vtcpManagedIdentity, _log);
            _orgService = _manager.GetOrganizationService().Result;
        }

        #region Auto Lead Creation request
        public void UpdateAutoLeadCreationRequestStatus(string requestID)
        {

            if (!string.IsNullOrWhiteSpace(requestID))
            {
                Entity autoLead = new Entity("new_autoleadcreationrequest");
                autoLead.Id = new Guid(requestID);
                autoLead["statuscode"] = new OptionSetValue(100000001);
                _orgService.Update(autoLead);
            }


        }
        public void LeadCreationOutput()
        {
            try
            {
                DateTime StartTime = DateTime.UtcNow;

                List<AutoLeadCreation> DeleteLeads = new List<AutoLeadCreation>();
                int batch = 0;
                ExecuteMultipleSettings excuteMultipleSettings = new ExecuteMultipleSettings();
                excuteMultipleSettings.ContinueOnError = true;
                excuteMultipleSettings.ReturnResponses = true;

                ExecuteMultipleRequest createLeadsRequest = new ExecuteMultipleRequest();
                createLeadsRequest.Requests = new OrganizationRequestCollection();
                createLeadsRequest.Settings = excuteMultipleSettings;

                string retrieveQuery = "select Top 500 * from [dbo].[SSIS_Output_LeadCreation] nolock order by CustomerTPIDAggr ASC,AgreementProfileSortOrder ASC,CAST(ISNULL(ExpiringAmountTotal,0) as decimal(18,2)) DESC";
                DataTable dtleadoutput = _manager.RetrieveDatafromSQLDatabase(retrieveQuery);
                String MarketingListId = string.Empty;
                DataTable dtleadoutputlist = new DataTable();
                string AutoLeadCreationRequestId = string.Empty;
                string ProcessStartTime = string.Empty;
                if (dtleadoutput != null && dtleadoutput.Rows.Count > 0)
                {
                    List<SubsidiaryItem> objSubsidiaryItem = new List<SubsidiaryItem>();
                    var subsidiaraycoll = GetEntityCollection(_orgService, "new_subsidiary");
                    foreach (var subEntity in subsidiaraycoll.Entities)
                    {
                        objSubsidiaryItem.Add(new SubsidiaryItem
                        {
                            id = Convert.ToString(subEntity.GetAttributeValue<Guid>("new_subsidiaryid")),
                            subsidiaryname = subEntity.GetAttributeValue<string>("new_name")
                        });
                    }
                    //string retreiveList = "select distinct MarketingListId,AutoLeadCreationRequestId,MergeMultipleLeadsForSameCustomer from [dbo].[SSIS_Output_LeadCreation]";
                    //dtleadoutputlist = _manager.RetrieveDatafromSQLDatabase(retreiveList);
                    EnumAttributeMetadata primaryProductOptionset = GetOptionSetValue("lead", "new_primaryproductcampaign", "Azure", _orgService);
                    var primaryProductDictionary = new Dictionary<string, int?>();
                    foreach (var f in primaryProductOptionset.OptionSet.Options)
                    {
                        primaryProductDictionary.Add(f.Label.UserLocalizedLabel.Label.ToString(), f.Value);
                    }
                    foreach (DataRow row in dtleadoutput.Rows)
                    {
                        batch++;
                        string AgreementID = Convert.ToString(row["AgreementID"]);
                        string AgreementExpirationDate = Convert.ToString(row["AgreementExpirationDate"]);
                        string AdvisorTPName = Convert.ToString(row["AdvisorTPName"]);
                        string ResellerTPName = Convert.ToString(row["ResellerTPName"]);
                        string DistributorTPName = Convert.ToString(row["DistributorTPName"]);
                        string ExpiringAmountTotal = Convert.ToString(row["ExpiringAmountTotal"]);
                        string IsAutoCreatedLead = Convert.ToString(row["IsAutoCreatedLead"]);
                        string CustomerTPIDAggr = Convert.ToString(row["CustomerTPIDAggr"]);
                        string CustomerTPName = Convert.ToString(row["CustomerTPName"]);
                        string CityName = Convert.ToString(row["CityName"]);
                        string SubsidiaryName = Convert.ToString(row["SubsidiaryName"]);
                        string AccountNumber = Convert.ToString(row["AccountNumber"]);
                        string ExpiringProducts = Convert.ToString(row["ExpiringProducts"]);
                        string LicenseProgramName = Convert.ToString(row["LicenseProgramName"]);
                        int? AgreementProfileSortOrder = !string.IsNullOrWhiteSpace(row["AgreementProfileSortOrder"].ToString()) ? (int?)Convert.ToInt32(row["AgreementProfileSortOrder"]) : null;
                        string Topic = Convert.ToString(row["Topic"]);
                        MarketingListId = Convert.ToString(row["MarketingListId"]);
                        string PreApproveForMSXUpload = Convert.ToString(row["PreApproveForMSXUpload"]);
                        string MergeMultipleLeadsForSameCustomer = Convert.ToString(row["MergeMultipleLeadsForSameCustomer"]);
                        string MSXLeadOwnerAlias = Convert.ToString(row["MSXLeadOwnerAlias"]);
                        int? PrimaryProduct = !string.IsNullOrWhiteSpace(Convert.ToString(row["PrimaryProduct"])) ? primaryProductDictionary.TryGetValue(Convert.ToString(row["PrimaryProduct"]), out PrimaryProduct) ? PrimaryProduct : null : null;
                        long Id = Convert.ToInt64(row["Id"].ToString());
                        string InternalId = Convert.ToString(row["LeadID"].ToString());
                        string AgentName = Convert.ToString(row["AgentName"]);
                        string PhoneNumber = Convert.ToString(row["PhoneNumber"]);
                        string VT_Priority = Convert.ToString(row["VT_Priority"]);
                        decimal? NymeriaRanking = !string.IsNullOrWhiteSpace(row["NymeriaRanking"].ToString()) ? (decimal?)Convert.ToDecimal(row["NymeriaRanking"]) : null;
                        string NymeriaPriority = Convert.ToString(row["NymeriaPriority"]);
                        string AllocadiaID = Convert.ToString(row["AllocadiaID"]);
                        string CallPrepSheetFlag = Convert.ToString(row["CallPrepSheetFlag"]);
                        string CCMFlag = Convert.ToString(row["CCMFlag"]);
                        AutoLeadCreationRequestId = Convert.ToString(row["AutoLeadCreationRequestId"]);
                        DeleteLeads.Add(new AutoLeadCreation { Id = Id });
                        Entity objLeadEntity = new Entity("lead");
                        objLeadEntity["new_importedaccount"] = !string.IsNullOrEmpty(CustomerTPName) ? _manager.ReplaceHexadecimalSymbols(CustomerTPName) : string.Empty;
                        objLeadEntity["new_customerlistname"] = new EntityReference("list", new Guid(MarketingListId));
                        if (!string.IsNullOrEmpty(SubsidiaryName))
                        {
                            string subsidiaryDB = _manager.ReplaceHexadecimalSymbols(SubsidiaryName);
                            foreach (var subsidiary in objSubsidiaryItem)
                            {
                                if (subsidiary.subsidiaryname.ToLower() == subsidiaryDB.ToLower())
                                {
                                    objLeadEntity["new_subsidiary"] = new EntityReference("new_subsidiary", new Guid(subsidiary.id));
                                    break;
                                }
                            }
                        }

                        if (!string.IsNullOrWhiteSpace(Topic))
                        {
                            string topic = _manager.LimitCharacterCount(Topic, 300);
                            objLeadEntity["subject"] = _manager.ReplaceHexadecimalSymbols(topic);
                        }
                        else 
                        {
                            objLeadEntity["subject"] = CustomerTPName;
                        }
                        

                        objLeadEntity["new_agreementexpirationdate"] = !string.IsNullOrEmpty(AgreementExpirationDate) ? (DateTime?)Convert.ToDateTime(AgreementExpirationDate) : null;
                        objLeadEntity["new_advisorname"] = !string.IsNullOrEmpty(AdvisorTPName) ? _manager.ReplaceHexadecimalSymbols(AdvisorTPName) : string.Empty;
                        objLeadEntity["new_resellername"] = !string.IsNullOrEmpty(ResellerTPName) ? _manager.ReplaceHexadecimalSymbols(ResellerTPName) : string.Empty;
                        objLeadEntity["new_distributorname"] = !string.IsNullOrEmpty(DistributorTPName) ? _manager.ReplaceHexadecimalSymbols(DistributorTPName) : string.Empty;
                        objLeadEntity["new_expiringamount"] = !string.IsNullOrEmpty(ExpiringAmountTotal) ? Convert.ToDecimal(ExpiringAmountTotal) : (object)null;
                        objLeadEntity["new_expiringproductdetails"] = !string.IsNullOrEmpty(ExpiringProducts) ? ExpiringProducts : string.Empty;
                        objLeadEntity["new_licensingprogram"] = !string.IsNullOrEmpty(LicenseProgramName) ? _manager.ReplaceHexadecimalSymbols(LicenseProgramName) : string.Empty;
                        objLeadEntity["new_agreementid"] = !string.IsNullOrEmpty(AgreementID) ? AgreementID : string.Empty;
                        objLeadEntity["new_mssalestpid"] = !string.IsNullOrEmpty(CustomerTPIDAggr) ? CustomerTPIDAggr : string.Empty;
                        objLeadEntity["address1_city"] = !string.IsNullOrEmpty(CityName) ? _manager.ReplaceHexadecimalSymbols(CityName) : string.Empty;
                        objLeadEntity["new_msxaccountid"] = !string.IsNullOrEmpty(AccountNumber) ? AccountNumber : string.Empty;
                        objLeadEntity["new_msxleadowneralias"] = !string.IsNullOrEmpty(MSXLeadOwnerAlias) ? MSXLeadOwnerAlias : string.Empty;
                        objLeadEntity["new_primaryproductcampaign"] = PrimaryProduct != null ? new OptionSetValue(Convert.ToInt32(PrimaryProduct)) : null;
                        objLeadEntity["new_internalid"] = !string.IsNullOrWhiteSpace(InternalId) ? InternalId : string.Empty;
                        objLeadEntity["mobilephone"] = !string.IsNullOrWhiteSpace(PhoneNumber) ? PhoneNumber : string.Empty;
                        objLeadEntity["new_clas_vt_priority"] = !string.IsNullOrWhiteSpace(VT_Priority) ? VT_Priority : string.Empty;
                        objLeadEntity["new_nymeriaranking"] = NymeriaRanking != null ? NymeriaRanking : (object)null;
                        objLeadEntity["new_nymeriapriority"] = !string.IsNullOrWhiteSpace(NymeriaPriority) ?  NymeriaPriority : string.Empty;
                        objLeadEntity["new_campaigncode"] = !string.IsNullOrWhiteSpace(AllocadiaID) ? AllocadiaID : string.Empty;
                        if (CCMFlag == "True")
                        {
                            objLeadEntity["new_hascontactsinccm"] = new OptionSetValue(100000000);
                        }
                        else
                        {
                            objLeadEntity["new_hascontactsinccm"] = new OptionSetValue(100000001);
                        }
                        if (CallPrepSheetFlag == "True")
                        {
                            objLeadEntity["new_callprepsheetavailable"] = new OptionSetValue(100000000);
                        }
                        else
                        {
                            objLeadEntity["new_callprepsheetavailable"] = new OptionSetValue(100000001);
                        }

                        if (CallPrepSheetFlag == "True")
                        {
                            objLeadEntity["new_callprepsheetlink"] = $"https://msit.powerbi.com/groups/69cc3a03-b487-4d36-af62-d75b5da7992b/reports/013e6dd8-44fb-4138-8c2d-91326ecff9f8/ReportSection?experience=power-bi&filter=GenAIFact%2fTPID%20eq%20%27company{CustomerTPIDAggr}%27";
                        }
                        if (PreApproveForMSXUpload == "Yes")
                        {
                            objLeadEntity["new_preapproveformsxupload"] = new OptionSetValue(100000000);
                        }
                        else
                        {
                            objLeadEntity["new_preapproveformsxupload"] = new OptionSetValue(100000001);
                        }

                        if (MergeMultipleLeadsForSameCustomer == "Yes")
                        {
                            objLeadEntity["new_mergemultipleleadsforsamecustomer"] = new OptionSetValue(100000000);
                        }
                        else if (MergeMultipleLeadsForSameCustomer == "No")
                        {
                            objLeadEntity["new_mergemultipleleadsforsamecustomer"] = new OptionSetValue(100000001);
                            objLeadEntity["new_mergestatus"] = "N/A";
                        }
                        else
                        {
                            objLeadEntity["new_mergemultipleleadsforsamecustomer"] = new OptionSetValue(100000002);
                        }

                        if (IsAutoCreatedLead == "Yes")
                        {
                            objLeadEntity["new_isautocreatedlead"] = true;
                        }
                        else
                        {
                            objLeadEntity["new_isautocreatedlead"] = false;
                        }
                        objLeadEntity["new_agreementprofilesortorder"] = AgreementProfileSortOrder;

                        var createRequest = new CreateRequest();
                        createRequest.Target = objLeadEntity;
                        createLeadsRequest.Requests.Add(createRequest);


                        if (createLeadsRequest.Requests.Count == 250)
                        {
                            _manager.ExecuteBulkRequest(createLeadsRequest, "Created Lead Details", true);
                            DeleteRecordsInDatabase(DeleteLeads, "SSIS_Output_LeadCreation");
                            createLeadsRequest.Requests.Clear();
                            DeleteLeads.Clear();
                            batch = 0;
                        }
                    }
                    if (batch > 0)
                    {
                        _manager.ExecuteBulkRequest(createLeadsRequest, "Created Lead Details", true);
                        DeleteRecordsInDatabase(DeleteLeads, "SSIS_Output_LeadCreation");
                        createLeadsRequest.Requests.Clear();
                        DeleteLeads.Clear();
                        batch = 0;
                    }
                }
                QueryExpression autoLeadRequests = new QueryExpression
                {
                    EntityName = "new_autoleadcreationrequest",
                    ColumnSet = new ColumnSet(true),
                    Criteria = new FilterExpression()
                };
                autoLeadRequests.Criteria.AddCondition("statuscode", ConditionOperator.Equal, Convert.ToInt32(100000001));
                EntityCollection autoLeadColl = new EntityCollection();
                autoLeadColl = _orgService.RetrieveMultiple(autoLeadRequests);


                foreach (var autoLeadReq in autoLeadColl.Entities)
                {
                    string retrieveCount = "select count(1) as Count from [dbo].[SSIS_Output_LeadCreation] nolock  where MarketingListId='" + Convert.ToString(autoLeadReq.GetAttributeValue<EntityReference>("new_marketinglist").Id) + "'";
                    DataTable dtleadoutputcount = _manager.RetrieveDatafromSQLDatabase(retrieveCount);
                    int OutputPendingRecordCount = Convert.ToInt32(dtleadoutputcount.Rows[0].ItemArray[0]);
                    autoLeadReq["new_leadsawaitingcreation"] = OutputPendingRecordCount.ToString();

                    if (OutputPendingRecordCount == 0)
                    {

                        if (autoLeadReq.Contains("new_mergemultipleleadsforsamecustomer") && autoLeadReq.GetAttributeValue<OptionSetValue>("new_mergemultipleleadsforsamecustomer")!=null && autoLeadReq.GetAttributeValue<OptionSetValue>("new_mergemultipleleadsforsamecustomer").Value == 100000000)
                        {
                            autoLeadReq["statuscode"] = new OptionSetValue(100000002);
                        }
                        else
                        {
                            autoLeadReq["statuscode"] = new OptionSetValue(100000003);
                        }
                        if(autoLeadReq.GetAttributeValue<OptionSetValue>("new_leadcreationrequesttype").Value == 100000002)
                        {
                            SqlConnection conNymeria= new SqlConnection(AutoLeadCreationManager._nymeriaSqlConnection);
                            //AuthenticationResult authenticationResultNymeria = _manager.AADAunthenticationResult();
                            conNymeria.AccessToken = Manager.getAccessToken(System.Environment.GetEnvironmentVariable("ResourceId"), System.Environment.GetEnvironmentVariable("VTCPManagedIdentity"), true).Result;
                            SqlCommand SqlCommandsNymeria = new SqlCommand();
                            SqlCommandsNymeria = new SqlCommand("update "+_nymeriaSchema+".[PowerApp:LeadCreationRequests] set status = 'Leads Uploaded to VTCP', VTCPLeadUploadCompleteDateTime = getutcdate() where RequestId = '"+autoLeadReq.Id+"'",conNymeria);
                            SqlCommandsNymeria.CommandType = CommandType.Text;
                            conNymeria.Open();
                            SqlCommandsNymeria.ExecuteNonQuery();
                            conNymeria.Close();
                        }

                    }
                    string fetchTotalLeads = $@"<fetch distinct='false' mapping='logical' aggregate='true' ><entity name='lead' ><filter type='and'>
                   <condition attribute='new_customerlistname' operator='eq' value='" + Convert.ToString(autoLeadReq.GetAttributeValue<EntityReference>("new_marketinglist").Id) + "' /> </filter> <attribute name='new_customerlistname' alias='count' aggregate='count' /> </entity></fetch>";
                    //retrieveCount = "select count(1) as Count from [dbo].[SSIS_Output_LeadCreation_history] nolock  where MarketingListId='" + Convert.ToString(autoLeadReq.GetAttributeValue<EntityReference>("new_marketinglist").Id) + "'";
                    // DataTable dtleadoutputhistorycount = _manager.RetrieveDatafromSQLDatabase(retrieveCount);
                    int LeadsCreatedRecordCount = 0;
                    try
                    {
                         LeadsCreatedRecordCount = ExecuteFetchXmlCountQuery(fetchTotalLeads, _orgService);
                    }
                    catch(Exception)
                    {
                        LeadsCreatedRecordCount = 50000;
                    }
                    if (LeadsCreatedRecordCount == 50000)
                    {
                        autoLeadReq["new_leadscreated"] = Convert.ToString(LeadsCreatedRecordCount + "+");
                    }
                    else
                    {
                        autoLeadReq["new_leadscreated"] = Convert.ToString(LeadsCreatedRecordCount);
                    }
                    _orgService.Update(autoLeadReq);

                }
                DateTime EndTime = DateTime.UtcNow;
                SqlConnection con = new SqlConnection(AutoLeadCreationManager._sqlConnection);
                //AuthenticationResult authenticationResult = _manager.AADAunthenticationResult();
                con.AccessToken = Manager.getAccessToken(System.Environment.GetEnvironmentVariable("ResourceId"), System.Environment.GetEnvironmentVariable("VTCPManagedIdentity"), true).Result;
                SqlCommand SqlCommands = new SqlCommand();
                SqlCommands = new SqlCommand("AzureFunctionsDataLoad", con);
                SqlCommands.CommandType = CommandType.StoredProcedure;
                SqlCommands.Parameters.AddWithValue("@FunctionName", "LeadCreationOutput");
                SqlCommands.Parameters.AddWithValue("@ProcessStartTime", StartTime.ToString());
                SqlCommands.Parameters.AddWithValue("@FunctionStartTime", StartTime.ToString());
                SqlCommands.Parameters.AddWithValue("@FunctionEndTime", EndTime.ToString());
                SqlCommands.Parameters.AddWithValue("@InitialRowsAffected", dtleadoutput.Rows.Count.ToString());
                SqlCommands.Parameters.AddWithValue("@TotalRowsAffected", _manager.TotalProcessedRecords.ToString());
                con.Open();
                SqlCommands.ExecuteNonQuery();
                con.Close();
            }
            catch (Exception ex)
            {
                _log.LogInformation(ex.Message);
                _log.LogError(ex.Message);
                throw ex;
            }
        }
        public int ExecuteFetchXmlCountQuery(string fetchXml, IOrganizationService orgService)
        {
            FetchExpression fetchExpression = new FetchExpression(fetchXml);
            EntityCollection result = orgService.RetrieveMultiple(fetchExpression);

            if (result.Entities.Count > 0)
            {
                foreach (var c in result.Entities)
                {
                    int count = (Int32)((AliasedValue)c["count"]).Value;

                    return count;
                };
            }

            return 0;
        }
        public static EnumAttributeMetadata GetOptionSetValue(string entityName, string fieldName, string optionSetLabel, IOrganizationService service)
        {
            var attReq = new RetrieveAttributeRequest();
            attReq.EntityLogicalName = entityName;
            attReq.LogicalName = fieldName;
            attReq.RetrieveAsIfPublished = true;

            var attResponse = (RetrieveAttributeResponse)service.Execute(attReq);
            var attMetadata = (EnumAttributeMetadata)attResponse.AttributeMetadata;

            return attMetadata;
        }
        public void LeadCreationInsert()
        {
            DateTime StartTime = DateTime.UtcNow;
            string subsidiary = string.Empty;
            string area = string.Empty;
            string region = string.Empty;
            string subSegment = string.Empty;
            string smcType = string.Empty;
            string quarter = string.Empty;
            string month = string.Empty;
            string licenceProgram = string.Empty;
            string revenueRange = string.Empty;
            string customerRevenueRange = string.Empty;
            string marketingList = string.Empty;
            string campaignType = string.Empty;
            string selectedClasPropensity = string.Empty;
            string preApproveMSXUpload = string.Empty;
            string MergeMultipleLeadsForSameCustomer = string.Empty;
            string SelectedAgreementProfiles = string.Empty;
            string SelectedAgreementProfileDetails = string.Empty;
            string SelectedCustomerProfiles = string.Empty;
            string SelectedCustomerProfileDetails = string.Empty;
            string SelectedABMAccountFlags = string.Empty;
            string ExtendedTimeframe = string.Empty;
            string SelectedVLAllocationFlags = string.Empty;
            string SelectedMALDomainFlags = string.Empty;
            string SelectedMALNameMatchFlags = string.Empty;
            string SelectedAgreementIDs = string.Empty;
            string SelectedTPIDs = string.Empty;
            int leadCreationRequestType = 0;
            string Vendor = String.Empty;
            String autoLeadCreationRequestId = string.Empty;
            string leadAssignmentAgentDetails = string.Empty;
            string FullQuery = string.Empty;
            StringBuilder InsertQuery = new StringBuilder();
            string SelectedOrgSizes = string.Empty;
            string SelectedProducts = string.Empty;
            // Update #Leads Pending Creation details 
            UpdatePendingLeadCreation();

            QueryExpression leadCreation = new QueryExpression()
            {
                EntityName = "new_autoleadcreationrequest",
                ColumnSet = new ColumnSet(true),
                TopCount = 1,
                Criteria = new FilterExpression()
            };
            leadCreation.Criteria.AddCondition("statecode", ConditionOperator.Equal, Convert.ToInt32(0));
            leadCreation.Criteria.AddCondition("new_marketinglist", ConditionOperator.NotNull);

            var nymeriaFilter = new FilterExpression(LogicalOperator.Or);
            var condition1 = new FilterExpression(LogicalOperator.And);
            condition1.AddCondition("new_leadcreationrequesttype", ConditionOperator.In, Convert.ToInt32(100000001), Convert.ToInt32(100000000));
            condition1.AddCondition("statuscode", ConditionOperator.Equal, Convert.ToInt32(100000000));

            var condition2 = new FilterExpression(LogicalOperator.And);
            condition2.AddCondition("new_leadcreationrequesttype", ConditionOperator.Equal, Convert.ToInt32(100000002));
            condition2.AddCondition("new_nymeriarequestsenttodb", ConditionOperator.Equal, Convert.ToInt32(100000001));
            nymeriaFilter.AddFilter(condition1);
            nymeriaFilter.AddFilter(condition2);

            leadCreation.Criteria.AddFilter(nymeriaFilter);
            leadCreation.AddOrder("createdon", OrderType.Ascending);

            // Retrieve values from autoleadcreationrequest table
            EntityCollection leadCreationcoll = _orgService.RetrieveMultiple(leadCreation);

            if (leadCreationcoll != null && leadCreationcoll.Entities.Count > 0)
            {
                foreach (Entity lead in leadCreationcoll.Entities)
                {
                    subsidiary = lead.GetAttributeValue<string>("new_selectedsubsidiaries") != null ? CommaSeparation(lead.GetAttributeValue<string>("new_selectedsubsidiaries")) : string.Empty;
                    marketingList = lead.GetAttributeValue<EntityReference>("new_marketinglist") != null ? lead.GetAttributeValue<EntityReference>("new_marketinglist").Id.ToString() : string.Empty;
                    campaignType = lead.GetAttributeValue<string>("new_campaigntype") != null ? lead.GetAttributeValue<string>("new_campaigntype") : string.Empty;
                    area = lead.GetAttributeValue<string>("new_selectedareas") != null ? CommaSeparation(lead.GetAttributeValue<string>("new_selectedareas")) : string.Empty;
                    region = lead.GetAttributeValue<string>("new_selectedregions") != null ? CommaSeparation(lead.GetAttributeValue<string>("new_selectedregions")) : string.Empty;
                    subSegment = lead.GetAttributeValue<string>("new_selectedsubsegments") != null ? CommaSeparation(lead.GetAttributeValue<string>("new_selectedsubsegments")) : string.Empty;
                    smcType = lead.GetAttributeValue<string>("new_selectedsmctype") != null ? CommaSeparation(lead.GetAttributeValue<string>("new_selectedsmctype")) : string.Empty;
                    month = lead.GetAttributeValue<string>("new_selectedmonths") != null ? CommaSeparation(lead.GetAttributeValue<string>("new_selectedmonths")) : string.Empty;
                    quarter = lead.GetAttributeValue<string>("new_selectedquarters") != null ? CommaSeparation(lead.GetAttributeValue<string>("new_selectedquarters")) : string.Empty;
                    licenceProgram = lead.GetAttributeValue<string>("new_selectedlicenseprograms") != null ? CommaSeparation(lead.GetAttributeValue<string>("new_selectedlicenseprograms")) : string.Empty;
                    revenueRange = lead.GetAttributeValue<string>("new_selectedrevenueranges") != null ? CommaSeparation(lead.GetAttributeValue<string>("new_selectedrevenueranges")) : string.Empty;
                    customerRevenueRange = lead.GetAttributeValue<string>("new_selectedcustomerrevenueranges") != null ? CommaSeparation(lead.GetAttributeValue<string>("new_selectedcustomerrevenueranges")) : string.Empty;
                    selectedClasPropensity = lead.GetAttributeValue<string>("new_selectedclaspropensity") != null ? CommaSeparation(lead.GetAttributeValue<string>("new_selectedclaspropensity")) : string.Empty;
                    preApproveMSXUpload = lead.GetAttributeValue<OptionSetValue>("new_preapproveformsxupload") != null ? lead.FormattedValues["new_preapproveformsxupload"] : string.Empty;
                    MergeMultipleLeadsForSameCustomer = lead.GetAttributeValue<OptionSetValue>("new_mergemultipleleadsforsamecustomer") != null ? lead.FormattedValues["new_mergemultipleleadsforsamecustomer"] : string.Empty;
                    autoLeadCreationRequestId = Convert.ToString(lead.Id);
                    SelectedABMAccountFlags = lead.GetAttributeValue<string>("new_selectedabmaccountflags") != null ? CommaSeparation(lead.GetAttributeValue<string>("new_selectedabmaccountflags")) : string.Empty;
                    SelectedAgreementProfiles = lead.GetAttributeValue<string>("new_selectedagreementprofiles") != null ? CommaSeparation(lead.GetAttributeValue<string>("new_selectedagreementprofiles")) : string.Empty;
                    SelectedAgreementProfileDetails = lead.GetAttributeValue<string>("new_selectedagreementprofiledetails") != null ? CommaSeparation(lead.GetAttributeValue<string>("new_selectedagreementprofiledetails")) : string.Empty;
                    SelectedCustomerProfiles = lead.GetAttributeValue<string>("new_selectedcustomerprofiles") != null ? CommaSeparation(lead.GetAttributeValue<string>("new_selectedcustomerprofiles")) : string.Empty;
                    SelectedCustomerProfileDetails = lead.GetAttributeValue<string>("new_selectedcustomerprofiledetails") != null ? CommaSeparation(lead.GetAttributeValue<string>("new_selectedcustomerprofiledetails")) : string.Empty;
                    ExtendedTimeframe = lead.GetAttributeValue<OptionSetValue>("new_extendedtimeframeforadditionalexpirations") != null ? lead.FormattedValues["new_extendedtimeframeforadditionalexpirations"] : string.Empty;
                    SelectedVLAllocationFlags = lead.GetAttributeValue<string>("new_selectedpotentialvlallocationflags") != null ? CommaSeparation(lead.GetAttributeValue<string>("new_selectedpotentialvlallocationflags")) : string.Empty;
                    SelectedMALDomainFlags = lead.GetAttributeValue<string>("new_selectedmaldomainflags") != null ? CommaSeparation(lead.GetAttributeValue<string>("new_selectedmaldomainflags")) : string.Empty;
                    SelectedMALNameMatchFlags = lead.GetAttributeValue<string>("new_selectedmalnamematchflags") != null ? CommaSeparation(lead.GetAttributeValue<string>("new_selectedmalnamematchflags")) : string.Empty;
                    SelectedTPIDs = lead.GetAttributeValue<string>("new_selectedtpids") != null ? lead.GetAttributeValue<string>("new_selectedtpids").Replace("; ", ",") : string.Empty;
                    SelectedAgreementIDs = lead.GetAttributeValue<string>("new_selectedagreementids") != null ? lead.GetAttributeValue<string>("new_selectedagreementids").Replace("; ", ",") : string.Empty;
                    Vendor = lead.GetAttributeValue<EntityReference>("new_vendor") != null ? lead.GetAttributeValue<EntityReference>("new_vendor").Name : null;
                    leadCreationRequestType = lead.GetAttributeValue<OptionSetValue>("new_leadcreationrequesttype") != null ? lead.GetAttributeValue<OptionSetValue>("new_leadcreationrequesttype").Value : 0;
                    leadAssignmentAgentDetails = lead.GetAttributeValue<string>("new_leadassignmentagentdetails") != null ? lead.GetAttributeValue<string>("new_leadassignmentagentdetails") : string.Empty;
                    SelectedOrgSizes = lead.GetAttributeValue<string>("new_selectedorgsizes") != null ? CommaSeparation(lead.GetAttributeValue<string>("new_selectedorgsizes")) : string.Empty;
                    SelectedProducts = lead.GetAttributeValue<string>("new_selectedproducts") != null ? CommaSeparation(lead.GetAttributeValue<string>("new_selectedproducts")) : string.Empty;
                }

                if (leadCreationRequestType == 100000002)
                {
                    List<AutoLeadAssignmentDetails> leadDetailsList = new List<AutoLeadAssignmentDetails>();
                    if (!string.IsNullOrWhiteSpace(leadAssignmentAgentDetails))
                    {
                        List<string> subsidiaryDetails = new List<string>();
                        List<string> userDetails = new List<string>();
                        var leadDetails = leadAssignmentAgentDetails.Split(new string[] { "##" }, StringSplitOptions.None);
                        foreach (var e in leadAssignmentAgentDetails.Split(new string[] { "##" }, StringSplitOptions.None))
                        {
                            AutoLeadAssignmentDetails leadAssign = new AutoLeadAssignmentDetails();

                            leadAssign.alias = e.Split('#')[0];
                            leadAssign.leadCount = Convert.ToInt32(e.Split('>')[0].Split('#')[1]);
                            if (e.Split('>')[1].Split(';').Length > 1)
                            {
                                foreach (var f in e.Split('>')[1].Split(';'))
                                {
                                    AutoLeadAssignmentDetails leadAssignMultiple = new AutoLeadAssignmentDetails();
                                    leadAssignMultiple.alias = e.Split('#')[0];
                                    leadAssignMultiple.leadCount = Convert.ToInt32(e.Split('>')[0].Split('#')[1]);
                                    leadAssignMultiple.subsidiary = f.Split('#')[0];
                                    leadAssignMultiple.leadCountSubsidiary = Convert.ToInt32(f.Split('#')[1]);
                                    leadDetailsList.Add(leadAssignMultiple);

                                }
                            }
                            else
                            {
                                leadAssign.subsidiary = e.Split('>')[1].Split('#')[0];
                                leadAssign.leadCountSubsidiary = Convert.ToInt32(e.Split('>')[1].Split('#')[1]);
                                leadDetailsList.Add(leadAssign);
                            }
                        }

                        DataTable _dtNymeriaLeadCreationRequest = new DataTable();
                        _dtNymeriaLeadCreationRequest.Columns.Add("AutoLeadCreationRequestId", typeof(Guid));
                        _dtNymeriaLeadCreationRequest.Columns.Add("MarketingListID", typeof(Guid));
                        _dtNymeriaLeadCreationRequest.Columns.Add("SelectedAreas", typeof(string));
                        _dtNymeriaLeadCreationRequest.Columns.Add("SelectedRegions", typeof(string));
                        _dtNymeriaLeadCreationRequest.Columns.Add("SelectedSubsidiaries", typeof(string));
                        _dtNymeriaLeadCreationRequest.Columns.Add("SelectedOrgSizes", typeof(string));
                        _dtNymeriaLeadCreationRequest.Columns.Add("SelectedProducts", typeof(string));
                        _dtNymeriaLeadCreationRequest.Columns.Add("VendorName", typeof(string));
                        _dtNymeriaLeadCreationRequest.Columns.Add("CampaignName", typeof(string));
                        _dtNymeriaLeadCreationRequest.Columns.Add("LeadAgentAssignmentDetails", typeof(string));
                        _dtNymeriaLeadCreationRequest.Columns.Add("Status", typeof(string));
                        _dtNymeriaLeadCreationRequest.Columns.Add("CreatedDateTime", typeof(DateTime));
                        _dtNymeriaLeadCreationRequest.Columns.Add("NymeriaProcessStartTime", typeof(DateTime));
                        _dtNymeriaLeadCreationRequest.Columns.Add("NymeriaProcessCompleteTime", typeof(DateTime));
                        _dtNymeriaLeadCreationRequest.Columns.Add("VTCPLeadsReceivedDateTime", typeof(DateTime));
                        _dtNymeriaLeadCreationRequest.Columns.Add("VTCPLeadUploadCompleteDateTime", typeof(DateTime));

                        DataRow drNymeriaLeadRow = _dtNymeriaLeadCreationRequest.NewRow();
                        drNymeriaLeadRow["AutoLeadCreationRequestId"] = new Guid(autoLeadCreationRequestId);
                        drNymeriaLeadRow["MarketingListID"] = new Guid(marketingList);
                        drNymeriaLeadRow["SelectedAreas"] = area;
                        drNymeriaLeadRow["SelectedRegions"] = region;
                        drNymeriaLeadRow["SelectedSubsidiaries"] = subsidiary;
                        drNymeriaLeadRow["SelectedOrgSizes"] = SelectedOrgSizes;
                        drNymeriaLeadRow["SelectedProducts"] = SelectedProducts;
                        drNymeriaLeadRow["VendorName"] = Vendor;
                        drNymeriaLeadRow["CampaignName"] = campaignType;
                        drNymeriaLeadRow["LeadAgentAssignmentDetails"] = leadAssignmentAgentDetails;
                        drNymeriaLeadRow["Status"] = "New";
                        drNymeriaLeadRow["CreatedDateTime"] = DateTime.UtcNow;
                        drNymeriaLeadRow["NymeriaProcessStartTime"] = DBNull.Value;
                        drNymeriaLeadRow["NymeriaProcessCompleteTime"] = DBNull.Value;
                        drNymeriaLeadRow["VTCPLeadsReceivedDateTime"] = DBNull.Value;
                        drNymeriaLeadRow["VTCPLeadUploadCompleteDateTime"] = DBNull.Value;

                        _dtNymeriaLeadCreationRequest.Rows.Add(drNymeriaLeadRow);
                       
                        string nymeriaTableName = _nymeriaSchema + ".[LeadCreationRequests]";
                        _manager.InsertDataIntoSQLDatabase(nymeriaTableName, _dtNymeriaLeadCreationRequest,_nymeriaSqlConnection,_nymeriaSchema);

                        int i = 0;
                        DataTable _dtLeadAssignDetails = new DataTable();
                        _dtLeadAssignDetails.Columns.Add("AutoLeadCreationRequestId", typeof(Guid));
                        _dtLeadAssignDetails.Columns.Add("Alias", typeof(string));
                        _dtLeadAssignDetails.Columns.Add("MaxLeads", typeof(int));
                        _dtLeadAssignDetails.Columns.Add("Subsidiary", typeof(string));
                        _dtLeadAssignDetails.Columns.Add("MaxLeadsSubsidiary", typeof(int));
                        _dtLeadAssignDetails.Columns.Add("AssignedLeadsTotal", typeof(int));
                        _dtLeadAssignDetails.Columns.Add("AssignedLeadsSubsidiary", typeof(int));

                        foreach (var a in leadDetailsList)
                        {
                            i++;
                            DataRow drLeadAssign = _dtLeadAssignDetails.NewRow();
                            drLeadAssign["AutoLeadCreationRequestId"] =new Guid(autoLeadCreationRequestId);
                            drLeadAssign["Alias"] = a.alias;
                            drLeadAssign["MaxLeads"] = a.leadCount;
                            drLeadAssign["Subsidiary"] = a.subsidiary;
                            drLeadAssign["MaxLeadsSubsidiary"] = a.leadCountSubsidiary;
                            drLeadAssign["AssignedLeadsTotal"] = 0;
                            drLeadAssign["AssignedLeadsSubsidiary"] = 0;
                            _dtLeadAssignDetails.Rows.Add(drLeadAssign);
                            if (i == leadDetailsList.Count)
                            {
                                string tableName =_nymeriaSchema + ".[AgentAssignmentDetails]";
                                _manager.InsertDataIntoSQLDatabase(tableName, _dtLeadAssignDetails,_nymeriaSqlConnection,_nymeriaSchema);
                            }
                        }
                        Entity objAutoLeadEntity = new Entity("new_autoleadcreationrequest");
                        objAutoLeadEntity.Id = new Guid(autoLeadCreationRequestId);
                        objAutoLeadEntity["new_nymeriarequestsenttodb"] = new OptionSetValue(100000000);
                        _orgService.Update(objAutoLeadEntity);
                    }
                }
                else if (leadCreationRequestType == 100000000)
                {
                    StringBuilder CTEQuery = new StringBuilder("; WITH CTE_AgreementSplit AS(SELECT VALUE AS AgreementIDSplit FROM STRING_SPLIT('" + SelectedAgreementIDs + "', ',')) , CTE_TPIDSplit AS (SELECT VALUE AS TPIDSplit FROM STRING_SPLIT('" + SelectedTPIDs + "',',')) ");

                    StringBuilder BaseAgreementListQuery = new StringBuilder(" SELECT 'Yes' AS IsAutoCreatedLead, '" + preApproveMSXUpload + "' AS PreApproveForMSXUpload,'" + MergeMultipleLeadsForSameCustomer + "' AS MergeMultipleLeadsForSameCustomer, '" + marketingList + "' AS MarketingListId,'" + autoLeadCreationRequestId + "' AS AutoLeadCreationRequestId,AgreementID, AgreementEndDate AS AgreementExpirationDate, null as CityName, SubsidiaryName, CustomerTPIDAggr, CustomerTPName, AccountNumber, AdvisorTPName,ResellerTPName, DistributorTPName, LicenseProgramName, CASE WHEN AgreementProfile IN('Perpetual License Only') THEN 'License Conversion - ' + CustomerTPName + ' | ' + AgreementID + ' (' + LicenseProgramName + ')' ELSE 'Renewal - ' + CustomerTPName + ' | ' + AgreementID + ' (' + LicenseProgramName + ')' END AS Topic, SUM(ExpAmountAnnualised ) AS ExpiringAmountTotal, STRING_AGG(' - ' + RevSumDivisionName + ' (' + UserType + ') ' + FORMAT(ExpAmountAnnualised , 'C'), CHAR(10)) WITHIN GROUP ( ORDER BY UserType ASC ) ExpiringProductDetails,AgreementProfile,AgreementProfileSortOrder FROM [ExpiringAgreements_Consolidated](NOLOCK) " +
       "e inner join CTE_TPIDSplit a on e.CustomerTPIDAggr=a.TPIDSplit inner join CTE_AgreementSplit s on e.AgreementID = s.AgreementIDSplit " +
        "WHERE ExclusionFlag = 'No'");
                    StringBuilder BaseAgreementListExtendedQuery = new StringBuilder(" SELECT 'Yes' AS IsAutoCreatedLead, '" + preApproveMSXUpload + "' AS PreApproveForMSXUpload,'" + MergeMultipleLeadsForSameCustomer + "' AS MergeMultipleLeadsForSameCustomer, '" + marketingList + "' AS MarketingListId,'" + autoLeadCreationRequestId + "' AS AutoLeadCreationRequestId,AgreementID, AgreementEndDate AS AgreementExpirationDate, null as CityName, SubsidiaryName, CustomerTPIDAggr, CustomerTPName, AccountNumber, AdvisorTPName,ResellerTPName, DistributorTPName, LicenseProgramName, CASE WHEN AgreementProfile IN('Perpetual License Only') THEN 'License Conversion - ' + CustomerTPName + ' | ' + AgreementID + ' (' + LicenseProgramName + ')' ELSE 'Renewal - ' + CustomerTPName + ' | ' + AgreementID + ' (' + LicenseProgramName + ')' END AS Topic, SUM(ExpAmountAnnualised ) AS ExpiringAmountTotal, STRING_AGG(' - ' + RevSumDivisionName + ' (' + UserType + ') ' + FORMAT(ExpAmountAnnualised , 'C'), CHAR(10)) WITHIN GROUP ( ORDER BY UserType ASC ) ExpiringProductDetails,AgreementProfile,AgreementProfileSortOrder FROM [ExpiringAgreements_Consolidated](NOLOCK) " +
                      "e inner join CTE_TPIDSplit a on e.CustomerTPIDAggr=a.TPIDSplit " +
                       "WHERE ExclusionFlag = 'No'");

                    if (licenceProgram != "'All'") { BaseAgreementListExtendedQuery.Append(" AND LicenseProgramName in (" + licenceProgram + ")"); }
                    if (revenueRange != "'All'") { BaseAgreementListExtendedQuery.Append(" AND AgreementRevenueRange in (" + revenueRange + ")"); }
                    if (SelectedAgreementProfiles != "'All'") { BaseAgreementListExtendedQuery.Append(" AND AgreementProfile in (" + SelectedAgreementProfiles + ")"); }
                    if (SelectedAgreementProfileDetails != "'All'") { BaseAgreementListExtendedQuery.Append(" AND AgreementProfileDetail in (" + SelectedAgreementProfileDetails + ")"); }

                    StringBuilder GroupByQuery = new StringBuilder(" GROUP BY AgreementID, AgreementEndDate,SubsidiaryName, CustomerTPIDAggr, CustomerTPName, AccountNumber, AdvisorTPName, ResellerTPName,DistributorTPName, LicenseProgramName, AgreementProfile,AgreementProfileSortOrder");

                    StringBuilder DynamicDateFilters = new StringBuilder();
                    //if (month != "'All'") { DynamicDateFilters.Append(" AND ExpirationMonth in (" + month + ")"); }
                    //if (quarter != "'All'") { DynamicDateFilters.Append(" AND ExpirationQuarter in (" + quarter + ")"); }
                    //if (SelectedAgreementIDs != "'All'") { DynamicDateFilters.Append(" AND AgreementID in (" + SelectedAgreementIDs + ")"); }

                    if (!string.IsNullOrEmpty(ExtendedTimeframe) && ExtendedTimeframe != "0")
                    {
                        StringBuilder AdditionalAgreements = new StringBuilder(", UniqueTPIDs AS( SELECT CustomerTPIDAggr, MAX(AgreementExpirationDate) AS LatestAgreementDate FROM BaseAgreementList GROUP BY CustomerTPIDAggr)");

                        AdditionalAgreements.Append(" , AdditionalAgreements as (" + BaseAgreementListExtendedQuery);
                        AdditionalAgreements.Append(" AND CustomerTPIDAggr IN (SELECT CustomerTPIDAggr FROM UniqueTPIDs) AND AgreementEndDate > (SELECT MAX(AgreementExpirationDate) FROM BaseAgreementList) AND AgreementEndDate <= (SELECT EOMONTH(DATEADD(month, " + ExtendedTimeframe + ", MAX(AgreementExpirationDate))) FROM BaseAgreementList)");
                        AdditionalAgreements.Append(GroupByQuery + ")");


                        FullQuery = " , BaseAgreementList as (" + BaseAgreementListQuery.ToString() + DynamicDateFilters.ToString() + GroupByQuery.ToString() + ")" + AdditionalAgreements.ToString();
                        InsertQuery = new StringBuilder(CTEQuery + FullQuery + " INSERT INTO [dbo].[SSIS_Output_LeadCreation] (IsAutoCreatedLead,PreApproveForMSXUpload,MergeMultipleLeadsForSameCustomer,MarketingListId,AutoLeadCreationRequestId,AgreementID,AgreementExpirationDate,CityName,SubsidiaryName,CustomerTPIDAggr,CustomerTPName, AccountNumber, AdvisorTPName, ResellerTPName, DistributorTPName, LicenseProgramName, Topic, ExpiringAmountTotal, ExpiringProducts,AgreementProfile,AgreementProfileSortOrder) (SELECT * FROM BaseAgreementList UNION ALL SELECT * FROM AdditionalAgreements)");

                    }
                    else
                    {
                        FullQuery = BaseAgreementListQuery.ToString();
                        InsertQuery = new StringBuilder(CTEQuery + " INSERT INTO [dbo].[SSIS_Output_LeadCreation] (IsAutoCreatedLead,PreApproveForMSXUpload,MergeMultipleLeadsForSameCustomer,MarketingListId,AutoLeadCreationRequestId,AgreementID,AgreementExpirationDate,CityName,SubsidiaryName,CustomerTPIDAggr,CustomerTPName, AccountNumber, AdvisorTPName, ResellerTPName, DistributorTPName, LicenseProgramName, Topic, ExpiringAmountTotal, ExpiringProducts,AgreementProfile,AgreementProfileSortOrder) (" + BaseAgreementListQuery + DynamicDateFilters.ToString() + GroupByQuery.ToString() + ")");
                    }
                }
                else if (leadCreationRequestType == 100000001)
                {
                    StringBuilder EABaseAgreementListQuery = new StringBuilder(" SELECT 'Yes' AS IsAutoCreatedLead, '" + preApproveMSXUpload + "' AS PreApproveForMSXUpload,'" + MergeMultipleLeadsForSameCustomer + "' AS MergeMultipleLeadsForSameCustomer, '" + marketingList + "' AS MarketingListId,'" + autoLeadCreationRequestId + "' AS AutoLeadCreationRequestId,AgreementID, AgreementEndDate AS AgreementExpirationDate, null as CityName, SubsidiaryName, CustomerTPIDAggr, CustomerTPName, AccountNumber, AdvisorTPName,ResellerTPName, DistributorTPName, LicenseProgramName, CASE WHEN AgreementProfile IN('Perpetual License Only') THEN 'License Conversion - ' + CustomerTPName + ' | ' + AgreementID + ' (' + LicenseProgramName + ')' ELSE 'Renewal - ' + CustomerTPName + ' | ' + AgreementID + ' (' + LicenseProgramName + ')' END AS Topic, SUM(ExpAmountAnnualised ) AS ExpiringAmountTotal, STRING_AGG(' - ' + RevSumDivisionName + ' (' + UserType + ') ' + FORMAT(ExpAmountAnnualised , 'C'), CHAR(10)) WITHIN GROUP ( ORDER BY UserType ASC ) ExpiringProductDetails,AgreementProfile,AgreementProfileSortOrder FROM [ExpiringAgreements_Consolidated](NOLOCK) " +
                      "e WHERE ExclusionFlag = 'No' AND PotentialCampaign = 'EA Renewals' AND AssignedVendor = '" + Vendor + "' AND MSXOpportunityNumber IS NOT NULL AND AgreementEndDate >= GETUTCDATE() AND AgreementEndDate <= EOMONTH(DATEADD(Year,1,GETUTCDATE()))");

                    StringBuilder EAGroupByQuery = new StringBuilder(" GROUP BY AgreementID, AgreementEndDate,SubsidiaryName, CustomerTPIDAggr, CustomerTPName, AccountNumber, AdvisorTPName, ResellerTPName,DistributorTPName, LicenseProgramName, AgreementProfile,AgreementProfileSortOrder");

                    InsertQuery = new StringBuilder(" INSERT INTO [dbo].[SSIS_Output_LeadCreation] (IsAutoCreatedLead,PreApproveForMSXUpload,MergeMultipleLeadsForSameCustomer,MarketingListId,AutoLeadCreationRequestId,AgreementID,AgreementExpirationDate,CityName,SubsidiaryName,CustomerTPIDAggr,CustomerTPName, AccountNumber, AdvisorTPName, ResellerTPName, DistributorTPName, LicenseProgramName, Topic, ExpiringAmountTotal, ExpiringProducts,AgreementProfile,AgreementProfileSortOrder) (" + EABaseAgreementListQuery + EAGroupByQuery.ToString() + ")");
                }

                _log.LogInformation(InsertQuery.ToString());
                if (leadCreationRequestType != 100000002)
                {
                    //Insert the Dynamically generated Query into DB
                    DataTable _dtLeadCreation = new DataTable();
                    _dtLeadCreation.Columns.Add("Query");
                    _dtLeadCreation.Columns.Add("MarketingListId");
                    _dtLeadCreation.Columns.Add("AutoLeadCreationRequestId");
                    DataRow _drLeadCreation = _dtLeadCreation.NewRow();
                    _drLeadCreation["Query"] = InsertQuery.ToString();
                    _drLeadCreation["MarketingListId"] = marketingList;
                    _drLeadCreation["AutoLeadCreationRequestId"] = autoLeadCreationRequestId;
                    _dtLeadCreation.Rows.Add(_drLeadCreation);
                    _manager.InsertDataIntoSQLDatabase("SSIS_Staging_LeadCreation", _dtLeadCreation);

                    //Deactivate the processed AutoLead creation request
                    Entity objLeadEntity = new Entity("new_autoleadcreationrequest");
                    objLeadEntity.Id = leadCreationcoll.Entities[0].Id;
                    objLeadEntity["statecode"] = new OptionSetValue(0);
                    objLeadEntity["statuscode"] = new OptionSetValue(100000001);
                    _orgService.Update(objLeadEntity);
                }
            }
            DateTime EndTime = DateTime.UtcNow;
            SqlConnection con = new SqlConnection(AutoLeadCreationManager._sqlConnection);
            //AuthenticationResult authenticationResult = _manager.AADAunthenticationResult();
            con.AccessToken = Manager.getAccessToken(System.Environment.GetEnvironmentVariable("ResourceId"), System.Environment.GetEnvironmentVariable("VTCPManagedIdentity"), true).Result;
            SqlCommand SqlCommands = new SqlCommand();
            SqlCommands = new SqlCommand("AzureFunctionsDataLoad", con);
            SqlCommands.CommandType = CommandType.StoredProcedure;
            SqlCommands.Parameters.AddWithValue("@FunctionName", "LeadCreationInput");
            SqlCommands.Parameters.AddWithValue("@ProcessStartTime", StartTime.ToString());
            SqlCommands.Parameters.AddWithValue("@FunctionStartTime", StartTime.ToString());
            SqlCommands.Parameters.AddWithValue("@FunctionEndTime", EndTime.ToString());
            SqlCommands.Parameters.AddWithValue("@InitialRowsAffected", leadCreationcoll.Entities.Count.ToString());
            SqlCommands.Parameters.AddWithValue("@TotalRowsAffected", leadCreationcoll.Entities.Count.ToString());
            con.Open();
            SqlCommands.ExecuteNonQuery();
            con.Close();
        }

        /// <summary>
        /// Method to spilt and filters to query
        /// </summary>
        /// <param name="filter"></param>
        /// <returns></returns>
        public string CommaSeparation(string filter)
        {
            var list = filter.Split(';');
            string strConcat = "";
            foreach (string strfilter in list)
            {
                strConcat += "'" + strfilter.TrimStart() + "',";
            }
            return strConcat.TrimEnd(',');
        }

        /// <summary>
        /// Method Concatnate Month
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public string SelectedMonth(string name)
        {
            string strConcat = string.Empty;
            if (name != "All")
            {
                var list = Regex.Matches(name, "([^,]*,[^,]*)(?:, |$)").Cast<Match>().Select(m => m.Groups[1].Value).ToArray();
                foreach (string strFilter in list)
                {
                    strConcat += "'" + strFilter.TrimStart() + "',";
                }
            }
            else
            {
                strConcat = name;
            }
            return strConcat.TrimEnd(',');
        }

        /// <summary>
        /// Method to get Entity collection
        /// </summary>
        /// <param name="service"></param>
        /// <param name="entityName"></param>
        /// <returns></returns>
        public EntityCollection GetEntityCollection(IOrganizationService service, string entityName)
        {
            EntityCollection objEntityColl = new EntityCollection();
            if (entityName != null)
            {
                QueryExpression queryExpression = new QueryExpression()
                {
                    EntityName = entityName,
                    ColumnSet = new ColumnSet(true),
                    Criteria = new FilterExpression()
                };
                objEntityColl = service.RetrieveMultiple(queryExpression);
            }
            return objEntityColl;
        }
        public EntityCollection GetEntityCollectionName(IOrganizationService service, string entityName, string fieldValue, string fieldName)
        {
            EntityCollection objEntityColl = new EntityCollection();
            if (entityName != null)
            {
                QueryExpression queryExpression = new QueryExpression()
                {
                    EntityName = entityName,
                    ColumnSet = new ColumnSet(true),
                    Criteria = new FilterExpression()
                };

                queryExpression.Criteria.AddCondition(fieldName, ConditionOperator.Equal, fieldValue);
                objEntityColl = service.RetrieveMultiple(queryExpression);
            }
            return objEntityColl;
        }

        /// <summary>
        /// Method to delete records from database
        /// </summary>
        /// <param name="idList"></param>
        /// <param name="tableName"></param>
        public void DeleteRecordsInDatabase(List<AutoLeadCreation> idList, string tableName)
        {
            SqlConnection conn = new SqlConnection(_sqlConnection);
            //AuthenticationResult authenticationResult = _manager.AADAunthenticationResult();
            conn.AccessToken = Manager.getAccessToken(System.Environment.GetEnvironmentVariable("ResourceId"), System.Environment.GetEnvironmentVariable("VTCPManagedIdentity"), true).Result;
            conn.Open();
            string IdValue = string.Empty;
            SqlCommand cmd = null;
            foreach (var Id in idList)
            {
                IdValue += Id.Id + ",";
            }
            string Finalvalue = IdValue.TrimEnd(',');
            cmd = new SqlCommand("Delete from " + tableName + " where Id in(" + Finalvalue + ")", conn);
            cmd.ExecuteNonQuery();
            conn.Close();
        }
        /// <summary>
        /// Method to Update # Leads Pending Creation
        /// </summary>
        public void UpdatePendingLeadCreation()
        {
            QueryExpression UpdatePendingLead = new QueryExpression()
            {
                EntityName = "new_autoleadcreationrequest",
                ColumnSet = new ColumnSet(true),
                Criteria = new FilterExpression()
            };
            UpdatePendingLead.Criteria.AddCondition("statecode", ConditionOperator.Equal, Convert.ToInt32(0));
            UpdatePendingLead.Criteria.AddCondition("new_marketinglist", ConditionOperator.NotNull);
            UpdatePendingLead.Criteria.AddCondition("new_leadsawaitingcreation", ConditionOperator.Equal, "TBD");

            EntityCollection AutoLeadCreationColl = _orgService.RetrieveMultiple(UpdatePendingLead);

            if (AutoLeadCreationColl != null && AutoLeadCreationColl.Entities.Count > 0)
            {
                foreach (Entity Autolead in AutoLeadCreationColl.Entities)
                {
                    string marketingList = Autolead.GetAttributeValue<EntityReference>("new_marketinglist") != null ? Autolead.GetAttributeValue<EntityReference>("new_marketinglist").Id.ToString() : string.Empty;

                    string retrieveCount = "select count(1) as Count from [dbo].[SSIS_Output_LeadCreation_history] nolock  where MarketingListId= '" + marketingList + "'";
                    DataTable dtleadoutputhistorycount = _manager.RetrieveDatafromSQLDatabase(retrieveCount);
                    int TotalRecordCount = Convert.ToInt32(dtleadoutputhistorycount.Rows[0].ItemArray[0]);

                    if (TotalRecordCount > 0)
                    {
                        Entity AutoleadCreation = new Entity("new_autoleadcreationrequest");
                        AutoleadCreation.Id = Autolead.Id;
                        AutoleadCreation["new_leadsawaitingcreation"] = Convert.ToString(TotalRecordCount);
                        _orgService.Update(AutoleadCreation);
                    }

                }
            }
        }

        /// <summary>
        /// Class for AutoLeadCreation details
        /// </summary>
        public class AutoLeadCreation
        {
            public long Id;
        }

        /// <summary>
        /// Class for Subsidiary details
        /// </summary>
        public class SubsidiaryItem
        {
            public string id;
            public string subsidiaryname;
        }
        public class AutoLeadAssignmentDetails
        {
            public string alias;
            public string subsidiary;
            public int leadCount;
            public int leadCountSubsidiary;

        }
        #endregion
        #region EA Auto Lead
        public void MonthlyAutoEALeadCreationInput()
        {
            DateTime StartTime = DateTime.UtcNow;

            List<string> campaignList = new List<string>();
            string vendor = string.Empty;
            Entity autoLeadEA = new Entity("new_autoleadcreationrequest");
            ExecuteMultipleSettings excuteMultipleSettings = new ExecuteMultipleSettings();
            excuteMultipleSettings.ContinueOnError = true;
            excuteMultipleSettings.ReturnResponses = true;

            ExecuteMultipleRequest createLeadsRequest = new ExecuteMultipleRequest();
            createLeadsRequest.Requests = new OrganizationRequestCollection();
            createLeadsRequest.Settings = excuteMultipleSettings;

            campaignList.Add("EA Renewals");
            campaignList.Add("EA Year 3 True Up");
            string retrieveQuery = "select Distinct AssignedVendor from [dbo].[ExpiringAgreements_Consolidated] nolock Where AssignedVendor is not null and AssignedVendor <> 'N/A'";
            DataTable dtleadoutput = _manager.RetrieveDatafromSQLDatabase(retrieveQuery);

            foreach (DataRow v in dtleadoutput.Rows)
            {
                autoLeadEA = new Entity("new_autoleadcreationrequest");
                vendor = Convert.ToString(v["AssignedVendor"]);
                if (!string.IsNullOrWhiteSpace(vendor))
                {
                    foreach (var c in campaignList)
                    {
                        autoLeadEA["new_campaigntype"] = c;
                        autoLeadEA["new_vendor"] = new EntityReference("team", GetEntityCollectionName(_orgService, "team", vendor, "name").Entities[0].Id);
                        autoLeadEA["new_preapproveformsxupload"] = new OptionSetValue(Convert.ToInt32(100000000));
                        autoLeadEA["new_mergemultipleleadsforsamecustomer"] = new OptionSetValue(Convert.ToInt32(100000001));
                        autoLeadEA["new_leadcreationrequesttype"] = new OptionSetValue(Convert.ToInt32(100000001));

                        CreateRequest autoLeadEARequest = new CreateRequest();
                        autoLeadEARequest.Target = autoLeadEA;
                        createLeadsRequest.Requests.Add(autoLeadEARequest);
                    }
                }

            }

            _manager.ExecuteBulkRequest(createLeadsRequest, "Created Auto Lead EA Details", true);
            DateTime EndTime = DateTime.UtcNow;
            SqlConnection con = new SqlConnection(AutoLeadCreationManager._sqlConnection);
            //AuthenticationResult authenticationResult = _manager.AADAunthenticationResult();
            con.AccessToken = Manager.getAccessToken(System.Environment.GetEnvironmentVariable("ResourceId"), System.Environment.GetEnvironmentVariable("VTCPManagedIdentity"), true).Result;
            SqlCommand SqlCommands = new SqlCommand();
            SqlCommands = new SqlCommand("AzureFunctionsDataLoad", con);
            SqlCommands.CommandType = CommandType.StoredProcedure;
            SqlCommands.Parameters.AddWithValue("@FunctionName", "MonthlyAutoEALeadCreationInput");
            SqlCommands.Parameters.AddWithValue("@ProcessStartTime", StartTime.ToString());
            SqlCommands.Parameters.AddWithValue("@FunctionStartTime", StartTime.ToString());
            SqlCommands.Parameters.AddWithValue("@FunctionEndTime", EndTime.ToString());
            SqlCommands.Parameters.AddWithValue("@InitialRowsAffected", _manager.TotalProcessedRecords.ToString());
            SqlCommands.Parameters.AddWithValue("@TotalRowsAffected", _manager.TotalProcessedRecords.ToString());
            con.Open();
            SqlCommands.ExecuteNonQuery();
            con.Close();
        }
        #endregion
    }
}
