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
using System.ServiceModel.Description;
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


using System.Runtime.InteropServices.ComTypes;
using System.Drawing;

namespace Helper
{
    public class VariableFeeManager
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
        //private const int MaxLength = 4000;
        private const string MalAccountIdMatch = "Matched By VTCP System";
        Manager _manager;

        public VariableFeeManager(string instancename, string clientId, string secret, string msxclientId, string msxsecret, string sqlconnection, string ssasServer, string blobConnection,string vtcpManagedIdentity, ILogger log)
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

        #region Supporting Roles Calculation
        public void SupportingRolesCalculationInput()
        {
            List<FilterExpression> filtersList = new List<FilterExpression>();
            DateTime StartTime = DateTime.UtcNow;
            DataTable _dtOptInDetails = new DataTable();
            Dictionary<string, int> IDictionary = ReturnMonthNumericalValue();
            int count = 0;
            string monthName = DateTime.Now.ToString("MMM");
            int month = 0;
            foreach (var m in IDictionary)
            {
                if (m.Key == monthName)
                {
                    month = m.Value;
                }

            }
            string year = DateTime.Now.Year.ToString();
            year = year.Substring(year.Length - 2);

            if (DateTime.Now.Month > 6)
            {
                year = DateTime.Now.AddYears(1).Year.ToString();
                year = year.Substring(year.Length - 2);
            }


            QueryExpression fetchOptDetails = new QueryExpression
            {
                EntityName = "new_resourcingtargetdetail",
                ColumnSet = new ColumnSet(true),
                Criteria = new FilterExpression()
            };

            FilterExpression filterMonth = new FilterExpression(LogicalOperator.Or);
            List<month> months = new List<month>();
            EntityCollection monthColl = new EntityCollection();
            monthColl = GetEntityCollection(_orgService, "new_month", string.Empty);
            foreach (var monthList in monthColl.Entities)
            {
                months.Add(new month
                {
                    monthId = monthList.GetAttributeValue<Guid>("new_monthid"),
                    monthName = monthList.GetAttributeValue<string>("new_name")
                });

            }
            if (month == 1)
            {
                year = (Convert.ToInt32(year) - 1).ToString();
                month = 12;
                foreach (var i in IDictionary)
                {
                    if (i.Value <= month)
                    {
                        foreach (var m in months)
                        {
                            if ("FY" + year + "-" + i.Key == m.monthName)
                            {
                                filterMonth.AddCondition("new_month", ConditionOperator.Equal, m.monthId);

                                break;
                            }
                        }
                    }
                }
            }
            else
            {
                foreach (var i in IDictionary)
                {
                    if (i.Value < month)
                    {
                        foreach (var m in months)
                        {
                            if ("FY" + year + "-" + i.Key == m.monthName)
                            {
                                filterMonth.AddCondition("new_month", ConditionOperator.Equal, m.monthId);

                                break;
                            }
                        }
                    }
                }
            }

            //fetchOptDetails.Criteria.AddFilter(filterMonth);
            filtersList.Add(filterMonth);

            QueryExpression fetchOptInModifiedDateDetails = new QueryExpression
            {
                EntityName = "new_resourcingtargetdetail",
                ColumnSet = new ColumnSet("modifiedon"),
                Criteria = new FilterExpression(),
                TopCount = 1
            };
            fetchOptInModifiedDateDetails.Criteria.AddCondition("modifiedon", ConditionOperator.LastXHours, 1);
            EntityCollection optInModifiedDateDetails = _orgService.RetrieveMultiple(fetchOptInModifiedDateDetails);
            if (optInModifiedDateDetails != null && optInModifiedDateDetails.Entities.Count > 0)
            {
                EntityCollection optInDetails = _manager.Retrieve5000PlusRecordsUsingQueryExpression("new_resourcingtargetdetail", true, null, filtersList, null);

                _dtOptInDetails.Columns.Add("OptInDetailID", typeof(Guid));
                _dtOptInDetails.Columns.Add("Program", typeof(string));
                _dtOptInDetails.Columns.Add("TargetDate", typeof(DateTime));
                _dtOptInDetails.Columns.Add("AreaName", typeof(string));
                _dtOptInDetails.Columns.Add("VendorName", typeof(string));
                _dtOptInDetails.Columns.Add("IsBillable", typeof(string));
                _dtOptInDetails.Columns.Add("IsBlended", typeof(string));
                _dtOptInDetails.Columns.Add("RoleDescription", typeof(string));
                _dtOptInDetails.Columns.Add("NumResources", typeof(decimal));
                _dtOptInDetails.Columns.Add("NumResourcesActual", typeof(decimal));
                _dtOptInDetails.Columns.Add("StaffedNonBillableRoles", typeof(decimal));
                _dtOptInDetails.Columns.Add("NonBillableRolesDetails", typeof(string));
                _dtOptInDetails.Columns.Add("ProcessStartTime", typeof(string));
                _dtOptInDetails.Columns.Add("AdjustForNonBillableRoles", typeof(string));
                EntityCollection MonthColl = GetAllMonthsInVTCP(_orgService);

                List<Entity> AllMonthlist = MonthColl.Entities.ToList().OrderBy(c => c.Attributes["createdon"]).ToList();
                DateTime MonthStartDate = DateTime.MinValue;

                foreach (Entity optInDetail in optInDetails.Entities)
                {
                    DataRow drOptIndetail = _dtOptInDetails.NewRow();


                    foreach (Entity Monthlist in AllMonthlist)
                    {
                        string OptInMonth = optInDetail.GetAttributeValue<EntityReference>("new_month") != null ? optInDetail.GetAttributeValue<EntityReference>("new_month").Name : string.Empty;
                        string MonthName = Monthlist.Contains("new_name") ? Monthlist.GetAttributeValue<string>("new_name") : string.Empty;

                        if (OptInMonth == MonthName)
                        {
                            MonthStartDate = Monthlist.GetAttributeValue<DateTime>("new_monthstartdate") != null ? Monthlist.GetAttributeValue<DateTime>("new_monthstartdate") : DateTime.MinValue;
                            break;
                        }
                    }

                    drOptIndetail["TargetDate"] = MonthStartDate;

                    drOptIndetail["AreaName"] = optInDetail.GetAttributeValue<EntityReference>("new_areaname") != null ? optInDetail.GetAttributeValue<EntityReference>("new_areaname").Name : null;

                    drOptIndetail["Program"] = optInDetail.GetAttributeValue<EntityReference>("new_programname") != null ? optInDetail.GetAttributeValue<EntityReference>("new_programname").Name : null;

                    drOptIndetail["OptInDetailID"] = optInDetail.Id;

                    drOptIndetail["IsBillable"] = optInDetail.GetAttributeValue<OptionSetValue>("new_isbillable") != null ? optInDetail.FormattedValues["new_isbillable"] : string.Empty;

                    drOptIndetail["IsBlended"] = optInDetail.GetAttributeValue<OptionSetValue>("new_resourcecosttype") != null ? optInDetail.FormattedValues["new_resourcecosttype"] : null;

                    drOptIndetail["RoleDescription"] = optInDetail.GetAttributeValue<EntityReference>("new_roledescription") != null ? optInDetail.GetAttributeValue<EntityReference>("new_roledescription").Name : null;

                    drOptIndetail["VendorName"] = optInDetail.GetAttributeValue<EntityReference>("new_vendorname") != null ? optInDetail.GetAttributeValue<EntityReference>("new_vendorname").Name : null;
                    drOptIndetail["NumResources"] = optInDetail.GetAttributeValue<decimal?>("new_salesagents") != null ? optInDetail.GetAttributeValue<decimal?>("new_salesagents") : (object)DBNull.Value;
                    drOptIndetail["NumResourcesActual"] = optInDetail.GetAttributeValue<decimal?>("new_actualsalesagents") != null ? optInDetail.GetAttributeValue<decimal?>("new_actualsalesagents") : (object)DBNull.Value;
                    _dtOptInDetails.Rows.Add(drOptIndetail);
                    drOptIndetail["StaffedNonBillableRoles"] = optInDetail.GetAttributeValue<decimal?>("new_staffednonbillableroles") != null ? optInDetail.GetAttributeValue<decimal?>("new_staffednonbillableroles") : (object)DBNull.Value;
                    drOptIndetail["NonBillableRolesDetails"] = optInDetail.Contains("new_nonbillablerolesdetails") ? optInDetail.GetAttributeValue<string>("new_nonbillablerolesdetails") : string.Empty;
                    drOptIndetail["ProcessStartTime"] = StartTime.ToString();
                    drOptIndetail["AdjustForNonBillableRoles"] = optInDetail.GetAttributeValue<OptionSetValue>("new_adjustfornonbillableroles") != null ? optInDetail.FormattedValues["new_adjustfornonbillableroles"] : null;

                    if (!string.IsNullOrWhiteSpace(drOptIndetail["AreaName"].ToString()))
                    {
                        if (drOptIndetail["AreaName"].ToString() == "Netherlands" || drOptIndetail["AreaName"].ToString() == "Switzerland")
                        {
                            drOptIndetail["AreaName"] = "Western Europe";
                        }
                    }
                    if (!string.IsNullOrWhiteSpace(drOptIndetail["Program"].ToString()))
                     {
                        if (drOptIndetail["Program"].ToString() == "EA Renewals" ||
                            drOptIndetail["Program"].ToString() == "SMB Vendor Tele" ||
                            drOptIndetail["Program"].ToString() == "Top Managed" ||
                            drOptIndetail["Program"].ToString() == "MW Grow")
                        {
                            drOptIndetail["Program"] = "SMB Vendor Tele";
                        }
                    }
                }
                string tableName = "[dbo].[SSIS_Staging_SupportingRolesCalculation]";
                _manager.InsertDataIntoSQLDatabase(tableName, _dtOptInDetails);
            }
            DateTime EndTime = DateTime.UtcNow;
            SqlConnection con = new SqlConnection(VariableFeeManager._sqlConnection);
            //AuthenticationResult authenticationResult = _manager.AADAunthenticationResult();
            con.AccessToken = Manager.getAccessToken(System.Environment.GetEnvironmentVariable("ResourceId"), System.Environment.GetEnvironmentVariable("VTCPManagedIdentity"), true).Result;
            SqlCommand SqlCommands = new SqlCommand();
            SqlCommands = new SqlCommand("AzureFunctionsDataLoad", con);
            SqlCommands.CommandType = CommandType.StoredProcedure;
            SqlCommands.Parameters.AddWithValue("@FunctionName", "SupportingRolesCalculationInput");
            SqlCommands.Parameters.AddWithValue("@ProcessStartTime", StartTime.ToString());
            SqlCommands.Parameters.AddWithValue("@FunctionStartTime", StartTime.ToString());
            SqlCommands.Parameters.AddWithValue("@FunctionEndTime", EndTime.ToString());
            SqlCommands.Parameters.AddWithValue("@InitialRowsAffected", _dtOptInDetails.Rows.Count.ToString());
            SqlCommands.Parameters.AddWithValue("@TotalRowsAffected", _dtOptInDetails.Rows.Count.ToString());
            con.Open();
            SqlCommands.ExecuteNonQuery();
            con.Close();
        }

        public void SupportingRolesCalculationOutput()
        {
            try
            {
                DateTime StartTime = DateTime.UtcNow;
                int batch = 0;
                List<OptionIndetails> DeleteOptDetails = new List<OptionIndetails>();
                ExecuteMultipleSettings excuteMultipleSettings = new ExecuteMultipleSettings();
                excuteMultipleSettings.ContinueOnError = true;
                excuteMultipleSettings.ReturnResponses = true;

                ExecuteMultipleRequest updateOptDetailRequest = new ExecuteMultipleRequest();
                updateOptDetailRequest.Requests = new OrganizationRequestCollection();
                updateOptDetailRequest.Settings = excuteMultipleSettings;

                string retrieveQuery = "select * from [dbo].[SSIS_Output_SupportingRolesCalculation]  (nolock)";
                DataTable dtOptInDetail = _manager.RetrieveDatafromSQLDatabase(retrieveQuery);
                string ProcessStartTime = string.Empty;
                if (dtOptInDetail != null && dtOptInDetail.Rows.Count > 0)
                {
                    foreach (DataRow row in dtOptInDetail.Rows)
                    {
                        batch++;
                        Guid OptInDetailID = (Guid)row["OptInDetailID"];
                        DeleteOptDetails.Add(new OptionIndetails
                        {
                            id = Convert.ToString(OptInDetailID)
                        });

                        Entity optInDetailEntity = new Entity("new_resourcingtargetdetail");
                        optInDetailEntity.Id = OptInDetailID;
                        ProcessStartTime = Convert.ToString(row["ProcessStartTime"]);
                        optInDetailEntity["new_staffednonbillableroles"] = row["%StaffedSupportingRoles"] != DBNull.Value ? Convert.ToDecimal(row["%StaffedSupportingRoles"]) : (decimal?)null;
                        optInDetailEntity["new_nonbillablerolesdetails"] = row["SupportingRolesDetail"] != null ? Convert.ToString(row["SupportingRolesDetail"]) : null;

                        var updateRequest = new UpdateRequest();
                        updateRequest.Target = optInDetailEntity;
                        updateOptDetailRequest.Requests.Add(updateRequest);

                        if (updateOptDetailRequest.Requests.Count == 100)
                        {
                            _manager.ExecuteBulkRequest(updateOptDetailRequest, "Updated SupportingRolesCalculation");
                            updateOptDetailRequest.Requests.Clear();
                            DeleteRecordsInDatabase(DeleteOptDetails, "SSIS_staging_SupportingRolesCalculation");
                            DeleteRecordsInDatabase(DeleteOptDetails, "SSIS_Output_SupportingRolesCalculation");
                            DeleteOptDetails.Clear();
                            batch = 0;
                        }
                    }
                    if (batch > 0)
                    {
                        _manager.ExecuteBulkRequest(updateOptDetailRequest, "Updated SupportingRolesCalculation");
                        updateOptDetailRequest.Requests.Clear();
                        DeleteRecordsInDatabase(DeleteOptDetails, "SSIS_staging_SupportingRolesCalculation");
                        DeleteRecordsInDatabase(DeleteOptDetails, "SSIS_Output_SupportingRolesCalculation");
                        DeleteOptDetails.Clear();
                        batch = 0;
                    }

                }
                DateTime EndTime = DateTime.UtcNow;
                SqlConnection con = new SqlConnection(VariableFeeManager._sqlConnection);
                //AuthenticationResult authenticationResult = _manager.AADAunthenticationResult();
                con.AccessToken = Manager.getAccessToken(System.Environment.GetEnvironmentVariable("ResourceId"), System.Environment.GetEnvironmentVariable("VTCPManagedIdentity"), true).Result;
                SqlCommand SqlCommands = new SqlCommand();
                SqlCommands = new SqlCommand("AzureFunctionsDataLoad", con);
                SqlCommands.CommandType = CommandType.StoredProcedure;
                SqlCommands.Parameters.AddWithValue("@FunctionName", "SupportingRolesCalculationOutput");
                SqlCommands.Parameters.AddWithValue("@ProcessStartTime", ProcessStartTime);
                SqlCommands.Parameters.AddWithValue("@FunctionStartTime", StartTime.ToString());
                SqlCommands.Parameters.AddWithValue("@FunctionEndTime", EndTime.ToString());
                SqlCommands.Parameters.AddWithValue("@InitialRowsAffected", dtOptInDetail.Rows.Count.ToString());
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
        #endregion
        #region Variable Fee Calculation
        public void VariableFeeInput(string fiscalQuarter, string variableFeeCalculationOutputType)
        {
            bool sendToDb = true;
            EntityCollection optInDetails = new EntityCollection();
            List<FilterExpression> filtersList = new List<FilterExpression>();
            DateTime StartTime = DateTime.UtcNow;
            Dictionary<string, int> IDictionary = ReturnMonthNumericalValue();
            Dictionary<string, int> IDictionaryQuarter = ReturnMonthInQuarterNumericalValue();
            string variableFeeCalculationOutputTypeCalc = string.Empty;
            string monthName = DateTime.Now.ToString("MMM");
            int month = 0;
            int quarter = 0;
            bool process = false;
            DataTable _dtOptInDetails = new DataTable();


            foreach (var n in IDictionaryQuarter)
            {
                if (n.Key == monthName)
                {
                    quarter = n.Value;
                }
            }

            variableFeeCalculationOutputTypeCalc = GetVarCalcType(variableFeeCalculationOutputType, fiscalQuarter);
            if (!string.IsNullOrWhiteSpace(fiscalQuarter))
            {
                FilterExpression filterMonth = new FilterExpression(LogicalOperator.Or);  
                filterMonth.AddCondition("new_quarter", ConditionOperator.Equal, fiscalQuarter);

                FilterExpression filterVarCalcMethod = new FilterExpression();
                filterVarCalcMethod.AddCondition("new_variablepaymentcalculationmethod", ConditionOperator.Equal, Convert.ToInt32(100000000));

                FilterExpression filterPayoutAttMethod = new FilterExpression();
                filterVarCalcMethod.AddCondition("new_payoutattainmentmodel", ConditionOperator.NotNull);

                FilterExpression filterProgram = new FilterExpression(LogicalOperator.Or);
                Guid smbprogramid = GetEntityCollection(_orgService, "new_program", "SMB Vendor Tele")[0].Id;
                Guid earenewalsprogramid = GetEntityCollection(_orgService, "new_program", "EA Renewals")[0].Id;
                Guid mwprogramid = GetEntityCollection(_orgService, "new_program", "MW Grow")[0].Id;
                Guid tumprogramid = GetEntityCollection(_orgService, "new_program", "Top Unmanaged Motion (TUM)")[0].Id;
                if (smbprogramid != Guid.Empty)
                {
                    filterProgram.AddCondition("new_programname", ConditionOperator.Equal, smbprogramid);
                }
                if (mwprogramid != Guid.Empty)
                {
                    filterProgram.AddCondition("new_programname", ConditionOperator.Equal, mwprogramid);
                }
                if (tumprogramid != Guid.Empty)
                {
                    filterProgram.AddCondition("new_programname", ConditionOperator.Equal, tumprogramid);
                }
                if (earenewalsprogramid != Guid.Empty)
                {
                    filterProgram.AddCondition("new_programname", ConditionOperator.Equal, earenewalsprogramid);
                }

                FilterExpression filterForecastPaymentMethod = new FilterExpression(LogicalOperator.And);
                FilterExpression filterForecastPaymentMethodOr = new FilterExpression(LogicalOperator.Or);
                filterForecastPaymentMethodOr.AddCondition("new_paymentmethod", ConditionOperator.Equal, Convert.ToInt32(100000004));
                filterForecastPaymentMethodOr.AddCondition("new_variablefeeforecast", ConditionOperator.GreaterThan, 0);
                filterForecastPaymentMethod.AddFilter(filterForecastPaymentMethodOr);

                filtersList.Add(filterForecastPaymentMethod);
                filtersList.Add(filterProgram);
                filtersList.Add(filterMonth);
                filtersList.Add(filterVarCalcMethod);
                filtersList.Add(filterPayoutAttMethod);
                //EntityCollection optInDetails = _orgService.RetrieveMultiple(fetchOptDetails);
                optInDetails = _manager.Retrieve5000PlusRecordsUsingQueryExpression("new_resourcingtargetdetail", true, null, filtersList, null);

                _dtOptInDetails.Columns.Add("OptInDetailID", typeof(Guid));
                _dtOptInDetails.Columns.Add("FiscalMonth", typeof(string));
                _dtOptInDetails.Columns.Add("FiscalQuarter", typeof(string));
                _dtOptInDetails.Columns.Add("SubsidiaryName", typeof(string));
                _dtOptInDetails.Columns.Add("RegionName", typeof(string));
                _dtOptInDetails.Columns.Add("AreaName", typeof(string));
                _dtOptInDetails.Columns.Add("VendorName", typeof(string));
                _dtOptInDetails.Columns.Add("PaymentMethod", typeof(string));
                _dtOptInDetails.Columns.Add("MaxVariableFee", typeof(decimal));
                _dtOptInDetails.Columns.Add("OptionalKPI1", typeof(string));
                _dtOptInDetails.Columns.Add("OptionalKPI1Weighting", typeof(decimal));
                _dtOptInDetails.Columns.Add("OptionalKPI2", typeof(string));
                _dtOptInDetails.Columns.Add("OptionalKPI2Weighting", typeof(decimal));
                _dtOptInDetails.Columns.Add("VariableFeeCalculationDetails", typeof(string));
                _dtOptInDetails.Columns.Add("CloudRevenueExtraWeighting", typeof(decimal));
                _dtOptInDetails.Columns.Add("CustomerAddsExtraWeighting", typeof(decimal));
                _dtOptInDetails.Columns.Add("ActualVariableFee", typeof(decimal));
                _dtOptInDetails.Columns.Add("ACRExtraWeighting", typeof(decimal));
                _dtOptInDetails.Columns.Add("PayoutAttainmentModelId", typeof(Guid));
                _dtOptInDetails.Columns.Add("ProcessStartTime", typeof(string));
                _dtOptInDetails.Columns.Add("IsYTD", typeof(string));
                _dtOptInDetails.Columns.Add("ProgramName", typeof(string));
                _dtOptInDetails.Columns.Add("TimeZone", typeof(string));
                _dtOptInDetails.Columns.Add("SubRegionName", typeof(string));
                _dtOptInDetails.Columns.Add("MinimumGeoAggregationMethod", typeof(string));
                _dtOptInDetails.Columns.Add("MinimumGeoAggregationLevel", typeof(int));
                _dtOptInDetails.Columns.Add("VariableFeeCalculationOutputType", typeof(string));
                EntityCollection MonthColl = GetAllMonthsInVTCP(_orgService);

                List<Entity> AllMonthlist = MonthColl.Entities.ToList().OrderBy(c => c.Attributes["createdon"]).ToList();
                DateTime MonthStartDate = DateTime.MinValue;
                DateTime MonthEndDate = DateTime.MinValue;
                string MinimumGeoAggregationMethod = null;
                int MinimumGeoAggregationLevel;

                foreach (Entity optInDetail in optInDetails.Entities)
                {
                    if (variableFeeCalculationOutputTypeCalc.ToLower() == "recon" && optInDetail.GetAttributeValue<decimal?>("new_actualvariablefee") == null)
                    {
                        sendToDb = false;
                        break;
                    }
                    string RegionName = optInDetail.GetAttributeValue<EntityReference>("new_regionname") != null ? optInDetail.GetAttributeValue<EntityReference>("new_regionname").Name : null;

                    Guid PayoutAttainmentModelId = optInDetail.GetAttributeValue<EntityReference>("new_payoutattainmentmodel") != null ? optInDetail.GetAttributeValue<EntityReference>("new_payoutattainmentmodel").Id : Guid.Empty;

                    string OptInMonth = optInDetail.GetAttributeValue<EntityReference>("new_month") != null ? optInDetail.GetAttributeValue<EntityReference>("new_month").Name : string.Empty;

                    string AreaName = optInDetail.GetAttributeValue<EntityReference>("new_areaname") != null ? optInDetail.GetAttributeValue<EntityReference>("new_areaname").Name : null;

                    string subsidiary = optInDetail.GetAttributeValue<EntityReference>("new_subsidiaryname") != null ? optInDetail.GetAttributeValue<EntityReference>("new_subsidiaryname").Name : null;

                    string subRegion = optInDetail.GetAttributeValue<EntityReference>("new_subregionname") != null ? optInDetail.GetAttributeValue<EntityReference>("new_subregionname").Name : null;

                    string TimeZone = optInDetail.GetAttributeValue<EntityReference>("new_timezone") != null ? optInDetail.GetAttributeValue<EntityReference>("new_timezone").Name : null;
                    if (!string.IsNullOrWhiteSpace(subsidiary))
                    {
                        MinimumGeoAggregationMethod = "Subsidiary";
                        MinimumGeoAggregationLevel = 5;
                    }
                    else if (!string.IsNullOrWhiteSpace(subRegion))
                    {
                        MinimumGeoAggregationMethod = "Sub Region";
                        MinimumGeoAggregationLevel = 4;
                    }
                    else if (!string.IsNullOrWhiteSpace(RegionName))
                    {
                        MinimumGeoAggregationMethod = "Region";
                        MinimumGeoAggregationLevel = 3;
                    }
                    else if (!string.IsNullOrWhiteSpace(AreaName))
                    {
                        MinimumGeoAggregationMethod = "Area";
                        MinimumGeoAggregationLevel = 2;
                    }
                    else if (!string.IsNullOrWhiteSpace(TimeZone))
                    {
                        MinimumGeoAggregationMethod = "Time Zone";
                        MinimumGeoAggregationLevel = 1;
                    }
                    else
                    {
                        MinimumGeoAggregationMethod = "WW";
                        MinimumGeoAggregationLevel = 0;
                    }

                    DataRow drOptIndetail = _dtOptInDetails.NewRow();

                    drOptIndetail["OptInDetailID"] = optInDetail.Id;
                    drOptIndetail["PayoutAttainmentModelId"] = PayoutAttainmentModelId;
                    drOptIndetail["FiscalMonth"] = optInDetail.GetAttributeValue<EntityReference>("new_month") != null ? optInDetail.GetAttributeValue<EntityReference>("new_month").Name : null;
                    drOptIndetail["SubsidiaryName"] = optInDetail.GetAttributeValue<EntityReference>("new_subsidiaryname") != null ? optInDetail.GetAttributeValue<EntityReference>("new_subsidiaryname").Name : null;
                    drOptIndetail["FiscalQuarter"] = optInDetail.GetAttributeValue<string>("new_quarter") != null ? optInDetail.GetAttributeValue<string>("new_quarter") : null;
                    drOptIndetail["MinimumGeoAggregationMethod"] = MinimumGeoAggregationMethod;
                    drOptIndetail["MinimumGeoAggregationLevel"] = MinimumGeoAggregationLevel;
                    if (AreaName != null && AreaName == "CEMA" && subsidiary != null && subsidiary == "Pakistan")
                    {

                        drOptIndetail["RegionName"] = "Middle East";

                    }
                    else
                    {
                        drOptIndetail["RegionName"] = optInDetail.GetAttributeValue<EntityReference>("new_regionname") != null ? optInDetail.GetAttributeValue<EntityReference>("new_regionname").Name : null;
                    }
                    if (AreaName != null && AreaName == "CEMA")
                    {
                        if (RegionName != null && RegionName == "Africa" && subRegion != null && subRegion == "South Africa")
                        {
                            drOptIndetail["SubRegionName"] = "South Africa";
                        }
                        else if (RegionName != null && RegionName == "Africa" && subRegion != null && subRegion != "South Africa")
                        {
                            drOptIndetail["SubRegionName"] = "Rest Of Africa";
                        }
                        else if (RegionName != null && RegionName == "Middle East" && subRegion != null && (subRegion == "Saudi Arabia" || subRegion == "United Arab Emirates"))
                        {
                            if (subRegion == "Saudi Arabia")
                                drOptIndetail["SubRegionName"] = "Saudi Arabia";
                            if (subRegion == "United Arab Emirates")
                                drOptIndetail["SubRegionName"] = "United Arab Emirates";
                        }
                        else if (RegionName != null && RegionName == "Middle East" && subRegion != null && (subRegion != "Saudi Arabia" && subRegion != "United Arab Emirates"))
                        {
                            drOptIndetail["SubRegionName"] = "Rest Of Middle East";
                        }
                        else if (RegionName != null && RegionName == "Central Europe" && subRegion != null && subRegion == "Pakistan")
                        {
                            drOptIndetail["SubRegionName"] = "Rest Of Middle East";
                        }
                        else
                            drOptIndetail["SubRegionName"] = optInDetail.GetAttributeValue<EntityReference>("new_subregionname") != null ? optInDetail.GetAttributeValue<EntityReference>("new_subregionname").Name : null;
                    }
                    else
                        drOptIndetail["SubRegionName"] = optInDetail.GetAttributeValue<EntityReference>("new_subregionname") != null ? optInDetail.GetAttributeValue<EntityReference>("new_subregionname").Name : null;

                    drOptIndetail["AreaName"] = optInDetail.GetAttributeValue<EntityReference>("new_areaname") != null ? optInDetail.GetAttributeValue<EntityReference>("new_areaname").Name : null;

                    drOptIndetail["ActualVariableFee"] = optInDetail.GetAttributeValue<decimal?>("new_actualvariablefee") != null ? optInDetail.GetAttributeValue<decimal?>("new_actualvariablefee") : (object)DBNull.Value;
                    drOptIndetail["VendorName"] = optInDetail.GetAttributeValue<EntityReference>("new_vendorname") != null ? optInDetail.GetAttributeValue<EntityReference>("new_vendorname").Name : null;
                    drOptIndetail["PaymentMethod"] = optInDetail.GetAttributeValue<OptionSetValue>("new_paymentmethod") != null ? optInDetail.FormattedValues["new_paymentmethod"] : null;

                    drOptIndetail["OptionalKPI1"] = optInDetail.GetAttributeValue<OptionSetValue>("new_optionalpayoutkpi1") != null ? optInDetail.FormattedValues["new_optionalpayoutkpi1"] : null;
                    drOptIndetail["OptionalKPI1Weighting"] = optInDetail.GetAttributeValue<decimal?>("new_optionalpayoutkpi1weighting") != null ? optInDetail.GetAttributeValue<decimal?>("new_optionalpayoutkpi1weighting") : (object)DBNull.Value;
                    drOptIndetail["OptionalKPI2"] = optInDetail.GetAttributeValue<OptionSetValue>("new_optionalpayoutkpi2") != null ? optInDetail.FormattedValues["new_optionalpayoutkpi2"] : null;
                    drOptIndetail["OptionalKPI2Weighting"] = optInDetail.GetAttributeValue<decimal?>("new_optionalpayoutkpi2weighting") != null ? optInDetail.GetAttributeValue<decimal?>("new_optionalpayoutkpi2weighting") : (object)DBNull.Value;

                    drOptIndetail["VariableFeeCalculationDetails"] = optInDetail.GetAttributeValue<string>("new_variablefeecalculationdetails") != null ? optInDetail.GetAttributeValue<string>("new_variablefeecalculationdetails") : null;

                    drOptIndetail["CloudRevenueExtraWeighting"] = optInDetail.GetAttributeValue<decimal?>("new_cloudrevenueextraweighting") != null ? optInDetail.GetAttributeValue<decimal?>("new_cloudrevenueextraweighting") : (object)DBNull.Value;
                    drOptIndetail["CustomerAddsExtraWeighting"] = optInDetail.GetAttributeValue<decimal?>("new_customeraddsextraweighting") != null ? optInDetail.GetAttributeValue<decimal?>("new_customeraddsextraweighting") : (object)DBNull.Value;

                    string PaymentMethod = drOptIndetail["PaymentMethod"].ToString();
                    decimal? NumAgentsActual = optInDetail.GetAttributeValue<decimal?>("new_actualsalesagents") != null ? optInDetail.GetAttributeValue<decimal?>("new_actualsalesagents") : null;

                    drOptIndetail["ACRExtraWeighting"] = optInDetail.GetAttributeValue<decimal?>("new_azureconsumedrevenueextraweighting") != null ? optInDetail.GetAttributeValue<decimal?>("new_azureconsumedrevenueextraweighting") : (object)DBNull.Value; ;

                    drOptIndetail["MaxVariableFee"] = optInDetail.GetAttributeValue<decimal?>("new_maxvariablefeeadjusted") != null ? optInDetail.GetAttributeValue<decimal?>("new_maxvariablefeeadjusted") : (object)DBNull.Value;
                    drOptIndetail["ProgramName"] = optInDetail.GetAttributeValue<EntityReference>("new_programname") != null ? optInDetail.GetAttributeValue<EntityReference>("new_programname").Name : null;
                    drOptIndetail["TimeZone"] = optInDetail.GetAttributeValue<EntityReference>("new_timezone") != null ? optInDetail.GetAttributeValue<EntityReference>("new_timezone").Name : null;

                    drOptIndetail["VariableFeeCalculationOutputType"] = variableFeeCalculationOutputTypeCalc;

                    drOptIndetail["ProcessStartTime"] = StartTime.ToString();
                    _dtOptInDetails.Rows.Add(drOptIndetail);
                }
                string tableName = "[dbo].[SSIS_Staging_VariablePaymentCalc]";
                if (sendToDb == true)
                {
                    _manager.InsertDataIntoSQLDatabase(tableName, _dtOptInDetails);
                }

            }
            DateTime EndTime = DateTime.UtcNow;
            SqlConnection con = new SqlConnection(VariableFeeManager._sqlConnection);
            //AuthenticationResult authenticationResult = _manager.AADAunthenticationResult();
            con.AccessToken = Manager.getAccessToken(System.Environment.GetEnvironmentVariable("ResourceId"), System.Environment.GetEnvironmentVariable("VTCPManagedIdentity"), true).Result;
            SqlCommand SqlCommands = new SqlCommand();
            SqlCommands = new SqlCommand("AzureFunctionsDataLoad", con);
            SqlCommands.CommandType = CommandType.StoredProcedure;
            SqlCommands.Parameters.AddWithValue("@FunctionName", "VariableFeeInput");
            SqlCommands.Parameters.AddWithValue("@ProcessStartTime", StartTime.ToString());
            SqlCommands.Parameters.AddWithValue("@FunctionStartTime", StartTime.ToString());
            SqlCommands.Parameters.AddWithValue("@FunctionEndTime", EndTime.ToString());
            SqlCommands.Parameters.AddWithValue("@InitialRowsAffected", _dtOptInDetails.Rows.Count.ToString());
            SqlCommands.Parameters.AddWithValue("@TotalRowsAffected", _dtOptInDetails.Rows.Count.ToString());
            con.Open();
            SqlCommands.ExecuteNonQuery();
            con.Close();
        }

        /// <summary>
        /// Method to return Month numerical value with Month name as input
        /// </summary>
        /// <returns></returns>
        private static Dictionary<string, int> ReturnMonthNumericalValue()
        {
            Dictionary<string, int> IDictionary = new Dictionary<string, int>();
            IDictionary.Add("Jan", 7);
            IDictionary.Add("Feb", 8);
            IDictionary.Add("Mar", 9);
            IDictionary.Add("Apr", 10);
            IDictionary.Add("May", 11);
            IDictionary.Add("Jun", 12);
            IDictionary.Add("Jul", 1);
            IDictionary.Add("Aug", 2);
            IDictionary.Add("Sep", 3);
            IDictionary.Add("Oct", 4);
            IDictionary.Add("Nov", 5);
            IDictionary.Add("Dec", 6);
            return IDictionary;
        }
        private static Dictionary<string, int> ReturnQuarterNumericalValue()
        {
            Dictionary<string, int> IDictionary = new Dictionary<string, int>();
            IDictionary.Add("Jan", 3);
            IDictionary.Add("Feb", 3);
            IDictionary.Add("Mar", 3);
            IDictionary.Add("Apr", 4);
            IDictionary.Add("May", 4);
            IDictionary.Add("Jun", 4);
            IDictionary.Add("Jul", 1);
            IDictionary.Add("Aug", 1);
            IDictionary.Add("Sep", 1);
            IDictionary.Add("Oct", 2);
            IDictionary.Add("Nov", 2);
            IDictionary.Add("Dec", 2);
            return IDictionary;
        }

        private static Dictionary<string, int> ReturnMonthInQuarterNumericalValue()
        {
            Dictionary<string, int> IDictionary = new Dictionary<string, int>();
            IDictionary.Add("Jan", 1);
            IDictionary.Add("Feb", 2);
            IDictionary.Add("Mar", 3);
            IDictionary.Add("Apr", 1);
            IDictionary.Add("May", 2);
            IDictionary.Add("Jun", 3);
            IDictionary.Add("Jul", 1);
            IDictionary.Add("Aug", 2);
            IDictionary.Add("Sep", 3);
            IDictionary.Add("Oct", 1);
            IDictionary.Add("Nov", 2);
            IDictionary.Add("Dec", 3);
            return IDictionary;
        }
        public static DateTime GetNthWeekdayOfMonth(DateTime month, DayOfWeek dayOfWeek, int occurrence)
        {
            DateTime date = new DateTime(month.Year, month.Month, 1);

            while (date.DayOfWeek != dayOfWeek)
            {
                date = date.AddDays(1);
            }
            date = date.AddDays((occurrence - 1) * 7);

            return date;
        }
        public int GetWeekOfMonth(DateTime date)
        {
            // Determine the first day of the month
            DateTime firstDayOfMonth = new DateTime(date.Year, date.Month, 1);

            // Determine the day of the week the first day falls on (0 - Sunday, 1 - Monday, ..., 6 - Saturday)
            int firstDayOfWeek = (int)firstDayOfMonth.DayOfWeek;

            // Determine the offset to the nearest day of the week we're interested in
            int daysOffset = (int)date.DayOfWeek - firstDayOfWeek;

            // Calculate the week of the month
            int weekOfMonth = (date.Day + daysOffset - 1) / 7 + 1;

            return weekOfMonth;
        }

        public void VariableFeeOutput()
        {
            DateTime StartTime = DateTime.UtcNow;
            try
            {
                int batch = 0;
                List<OptionIndetails> DeleteOptDetails = new List<OptionIndetails>();
                ExecuteMultipleSettings excuteMultipleSettings = new ExecuteMultipleSettings();
                excuteMultipleSettings.ContinueOnError = true;
                excuteMultipleSettings.ReturnResponses = true;

                ExecuteMultipleRequest updateOptDetailRequest = new ExecuteMultipleRequest();
                updateOptDetailRequest.Requests = new OrganizationRequestCollection();
                updateOptDetailRequest.Settings = excuteMultipleSettings;

               string retrieveQuery = "select distinct O.*,I.VariableFeeCalculationDetails as OldVariableFeeCalculationDetails,I.VariableFeeCalculationOutputType as VariableFeeCalculationOutputType from [dbo].[SSIS_Output_VariablePaymentCalc]  (nolock) O join  [SSIS_staging_VariablePaymentCalc] (nolock) I on I.optinDetailID=o.optinDetailID ";
                DataTable dtOptInDetail = _manager.RetrieveDatafromSQLDatabase(retrieveQuery);

                string ProcessStartTime = string.Empty;
                if (dtOptInDetail != null && dtOptInDetail.Rows.Count > 0)
                {
                    foreach (DataRow row in dtOptInDetail.Rows)
                    {
                        batch++;
                        Guid OptInDetailID = (Guid)row["OptInDetailID"];
                        DeleteOptDetails.Add(new OptionIndetails
                        {
                            id = Convert.ToString(OptInDetailID)
                        });
                        decimal? VariablePayoutAmount = row["VariablePayoutAmount"] != DBNull.Value ? Convert.ToDecimal(row["VariablePayoutAmount"]) : (decimal?)null;
                        string VariableFeeCalculationDetails = row["VariableFeeCalculationDetails"] != null ? Convert.ToString(row["VariableFeeCalculationDetails"]) : null;
                        string OldVariableFeeCalculationDetails = row["OldVariableFeeCalculationDetails"] != null ? Convert.ToString(row["OldVariableFeeCalculationDetails"]) : null;
                        decimal? VariablePayoutPercentage = row["VariablePayoutPercentage"] != DBNull.Value ? Convert.ToDecimal(row["VariablePayoutPercentage"]) : (decimal?)null;
                        decimal? MaxVariableFeeCommission = row["MaxVariableFeeCommission"] != DBNull.Value ? Convert.ToDecimal(row["MaxVariableFeeCommission"]) : (decimal?)null;
                        ProcessStartTime = Convert.ToString(row["ProcessStartTime"]);
                        string VariableFeeCalculationOutputType = row["VariableFeeCalculationOutputType"]!=null? Convert.ToString(row["VariableFeeCalculationOutputType"]) : null;
                        if (!string.IsNullOrEmpty(OldVariableFeeCalculationDetails))
                        {
                            VariableFeeCalculationDetails += "\r\n\r\n------------------------------------ \r\n\r\n" + OldVariableFeeCalculationDetails;
                        }

                        Entity optInDetailEntity = new Entity("new_resourcingtargetdetail");
                        optInDetailEntity.Id = OptInDetailID;


                        optInDetailEntity["new_variablefeecalculationdetails"] = VariableFeeCalculationDetails == null ? string.Empty : _manager.LimitCharacterCount(VariableFeeCalculationDetails, 1048575);

                        optInDetailEntity["new_maxvariablefeeadjusted"] = MaxVariableFeeCommission == null ? null : MaxVariableFeeCommission;

                        if (VariableFeeCalculationOutputType == "Preliminary")
                        {
                            optInDetailEntity["new_variablepayoutcalculated"] = VariablePayoutPercentage==null?null: VariablePayoutPercentage;
                            optInDetailEntity["new_preliminaryvariablefee"] = VariablePayoutAmount == null ? null : VariablePayoutAmount;
                        }
                        else if (VariableFeeCalculationOutputType == "Final")
                        {
                            optInDetailEntity["new_variablepayoutcalculated"] = VariablePayoutPercentage == null ? null : VariablePayoutPercentage;
                            optInDetailEntity["new_preliminaryvariablefee"] = VariablePayoutAmount == null ? null : VariablePayoutAmount;
                            optInDetailEntity["new_isfinalvariablecalculation"] = new OptionSetValue(Convert.ToInt32(100000000));
                        }
                        else if (VariableFeeCalculationOutputType == "Recon")
                        {
                            optInDetailEntity["new_variablefeereconpercentage"] = VariablePayoutPercentage == null ? null : VariablePayoutPercentage; 
                            optInDetailEntity["new_variablefeerecon"] = VariablePayoutAmount == null ? null : VariablePayoutAmount;
                        }

                        if (MaxVariableFeeCommission!=null)
                        {
                            optInDetailEntity["new_maxvariablefeeadjusted"] = MaxVariableFeeCommission == null ? null : MaxVariableFeeCommission;
                        }
                        var updateRequest = new UpdateRequest();
                        updateRequest.Target = optInDetailEntity;
                        updateOptDetailRequest.Requests.Add(updateRequest);

                        if (updateOptDetailRequest.Requests.Count == 100)
                        {
                            _manager.ExecuteBulkRequest(updateOptDetailRequest, "Updated Opt Details");
                            updateOptDetailRequest.Requests.Clear();
                            DeleteRecordsInDatabase(DeleteOptDetails, "SSIS_staging_VariablePaymentCalc");
                            DeleteRecordsInDatabase(DeleteOptDetails, "SSIS_Output_VariablePaymentCalc");
                            DeleteOptDetails.Clear();
                            batch = 0;
                        }
                    }
                    if (batch > 0)
                    {
                        _manager.ExecuteBulkRequest(updateOptDetailRequest, "Updated Opt Details");
                        updateOptDetailRequest.Requests.Clear();
                        DeleteRecordsInDatabase(DeleteOptDetails, "SSIS_staging_VariablePaymentCalc");
                        DeleteRecordsInDatabase(DeleteOptDetails, "SSIS_Output_VariablePaymentCalc");
                        DeleteOptDetails.Clear();
                        batch = 0;
                    }
                }
                DateTime EndTime = DateTime.UtcNow;
                SqlConnection con = new SqlConnection(VariableFeeManager._sqlConnection);
                //AuthenticationResult authenticationResult = _manager.AADAunthenticationResult();
                con.AccessToken = Manager.getAccessToken(System.Environment.GetEnvironmentVariable("ResourceId"), System.Environment.GetEnvironmentVariable("VTCPManagedIdentity"), true).Result;
                SqlCommand SqlCommands = new SqlCommand();
                SqlCommands = new SqlCommand("AzureFunctionsDataLoad", con);
                SqlCommands.CommandType = CommandType.StoredProcedure;
                SqlCommands.Parameters.AddWithValue("@FunctionName", "VariablePaymentCalc");
                SqlCommands.Parameters.AddWithValue("@ProcessStartTime", ProcessStartTime);
                SqlCommands.Parameters.AddWithValue("@FunctionStartTime", StartTime.ToString());
                SqlCommands.Parameters.AddWithValue("@FunctionEndTime", EndTime.ToString());
                SqlCommands.Parameters.AddWithValue("@InitialRowsAffected", dtOptInDetail.Rows.Count.ToString());
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
        /// <summary>
        /// Method to get Entity collection
        /// </summary>
        /// <param name="service"></param>
        /// <param name="entityName"></param>
        /// <param name="monthName"></param>
        /// <returns></returns>
        public EntityCollection GetEntityCollection(IOrganizationService service, string entityName, string name)
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
                queryExpression.Criteria.AddCondition("statecode", ConditionOperator.Equal, 0);
                queryExpression.Criteria.AddCondition("statuscode", ConditionOperator.Equal, 1);
                if (!string.IsNullOrWhiteSpace(name))
                {
                    queryExpression.Criteria.AddCondition("new_name", ConditionOperator.Equal, name);
                }
                objEntityColl = service.RetrieveMultiple(queryExpression);
            }

            return objEntityColl;
        }

        /// <summary>
        /// Class for OptionIn details
        /// </summary>
        public class OptionIndetails
        {
            public string id;
        }
        public class month
        {
            public Guid monthId;
            public string monthName;
        }
        /// <summary>
        /// Method to delete records from database
        /// </summary>
        /// <param name="idList"></param>
        /// <param name="tableName"></param>
        public void DeleteRecordsInDatabase(List<OptionIndetails> idList, string tableName)
        {
            SqlConnection conn = new SqlConnection(_sqlConnection);
            //AuthenticationResult authenticationResult = _manager.AADAunthenticationResult();
            conn.AccessToken = Manager.getAccessToken(System.Environment.GetEnvironmentVariable("ResourceId"), System.Environment.GetEnvironmentVariable("VTCPManagedIdentity"), true).Result;
            conn.Open();
            string IdValue = string.Empty;
            SqlCommand cmd = null;
            foreach (var Id in idList)
            {
                IdValue += "'" + Id.id + "',";
            }
            string Finalvalue = IdValue.TrimEnd(',');
            cmd = new SqlCommand("Delete from " + tableName + " where OptInDetailID in(" + Finalvalue + ")", conn);
            cmd.ExecuteNonQuery();
            conn.Close();
        }
        public string GetVarCalcType(string variableFeeCalculationOutputType, string fiscalQuarter)
        {
            string variableFeeCalculationOutputTypeCalc = string.Empty;
            QueryExpression getLastMonth = new QueryExpression
            {
                EntityName = "new_month",
                ColumnSet = new ColumnSet(true),
                Criteria = new FilterExpression(),
                TopCount = 1
            };
            getLastMonth.Criteria.AddCondition("new_quarter", ConditionOperator.Equal, fiscalQuarter);
            getLastMonth.AddOrder("new_monthenddate", OrderType.Descending);
            EntityCollection lastMonth = _orgService.RetrieveMultiple(getLastMonth);
            DateTime nextMonth = lastMonth[0].GetAttributeValue<DateTime>("new_monthenddate").AddMonths(1);

            DateTime thirdThursday = GetNthWeekdayOfMonth(nextMonth, DayOfWeek.Thursday, 3);
            DateTime fourthThursday = GetNthWeekdayOfMonth(nextMonth, DayOfWeek.Thursday, 4);

            if (variableFeeCalculationOutputType.ToLower() == "default")
            {
                if (DateTime.UtcNow <= thirdThursday)
                {
                    variableFeeCalculationOutputTypeCalc = "Preliminary";
                }
                else if (DateTime.UtcNow > thirdThursday && DateTime.UtcNow <= fourthThursday)
                {
                    variableFeeCalculationOutputTypeCalc = "Final";
                }
                else if (DateTime.UtcNow > fourthThursday)
                {
                    variableFeeCalculationOutputTypeCalc = "Recon";
                }
            }
            else if (!string.IsNullOrWhiteSpace(variableFeeCalculationOutputType))
            {
                variableFeeCalculationOutputTypeCalc = variableFeeCalculationOutputType;
            }
            return variableFeeCalculationOutputTypeCalc;
        }
        public EntityCollection GetAllMonthsInVTCP(IOrganizationService service)
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
            pagequery.EntityName = "new_month";

            //pagequery.ColumnSet.AllColumns = true;
            pagequery.ColumnSet = new ColumnSet("new_monthid", "new_monthstartdate", "new_monthenddate", "new_name", "createdon");

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
        #endregion
    }
}
