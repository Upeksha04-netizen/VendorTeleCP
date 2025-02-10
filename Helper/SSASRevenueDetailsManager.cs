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

namespace Helper
{
    public class SSASRevenueDetailsManager
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

        public SSASRevenueDetailsManager(string instancename, string clientId, string secret, string msxclientId, string msxsecret, string sqlconnection, string ssasServer, string blobConnection,string vtcpManagedIdentity, ILogger log)
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

        #region Logic of SSAS Authentication

        /// <summary>
        /// Class for SSASData
        /// </summary>
        public class KPICloudRevenue
        {
            public string VendorName;
            public string AreaName;
            public string RegionName;
            public string SubsidiaryName;
            public string SalesMotion;
            public string Month;
            public string Quarter;
            public string RevenueAttributed;
            public string ProductCategory;
            public string ProductName;
            public string CustomerAdds;
            public string ReportedRevenueACR;
            public string RevSumDivision;
            public string NPSAMTD;
            public string NPSAPM;
            public string ExpAgreements;
            public string RenAgreements;
            public string AgreementRetentionRate;
            public string ExpCustomers;
            public string RenCustomers;
            public string CustomerRetentionRate;
            public string CustomersTransitionToModern;
            public string ExpiredAmountAnnualised;
            public string RenewedAmountAnnualised;
            public string RevenueRecaptureRateAnnalised;
            public string SolutionArea;
            public string Year;
            public string MonthYearName;
            public string MonthID;
            public string CalendarDate;
            public string SalesDateID;
            public string RevSumDivisionID;
            public string SubProduct;
            public string SalesPlay;
            public string LeadSourceSubtype;
            public string LeadNumber;
            public string SourceSubtype;
            public string OpportunityNumber;
            public string OpportunityRevenueStatus;
            public string IsDCLM;
            public string LandedRevenue;
            public string CreatedDate;
            public string SubRegion;
            public string TimeZone;
            public string RevenueForecast;
            public string ProjectedRevenue;
            public string TotalRevenueOutlook;
            public string DataSource;
            public string ExpVolume;
            public string RenVolume;
            public string OnTimeRenVolume;
            public string ExclusionFlag;
        }

        /// <summary>
        /// To Pull data from SSAS server
        /// </summary>
        public List<KPICloudRevenue> GetDataFromAzureAnalysisService(string TableName)
        {
            StringBuilder query = new StringBuilder();
            var token = _manager.GetAccessTokenClientId("https://westus.asazure.windows.net");

            List<KPICloudRevenue> outputList = new List<KPICloudRevenue>();
            var connectionString = $"Provider=MSOLAP;Data Source={_ssasServer};Initial Catalog=RevAttrProd;User ID=;Password={token};Persist Security Info=True;Impersonation Level=Impersonate";
            try
            {
                using (AdomdConnection connection = new AdomdConnection(connectionString))
                {
                    connection.Open();
                    if (TableName == "[dbo].[KPIResults_CustomerAdds]")
                    {
                        query = new StringBuilder(@"EVALUATE SUMMARIZECOLUMNS (Vendors[Vendor],'Upper Geography'[Area],'Upper Geography'[Region],'Upper Geography'[Subsidiary],'Sales Motions'[Sales Motion],'Sales Date'[Sales Quarter],'Sales Date'[Sales Month Year],Products[Product],Opportunities[IsDCLM],'Upper Geography'[Sub Region],'Upper Geography'[Time Zone],""Customer Adds"", [Customer Adds])");
                    }
                    else if (TableName == "[dbo].[KPIResults_CloudRevenue]")
                    {
                        query = new StringBuilder(@"EVALUATE SUMMARIZECOLUMNS (Vendors[Vendor],'Upper Geography'[Area],'Upper Geography'[Region],'Upper Geography'[Subsidiary],'Sales Motions'[Sales Motion],'Sales Date'[Sales Quarter],'Sales Date'[Sales Month Year],Products[Product Category],Products[Product],'Solution Areas'[Solution Area],Opportunities[IsDCLM],'Upper Geography'[Sub Region],'Upper Geography'[Time Zone],""Attributed Revenue"", [Attributed Revenue],""Landed Revenue"", [Landed Revenue])");
                    }
                    else if (TableName == "[dbo].[KPIResults_AzureConsumedRevenue]")
                    {
                        query = new StringBuilder(@"EVALUATE SUMMARIZECOLUMNS (Vendors[Vendor],'Upper Geography'[Area],'Upper Geography'[Region],'Upper Geography'[Subsidiary],'Sales Motions'[Sales Motion],'Sales Date'[Sales Quarter],'Sales Date'[Sales Month Year],
                              Products[Product],Opportunities[IsDCLM],'Upper Geography'[Sub Region],'Upper Geography'[Time Zone],""Azure Consumed Revenue"", [Azure Consumed Revenue])");
                    }
                    else if (TableName == "[dbo].[KPIResults_NPSA]")
                    {
                        query = new StringBuilder(@"EVALUATE SUMMARIZECOLUMNS (Vendors[Vendor],'Upper Geography'[Area],'Upper Geography'[Region],'Upper Geography'[Subsidiary],'Sales Motions'[Sales Motion],'Sales Date'[Sales Quarter],'Sales Date'[Sales Month Year],RevSumDivisions[RevSumDivision],Opportunities[IsDCLM],'Upper Geography'[Sub Region],'Upper Geography'[Time Zone],""NPSA"", [NPSA Attributed])");

                    }
                    else if (TableName == "[dbo].[KPIResults_Renewals]")
                    {
                        query = new StringBuilder(@"EVALUATE SUMMARIZECOLUMNS (Vendors[Vendor],'Upper Geography'[Time Zone],'Upper Geography'[Area],'Upper Geography'[Region]
                                                   ,'Upper Geography'[Sub Region],'Upper Geography'[Subsidiary],'Sales Date'[Sales Quarter],Expirations[DataSource],Expirations[ExclusionFlag]
                                                    ,""ExpVolume"", [#ExpVolume],""RenVolume"", [#RenVolume],""OnTimeRenVolume"", [#OnTimeRenVolume])");
                    }
                    else if (TableName == "[dbo].[RevSumMapping]")
                    {
                        query = new StringBuilder(@"DEFINE MEASURE'RevSumDivisions'[RevSumCount] = COUNTROWS('RevSumDivisions') EVALUATE SUMMARIZECOLUMNS('RevSumDivisions'[RevSumDivision],'RevSumDivisions'[RevSumDivisionID],'Products'[SubProduct],'Products'[Product],'Products'[Product Category],'Solution Areas'[Solution Area],""Count"", [RevSumCount])");
                    }
                    else if (TableName == "[dbo].[Calendar]")
                    {
                        query = new StringBuilder(@"DEFINE MEASURE'Sales Date'[DateCount] = COUNTROWS('Sales Date') EVALUATE SUMMARIZECOLUMNS('Sales Date'[Sales Date],'Sales Date'[SalesDateID],'Sales Date'[Sales Month Year],'Sales Date'[SalesMonthID],'Sales Date'[Sales Quarter],'Sales Date'[Sales Year],""Count"", [DateCount])");
                    }
                    else if (TableName == "[dbo].[SMBVT_Opportunities]")
                    {
                        query = new StringBuilder(@"EVALUATE SUMMARIZECOLUMNS (Vendors[Vendor],'Upper Geography'[Area],'Upper Geography'[Region],'Upper Geography'[Subsidiary],'Sales Motions'[Sales Motion],'Sales Plays'[Sales Play],'Source Subtypes'[Source Subtype],'Opportunity Revenue Status'[Opportunity Revenue Status],Opportunities[FILTER],Opportunities[Opportunity Number],Opportunities[IsDCLM],'Created Date'[Created Date],""OpportunityCount"", [#Opportunities])");
                    }
                    else if (TableName == "[dbo].[SMBVT_Leads]")
                    {
                        query = new StringBuilder(@"EVALUATE SUMMARIZECOLUMNS (Vendors[Vendor],'Upper Geography'[Area],'Upper Geography'[Region],'Upper Geography'[Subsidiary],'Sales Motions'[Sales Motion],'Sales Plays'[Sales Play],'Lead Source Subtypes'[Lead Source Subtype],Leads[Lead Number],'Created Date'[Created Date],FILTER('Created Date', 'Created Date' [Created Relative Month] >= -18),""LeadCount"", [Leads])");
                    }
                    else if (TableName == "[dbo].[KPIResults_RevenueForecast]")
                    {
                        query = new StringBuilder(@"EVALUATE SUMMARIZECOLUMNS (Vendors[Vendor],'Upper Geography'[Area],'Upper Geography'[Region],'Upper Geography'[Subsidiary],'Sales Date'[Sales Quarter],'Sales Date'[Sales Month Year],Products[Product Category],Products[Product],'Upper Geography'[Sub Region],'Upper Geography'[Time Zone], Opportunities[IsDCLM],""Attributed Revenue"", [Attributed Revenue],""Future Monthly Subscription Revenue Forecast"", [Future Monthly Subscription Revenue Forecast], ""Projected Revenue"", [Projected Revenue], ""Total Revenue Outlook"", [Total Revenue Outlook])");
                    }
                    using (AdomdCommand command = new AdomdCommand(query.ToString(), connection))
                    {
                        var results = command.ExecuteReader();
                        int count = results.FieldCount;
                        foreach (var result in results)
                        {
                            if (TableName == "[dbo].[KPIResults_CustomerAdds]")
                            {
                                KPICustomerAddsResult(outputList, result);
                            }
                            else if (TableName == "[dbo].[KPIResults_CloudRevenue]")
                            {
                                KPICloudRevenueResult(outputList, result);
                            }
                            else if (TableName == "[dbo].[KPIResults_AzureConsumedRevenue]")
                            {
                                KPICloudConsumedRevenue(outputList, result);
                            }
                            else if (TableName == "[dbo].[KPIResults_NPSA]")
                            {
                                KPIResults_NPSA(outputList, result);
                            }
                            else if (TableName == "[dbo].[KPIResults_Renewals]")
                            {
                                KPIResults_Renewals(outputList, result);
                            }
                            else if (TableName == "[dbo].[RevSumMapping]")
                            {
                                KPIRevSumMappingResult(outputList, result);
                            }
                            else if (TableName == "[dbo].[Calendar]")
                            {
                                KPICalendarResult(outputList, result);
                            }
                            else if (TableName == "[dbo].[SMBVT_Opportunities]")
                            {
                                SMBVT_Opportunities(outputList, result);
                            }
                            else if (TableName == "[dbo].[SMBVT_Leads]")
                            {
                                SMBVT_Leads(outputList, result);
                            }
                            else if (TableName == "[dbo].[KPIResults_RevenueForecast]")
                            {
                                KPIResults_RevenueForecast(outputList, result);
                            }
                        }
                    }
                    _log.LogInformation(Convert.ToString(outputList.Count));
                }
            }
            catch (Exception ex)
            {
                _log.LogError(ex.Message);
            }
            return outputList;
        }

        public void KPIResults_RevenueForecast(List<KPICloudRevenue> outputList, IDataRecord result)
        {
            outputList.Add(new KPICloudRevenue
            {
                VendorName = result[0]?.ToString(),
                AreaName = result[1]?.ToString(),
                RegionName = result[2]?.ToString(),
                SubsidiaryName = result[3]?.ToString(),
                Quarter = result[4]?.ToString(),
                Month = result[5]?.ToString(),
                ProductCategory = result[6]?.ToString(),
                ProductName = result[7]?.ToString(),
                SubRegion = result[8]?.ToString(),
                TimeZone = result[9]?.ToString(),
                IsDCLM = result[10]?.ToString(),
                RevenueAttributed = result[11]?.ToString(),
                RevenueForecast = result[12]?.ToString(),
                ProjectedRevenue = result[13]?.ToString(),
                TotalRevenueOutlook = result[14]?.ToString()

            });
        }
        public void KPICloudConsumedRevenue(List<KPICloudRevenue> outputList, IDataRecord result)
        {
            outputList.Add(new KPICloudRevenue
            {
                VendorName = result[0]?.ToString(),
                AreaName = result[1]?.ToString(),
                RegionName = result[2]?.ToString(),
                SubsidiaryName = result[3]?.ToString(),
                SalesMotion = result[4]?.ToString(),
                Quarter = result[5]?.ToString(),
                Month = result[6]?.ToString(),
                ProductName = result[7]?.ToString(),
                IsDCLM = result[8]?.ToString(),
                SubRegion = result[9]?.ToString(),
                TimeZone = result[10]?.ToString(),
                ReportedRevenueACR = result[11]?.ToString()
            });
        }

        public void KPICustomerAddsResult(List<KPICloudRevenue> outputList, IDataRecord result)
        {
            outputList.Add(new KPICloudRevenue
            {
                VendorName = result[0]?.ToString(),
                AreaName = result[1]?.ToString(),
                RegionName = result[2]?.ToString(),
                SubsidiaryName = result[3]?.ToString(),
                SalesMotion = result[4]?.ToString(),
                Quarter = result[5]?.ToString(),
                Month = result[6]?.ToString(),
                ProductName = result[7]?.ToString(),
                IsDCLM = result[8]?.ToString(),
                SubRegion = result[9]?.ToString(),
                TimeZone = result[10]?.ToString(),
                CustomerAdds = result[11]?.ToString()
            });
        }

        public void KPICloudRevenueResult(List<KPICloudRevenue> outputList, IDataRecord result)
        {
            outputList.Add(new KPICloudRevenue
            {
                VendorName = result[0]?.ToString(),
                AreaName = result[1]?.ToString(),
                RegionName = result[2]?.ToString(),
                SubsidiaryName = result[3]?.ToString(),
                SalesMotion = result[4]?.ToString(),
                Quarter = result[5]?.ToString(),
                Month = result[6]?.ToString(),
                ProductCategory = result[7]?.ToString(),
                ProductName = result[8]?.ToString(),
                SolutionArea = result[9]?.ToString(),
                IsDCLM = result[10]?.ToString(),
                SubRegion = result[11]?.ToString(),
                TimeZone = result[12]?.ToString(),
                RevenueAttributed = result[13]?.ToString(),
                LandedRevenue = result[14]?.ToString()
            });
        }

        public void KPIRevSumMappingResult(List<KPICloudRevenue> outputList, IDataRecord result)
        {
            outputList.Add(new KPICloudRevenue
            {
                RevSumDivision = result[0]?.ToString(),
                RevSumDivisionID = result[1]?.ToString(),
                SubProduct = result[2]?.ToString(),
                ProductName = result[3]?.ToString(),
                ProductCategory = result[4]?.ToString(),
                SolutionArea = result[5]?.ToString()
            });
        }
        public void KPICalendarResult(List<KPICloudRevenue> outputList, IDataRecord result)
        {
            outputList.Add(new KPICloudRevenue
            {
                CalendarDate = result[0]?.ToString(),
                SalesDateID = result[1]?.ToString(),
                MonthYearName = result[2]?.ToString(),
                MonthID = result[3]?.ToString(),
                Quarter = result[4]?.ToString(),
                Year = result[5]?.ToString()
            });
        }

        public void SMBVT_Opportunities(List<KPICloudRevenue> outputList, IDataRecord result)
        {
            outputList.Add(new KPICloudRevenue
            {
                VendorName = result[0]?.ToString(),
                AreaName = result[1]?.ToString(),
                RegionName = result[2]?.ToString(),
                SubsidiaryName = result[3]?.ToString(),
                SalesMotion = result[4]?.ToString(),
                SalesPlay = result[5]?.ToString(),
                SourceSubtype = result[6]?.ToString(),
                OpportunityRevenueStatus = result[7]?.ToString(),
                OpportunityNumber = result[9]?.ToString(),
                IsDCLM = result[10]?.ToString(),
                CreatedDate = result[11]?.ToString()
            });
        }


        public void SMBVT_Leads(List<KPICloudRevenue> outputList, IDataRecord result)
        {
            outputList.Add(new KPICloudRevenue
            {
                VendorName = result[0]?.ToString(),
                AreaName = result[1]?.ToString(),
                RegionName = result[2]?.ToString(),
                SubsidiaryName = result[3]?.ToString(),
                SalesMotion = result[4]?.ToString(),
                SalesPlay = result[5]?.ToString(),
                LeadSourceSubtype = result[6]?.ToString(),
                LeadNumber = result[7]?.ToString(),
                CreatedDate = result[8]?.ToString()
            });
        }
        public void KPIResults_NPSA(List<KPICloudRevenue> outputList, IDataRecord result)
        {
            outputList.Add(new KPICloudRevenue
            {
                VendorName = result[0]?.ToString(),
                AreaName = result[1]?.ToString(),
                RegionName = result[2]?.ToString(),
                SubsidiaryName = result[3]?.ToString(),
                SalesMotion = result[4]?.ToString(),
                Quarter = result[5]?.ToString(),
                Month = result[6]?.ToString(),
                RevSumDivision = result[7]?.ToString(),
                IsDCLM = result[8]?.ToString(),
                SubRegion = result[9]?.ToString(),
                TimeZone = result[10]?.ToString(),
                NPSAMTD = result[11]?.ToString()
                //NPSAPM = result[9]?.ToString()
            });
        }

        public void KPIResults_Renewals(List<KPICloudRevenue> outputList, IDataRecord result)
        {
            outputList.Add(new KPICloudRevenue
            {
                VendorName = result[0]?.ToString(),
                TimeZone = result[1]?.ToString(),
                AreaName = result[2]?.ToString(),
                RegionName = result[3]?.ToString(),
                SubRegion = result[4]?.ToString(),
                SubsidiaryName = result[5]?.ToString(),
                Quarter = result[6]?.ToString(),
                DataSource = result[7].ToString(),
                ExclusionFlag = result[8]?.ToString(),
                ExpVolume = result[9].ToString(),
                RenVolume = result[10]?.ToString(),
                OnTimeRenVolume = result[11]?.ToString()


            });
        }

        /// <summary>
        /// To Insert revenue data from SSAS into DB
        /// </summary>
        public void InsertSSASCubeData(string TableName, DateTime ProcessStartTime)
        {
            int RetryCount = 0;
        Retrigger:
            DateTime StartTime = DateTime.UtcNow;
            Stopwatch timer = new Stopwatch();
            timer.Start();
            _log.LogInformation("Started Insertion SSAS Cube data");
            int i = 0;
            List<KPICloudRevenue> revenueList = GetDataFromAzureAnalysisService(TableName);

            foreach (var item in revenueList.Where(w => w.VendorName == "CISC" && (w.Quarter == "FY21-Q3" || w.Quarter == "FY21-Q4")))
            {
                item.VendorName = "Webhelp";
            }
            foreach (var item in revenueList.Where(w => w.AreaName != null && w.RegionName != null && w.AreaName.ToUpper().TrimEnd().TrimStart() == "LATAM" && w.RegionName.ToUpper().TrimEnd().TrimStart() != "BRAZIL"))
            {
                //item.RegionName = "ROLA";
                if (item.RegionName.ToUpper().TrimEnd().TrimStart() == "MEXICO"
                    || item.RegionName.ToUpper().TrimEnd().TrimStart() == "CENTRAL AND CARIBBEAN REGION")
                {
                    item.RegionName = "NOLA";
                }
                else if (item.RegionName.ToUpper().TrimEnd().TrimStart() == "ARGENTINA"
                    || item.RegionName.ToUpper().TrimEnd().TrimStart() == "CHILE"
                    || item.RegionName.ToUpper().TrimEnd().TrimStart() == "ANDEAN AND SOUTH REGION")
                {
                    item.RegionName = "SOLA";
                }
            }
            DataTable _dtRevenueDetails = new DataTable();
            try
            {
                if (revenueList.Count > 0)
                {
                    if (TableName == "[dbo].[RevSumMapping]")
                    {
                        _dtRevenueDetails.Columns.Add("RevSumDivisionName", typeof(string));
                        _dtRevenueDetails.Columns.Add("ProductName", typeof(string));
                        _dtRevenueDetails.Columns.Add("ProductCategory", typeof(string));
                        _dtRevenueDetails.Columns.Add("SolutionArea", typeof(string));
                        _dtRevenueDetails.Columns.Add("RevSumDivisionID", typeof(string));
                        _dtRevenueDetails.Columns.Add("SubProduct", typeof(string));
                    }
                    else if (TableName == "[dbo].[Calendar]")
                    {
                        _dtRevenueDetails.Columns.Add("Year", typeof(string));
                        _dtRevenueDetails.Columns.Add("Quarter", typeof(string));
                        _dtRevenueDetails.Columns.Add("Month Year Name", typeof(string));
                        _dtRevenueDetails.Columns.Add("MonthID", typeof(string));
                        _dtRevenueDetails.Columns.Add("Calendar Date", typeof(DateTime));
                        _dtRevenueDetails.Columns.Add("SalesDateID", typeof(string));
                    }
                    else
                    {
                        if (TableName != "[dbo].[KPIResults_Renewals]" && TableName != "[dbo].[SMBVT_Opportunities]" && TableName != "[dbo].[SMBVT_Leads]" && TableName != "[dbo].[KPIResults_RevenueForecast]")
                        {
                            _dtRevenueDetails.Columns.Add("Month", typeof(string));
                        }
                        _dtRevenueDetails.Columns.Add("VendorName", typeof(string));
                        _dtRevenueDetails.Columns.Add("AreaName", typeof(string));
                        _dtRevenueDetails.Columns.Add("RegionName", typeof(string));
                        _dtRevenueDetails.Columns.Add("SubsidiaryName", typeof(string));
                        if (TableName != "[dbo].[KPIResults_RevenueForecast]" && TableName != "[dbo].[KPIResults_Renewals]")
                        {
                            _dtRevenueDetails.Columns.Add("SalesMotion", typeof(string));
                        }

                        if (TableName != "[dbo].[SMBVT_Opportunities]" && TableName != "[dbo].[SMBVT_Leads]" && TableName != "[dbo].[KPIResults_RevenueForecast]")
                        {
                            _dtRevenueDetails.Columns.Add("Quarter", typeof(string));
                        }
                        if (TableName != "[dbo].[SMBVT_Leads]"  && TableName != "[dbo].[KPIResults_Renewals]")
                        {
                            _dtRevenueDetails.Columns.Add("IsDCLM", typeof(string));
                        }

                    }

                    if (TableName == "[dbo].[KPIResults_CustomerAdds]")
                    {
                        _dtRevenueDetails.Columns.Add("ProductName", typeof(string));
                        _dtRevenueDetails.Columns.Add("CustomerAdds", typeof(string));
                        _dtRevenueDetails.Columns.Add("Sub Region", typeof(string));
                        _dtRevenueDetails.Columns.Add("Time Zone", typeof(string));
                    }
                    else if (TableName == "[dbo].[KPIResults_CloudRevenue]")
                    {
                        _dtRevenueDetails.Columns.Add("ProductCategory", typeof(string));
                        _dtRevenueDetails.Columns.Add("RevenueAttributed", typeof(decimal));
                        _dtRevenueDetails.Columns.Add("ProductName", typeof(string));
                        _dtRevenueDetails.Columns.Add("SolutionAreaName", typeof(string));
                        _dtRevenueDetails.Columns.Add("LandedRevenue", typeof(decimal));
                        _dtRevenueDetails.Columns.Add("Sub Region", typeof(string));
                        _dtRevenueDetails.Columns.Add("Time Zone", typeof(string));
                    }
                    else if (TableName == "[dbo].[KPIResults_AzureConsumedRevenue]")
                    {
                        _dtRevenueDetails.Columns.Add("ProductName", typeof(string));
                        _dtRevenueDetails.Columns.Add("ReportedRevenueACR", typeof(decimal));
                        _dtRevenueDetails.Columns.Add("Sub Region", typeof(string));
                        _dtRevenueDetails.Columns.Add("Time Zone", typeof(string));
                    }
                    else if (TableName == "[dbo].[KPIResults_NPSA]")
                    {
                        _dtRevenueDetails.Columns.Add("RevSumDivision", typeof(string));
                        _dtRevenueDetails.Columns.Add("NPSAMTD", typeof(string));
                        _dtRevenueDetails.Columns.Add("NPSAPM", typeof(string));
                        _dtRevenueDetails.Columns.Add("Sub Region", typeof(string));
                        _dtRevenueDetails.Columns.Add("Time Zone", typeof(string));
                    }
                    else if (TableName == "[dbo].[KPIResults_Renewals]")
                    {
                        _dtRevenueDetails.Columns.Add("ExpVolume", typeof(string));
                        _dtRevenueDetails.Columns.Add("RenVolume", typeof(string));
                        _dtRevenueDetails.Columns.Add("OnTimeRenVolume", typeof(string));
                        _dtRevenueDetails.Columns.Add("DataSource", typeof(string));
                        _dtRevenueDetails.Columns.Add("Sub Region", typeof(string));
                        _dtRevenueDetails.Columns.Add("Time Zone", typeof(string));
                        _dtRevenueDetails.Columns.Add("ExclusionFlag", typeof(string));
                    }
                    else if (TableName == "[dbo].[SMBVT_Opportunities]")
                    {
                        _dtRevenueDetails.Columns.Add("Sales Play", typeof(string));
                        _dtRevenueDetails.Columns.Add("Source Subtype", typeof(string));
                        _dtRevenueDetails.Columns.Add("Opportunity Revenue Status", typeof(string));
                        _dtRevenueDetails.Columns.Add("Opportunity Number", typeof(string));
                        _dtRevenueDetails.Columns.Add("Created Date", typeof(DateTime));
                    }
                    else if (TableName == "[dbo].[SMBVT_Leads]")
                    {
                        _dtRevenueDetails.Columns.Add("Sales Play", typeof(string));
                        _dtRevenueDetails.Columns.Add("Lead Source Subtype", typeof(string));
                        _dtRevenueDetails.Columns.Add("Lead Number", typeof(string));
                        _dtRevenueDetails.Columns.Add("Created Date", typeof(DateTime));
                    }
                    else if (TableName == "[dbo].[KPIResults_RevenueForecast]")
                    {
                        _dtRevenueDetails.Columns.Add("Sales Quarter", typeof(string));
                        _dtRevenueDetails.Columns.Add("Sales Month Year", typeof(string));
                        _dtRevenueDetails.Columns.Add("Product Category", typeof(string));
                        _dtRevenueDetails.Columns.Add("Product", typeof(string));
                        _dtRevenueDetails.Columns.Add("Sub Region", typeof(string));
                        _dtRevenueDetails.Columns.Add("Time Zone", typeof(string));
                        _dtRevenueDetails.Columns.Add("Attributed Revenue", typeof(decimal));
                        _dtRevenueDetails.Columns.Add("Future Monthly Subscription Revenue Forecast", typeof(decimal));
                        _dtRevenueDetails.Columns.Add("Projected Revenue", typeof(decimal));
                        _dtRevenueDetails.Columns.Add("Total Revenue Outlook", typeof(decimal));

                    }
                    //EVALUATE SUMMARIZECOLUMNS(Vendors[Vendor],'Upper Geography'[Area],'Upper Geography'[Region],'Upper Geography'[Subsidiary],'Sales Date'[Sales Quarter],'Sales Date'[Sales Month Year], Products[Product Category], Products[Product],'Upper Geography'[Sub Region],'Upper Geography'[Time Zone],""Attributed Revenue"", [Attributed Revenue],""Future Monthly Subscription Revenue Forecast"", [Future Monthly Subscription Revenue Forecast], ""Projected Revenue"", [Projected Revenue], ""Total Revenue Outlook"", [Total Revenue Outlook]
                    foreach (var r in revenueList)
                    {
                        i++;
                        DataRow drRevenueNumbers = _dtRevenueDetails.NewRow();
                        if (TableName == "[dbo].[RevSumMapping]")
                        {
                            drRevenueNumbers["RevSumDivisionName"] = r.RevSumDivision;
                            drRevenueNumbers["ProductName"] = r.ProductName;
                            drRevenueNumbers["ProductCategory"] = r.ProductCategory;
                            drRevenueNumbers["SolutionArea"] = r.SolutionArea;
                            drRevenueNumbers["RevSumDivisionID"] = r.RevSumDivisionID;
                            drRevenueNumbers["SubProduct"] = r.SubProduct;

                        }
                        else if (TableName == "[dbo].[Calendar]")
                        {
                            drRevenueNumbers["Year"] = r.Year;
                            drRevenueNumbers["Quarter"] = r.Quarter;
                            drRevenueNumbers["Month Year Name"] = r.MonthYearName;
                            drRevenueNumbers["MonthID"] = r.MonthID;
                            drRevenueNumbers["Calendar Date"] = r.CalendarDate;
                            drRevenueNumbers["SalesDateID"] = r.SalesDateID;
                        }
                        else
                        {
                            if (TableName != "[dbo].[KPIResults_Renewals]" && TableName != "[dbo].[SMBVT_Opportunities]" && TableName != "[dbo].[SMBVT_Leads]" && TableName != "[dbo].[KPIResults_RevenueForecast]")
                            {
                                drRevenueNumbers["Month"] = r.Month;
                            }
                            drRevenueNumbers["VendorName"] = r.VendorName;
                            drRevenueNumbers["AreaName"] = r.AreaName;
                            drRevenueNumbers["RegionName"] = r.RegionName;
                            drRevenueNumbers["SubsidiaryName"] = r.SubsidiaryName;
                            if (TableName != "[dbo].[KPIResults_RevenueForecast]" && TableName != "[dbo].[KPIResults_Renewals]")
                            {
                                drRevenueNumbers["SalesMotion"] = r.SalesMotion;
                            }

                            if (TableName != "[dbo].[SMBVT_Opportunities]" && TableName != "[dbo].[SMBVT_Leads]" && TableName != "[dbo].[KPIResults_RevenueForecast]")
                            {
                                drRevenueNumbers["Quarter"] = r.Quarter;
                            }
                            if (TableName != "[dbo].[SMBVT_Leads]"  && TableName != "[dbo].[KPIResults_Renewals]")
                            {
                                drRevenueNumbers["IsDCLM"] = r.IsDCLM;
                            }
                        }

                        if (TableName == "[dbo].[KPIResults_CustomerAdds]")
                        {
                            drRevenueNumbers["ProductName"] = r.ProductName;
                            drRevenueNumbers["CustomerAdds"] = r.CustomerAdds;
                            drRevenueNumbers["Sub Region"] = r.SubRegion;
                            drRevenueNumbers["Time Zone"] = r.TimeZone;
                        }
                        else if (TableName == "[dbo].[KPIResults_CloudRevenue]")
                        {
                            drRevenueNumbers["ProductCategory"] = r.ProductCategory;
                            drRevenueNumbers["RevenueAttributed"] = r.RevenueAttributed;
                            drRevenueNumbers["ProductName"] = r.ProductName;
                            drRevenueNumbers["SolutionAreaName"] = r.SolutionArea;
                            drRevenueNumbers["LandedRevenue"] = r.LandedRevenue;
                            drRevenueNumbers["Sub Region"] = r.SubRegion;
                            drRevenueNumbers["Time Zone"] = r.TimeZone;
                        }
                        else if (TableName == "[dbo].[KPIResults_AzureConsumedRevenue]")
                        {
                            drRevenueNumbers["ProductName"] = r.ProductName;
                            drRevenueNumbers["ReportedRevenueACR"] = r.ReportedRevenueACR;
                            drRevenueNumbers["Sub Region"] = r.SubRegion;
                            drRevenueNumbers["Time Zone"] = r.TimeZone;
                        }
                        else if (TableName == "[dbo].[KPIResults_NPSA]")
                        {
                            drRevenueNumbers["RevSumDivision"] = r.RevSumDivision;
                            drRevenueNumbers["NPSAMTD"] = r.NPSAMTD;
                            drRevenueNumbers["NPSAPM"] = r.NPSAPM;
                            drRevenueNumbers["Sub Region"] = r.SubRegion;
                            drRevenueNumbers["Time Zone"] = r.TimeZone;
                        }
                        else if (TableName == "[dbo].[KPIResults_Renewals]")
                        {
                            drRevenueNumbers["ExpVolume"] = r.ExpVolume;
                            drRevenueNumbers["RenVolume"] = r.RenVolume;
                            drRevenueNumbers["OnTimeRenVolume"] = r.OnTimeRenVolume;
                            drRevenueNumbers["DataSource"] = r.DataSource;
                            drRevenueNumbers["Time Zone"] = r.TimeZone;
                            drRevenueNumbers["Sub Region"] = r.SubRegion;
                            drRevenueNumbers["ExclusionFlag"] = r.ExclusionFlag;
                        }
                        else if (TableName == "[dbo].[SMBVT_Opportunities]")
                        {
                            drRevenueNumbers["Sales Play"] = r.SalesPlay;
                            drRevenueNumbers["Source Subtype"] = r.SourceSubtype;
                            drRevenueNumbers["Opportunity Revenue Status"] = r.OpportunityRevenueStatus;
                            drRevenueNumbers["Opportunity Number"] = r.OpportunityNumber;
                            drRevenueNumbers["Created Date"] = string.IsNullOrWhiteSpace(r.CreatedDate) ? (object)DBNull.Value : Convert.ToDateTime(r.CreatedDate);
                        }
                        else if (TableName == "[dbo].[SMBVT_Leads]")
                        {
                            drRevenueNumbers["Sales Play"] = r.SalesPlay;
                            drRevenueNumbers["Lead Source Subtype"] = r.LeadSourceSubtype;
                            drRevenueNumbers["Lead Number"] = r.LeadNumber;
                            drRevenueNumbers["Created Date"] = string.IsNullOrWhiteSpace(r.CreatedDate) ? (object)DBNull.Value : Convert.ToDateTime(r.CreatedDate);
                        }
                        else if (TableName == "[dbo].[KPIResults_RevenueForecast]")
                        {
                            drRevenueNumbers["Sales Quarter"] = r.Quarter;
                            drRevenueNumbers["Sales Month Year"] = r.Month;
                            drRevenueNumbers["Product Category"] = r.ProductCategory;
                            drRevenueNumbers["Product"] = r.ProductName;
                            drRevenueNumbers["Sub Region"] = r.SubRegion;
                            drRevenueNumbers["Time Zone"] = r.TimeZone;
                            drRevenueNumbers["Attributed Revenue"] = r.RevenueAttributed != null ? Decimal.Parse(r.RevenueAttributed, System.Globalization.NumberStyles.Float) : (object)DBNull.Value;
                            drRevenueNumbers["Future Monthly Subscription Revenue Forecast"] = r.RevenueForecast != null ? Decimal.Parse(r.RevenueForecast, System.Globalization.NumberStyles.Float) : (object)DBNull.Value;
                            drRevenueNumbers["Projected Revenue"] = r.ProjectedRevenue != null ? Decimal.Parse(r.ProjectedRevenue, System.Globalization.NumberStyles.Float) : (object)DBNull.Value;
                            drRevenueNumbers["Total Revenue Outlook"] = r.TotalRevenueOutlook != null ? Decimal.Parse(r.TotalRevenueOutlook, System.Globalization.NumberStyles.Float) : (object)DBNull.Value;

                        }
                        if(TableName == "[dbo].[KPIResults_RevenueForecast]")
                        {
                            if(r.IsDCLM == "No")
                            {
                                _dtRevenueDetails.Rows.Add(drRevenueNumbers);
                            }
                        }
                        else
                        {
                            _dtRevenueDetails.Rows.Add(drRevenueNumbers);
                        }

                        if (i == revenueList.Count)
                        {
                            StringBuilder tableName = new StringBuilder();

                            if (TableName == "[dbo].[KPIResults_CustomerAdds]")
                            {
                                tableName = new StringBuilder("[staging].[KPIResults_CustomerAdds]");
                            }
                            else if (TableName == "[dbo].[KPIResults_CloudRevenue]")
                            {
                                tableName = new StringBuilder("[staging].[KPIResults_CloudRevenue]");
                            }
                            else if (TableName == "[dbo].[KPIResults_AzureConsumedRevenue]")
                            {
                                tableName = new StringBuilder("[staging].[KPIResults_AzureConsumedRevenue]");
                            }
                            else if (TableName == "[dbo].[KPIResults_NPSA]")
                            {
                                tableName = new StringBuilder("[staging].[KPIResults_NPSA]");
                            }
                            else if (TableName == "[dbo].[KPIResults_Renewals]")
                            {
                                tableName = new StringBuilder("[staging].[KPIResults_Renewals]");
                            }
                            else if (TableName == "[dbo].[RevSumMapping]")
                            {
                                tableName = new StringBuilder("[staging].[RevSumMapping]");
                            }
                            else if (TableName == "[dbo].[Calendar]")
                            {
                                tableName = new StringBuilder("[staging].[Calendar]");
                            }
                            else if (TableName == "[dbo].[SMBVT_Opportunities]")
                            {
                                tableName = new StringBuilder("[staging].[SMBVT_Opportunities]");
                            }
                            else if (TableName == "[dbo].[SMBVT_Leads]")
                            {
                                tableName = new StringBuilder("[staging].[SMBVT_Leads]");
                            }
                            else if (TableName == "[dbo].[KPIResults_RevenueForecast]")
                            {
                                tableName = new StringBuilder("[staging].[KPIResults_RevenueForecast]");
                            }
                            _manager.InsertDataIntoSQLDatabase(tableName.ToString(), _dtRevenueDetails);

                            if (_dtRevenueDetails.Rows.Count > 0)
                            {
                                SqlConnection _con = new SqlConnection(_sqlConnection);
                                // AuthenticationResult _authenticationResult = _manager.AADAunthenticationResult();
                                _con.AccessToken = Manager.getAccessToken(System.Environment.GetEnvironmentVariable("ResourceId"), System.Environment.GetEnvironmentVariable("VTCPManagedIdentity"), true).Result;
                                SqlCommand _SqlCommands = new SqlCommand();
                                _SqlCommands = new SqlCommand("RenameAndReindexTables", _con);
                                _SqlCommands.CommandType = CommandType.StoredProcedure;
                                _SqlCommands.Parameters.AddWithValue("@TableName", TableName.Replace("[dbo].[", "").TrimEnd(']'));
                                _con.Open();
                                _SqlCommands.ExecuteNonQuery();
                                _con.Close();
                            }
                            else
                            {
                                RetryCount++;
                                if (RetryCount > 3)
                                {
                                    throw new InvalidOperationException(tableName + ": Row count is empty");
                                }
                                goto Retrigger;
                            }

                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _log.LogError("Error Details UpdateSSASDetails Failed" + ex.Message);
            }
            DateTime EndTime = DateTime.UtcNow;
            SqlConnection con = new SqlConnection(SSASRevenueDetailsManager._sqlConnection);
            //AuthenticationResult authenticationResult = _manager.AADAunthenticationResult();
            con.AccessToken = Manager.getAccessToken(System.Environment.GetEnvironmentVariable("ResourceId"), System.Environment.GetEnvironmentVariable("VTCPManagedIdentity"), true).Result;
            SqlCommand SqlCommands = new SqlCommand();
            SqlCommands = new SqlCommand("AzureFunctionsDataLoad", con);
            SqlCommands.CommandType = CommandType.StoredProcedure;
            SqlCommands.Parameters.AddWithValue("@FunctionName", "RevenueDetailsSSAS" + "-" + TableName);
            SqlCommands.Parameters.AddWithValue("@ProcessStartTime", ProcessStartTime);
            SqlCommands.Parameters.AddWithValue("@FunctionStartTime", StartTime);
            SqlCommands.Parameters.AddWithValue("@FunctionEndTime", EndTime);
            SqlCommands.Parameters.AddWithValue("@InitialRowsAffected", revenueList.Count.ToString());
            SqlCommands.Parameters.AddWithValue("@TotalRowsAffected", _dtRevenueDetails.Rows.Count.ToString());
            con.Open();
            SqlCommands.ExecuteNonQuery();
            con.Close();

        }

        #endregion
    }
}


