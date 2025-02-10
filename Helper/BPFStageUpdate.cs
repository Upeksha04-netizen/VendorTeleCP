using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.Extensions.Logging;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Tooling.Connector;

namespace Helper
{
    public class BPFStageUpdate
    {
        public int _activeStagePosition;
        public string _procInstanceLogicalName;
        public Guid _activeStageId;
        public Guid _processLeadId;
        public string _activeStageName;
        public bool moveToNextStage = true;
        public Tuple<Entity,Guid> UpdateBPFStage(CrmServiceClient service, Guid leadid, ILogger log)
        {
            Entity retrievedProcessInstance = null;
            // Verify that an instance of "Lead Process" is created for the new Lead record.
            RetrieveProcessInstancesRequest procLeadReq = new RetrieveProcessInstancesRequest
            {
                EntityId = leadid,
                EntityLogicalName = "lead"
            };
            RetrieveProcessInstancesResponse procLeadResp = (RetrieveProcessInstancesResponse)service.Execute(procLeadReq);

            if (procLeadResp.Processes.Entities.Count > 0)
            {
                // First record is the active process instance
                var processLeadInstance = procLeadResp.Processes.Entities[0];
                // Id of the active process instance, which will be used
                _processLeadId = processLeadInstance.Id;
                log.LogInformation("Process instance automatically created for the new Lead record: '{0}'", processLeadInstance["name"]);

                _procInstanceLogicalName = "new_leadprocess";

                // Retrieve the active stage ID of the active process instance
                _activeStageId = new Guid(processLeadInstance.Attributes["processstageid"].ToString());

                // Retrieve the process stages in the active path of the current process instance
                RetrieveActivePathRequest pathReq = new RetrieveActivePathRequest
                {
                    ProcessInstanceId = _processLeadId
                };
                RetrieveActivePathResponse pathResp = (RetrieveActivePathResponse)service.Execute(pathReq);


                for (int i = 0; i < pathResp.ProcessStages.Entities.Count; i++)
                {
                    log.LogInformation("\tStage {0}: {1} (StageId: {2})", i + 1,
                        pathResp.ProcessStages.Entities[i].Attributes["stagename"], pathResp.ProcessStages.Entities[i].Attributes["processstageid"]);


                    // Retrieve the active stage name and active stage position based on the activeStageId for the process instance
                    if (pathResp.ProcessStages.Entities[i].Attributes["processstageid"].ToString() == _activeStageId.ToString())
                    {
                        _activeStageName = pathResp.ProcessStages.Entities[i].Attributes["stagename"].ToString();
                        _activeStagePosition = i;
                        if (_activeStageName.ToUpper() == "NOMINATE" || _activeStageName.ToUpper() == "ENGAGE")
                        {
                            moveToNextStage = false;
                        }
                    }
                }

                if (moveToNextStage)
                {
                    // Retrieve the stage ID of the next stage that you want to set as active
                    _activeStageId = (Guid)pathResp.ProcessStages.Entities[_activeStagePosition + 1].Attributes["processstageid"];

                    // Retrieve the process instance record to update its active stage
                    ColumnSet cols1 = new ColumnSet();
                    cols1.AddColumn("activestageid");
                    retrievedProcessInstance = service.Retrieve(_procInstanceLogicalName, _processLeadId, cols1);

                    #region keeping code for future reference
                    // Update the active stage to the next stage
                    //retrievedProcessInstance["activestageid"] = new EntityReference("processstage", _activeStageId);
                    //service.Update(retrievedProcessInstance);


                    // Retrieve the process instance record again to verify its active stage information
                    //ColumnSet cols2 = new ColumnSet();
                    //cols2.AddColumn("activestageid");
                    //Entity retrievedProcessInstance1 = service.Retrieve(_procInstanceLogicalName, _processLeadId, cols2);

                    //EntityReference activeStageInfo = retrievedProcessInstance1["activestageid"] as EntityReference;
                    //if (activeStageInfo.Id == _activeStageId)
                    //{
                    //    trace.Trace("\nChanged active stage for the process instance to: '{0}' (StageID: {1})",
                    //                  activeStageInfo.Name, activeStageInfo.Id);
                    //}
                    #endregion
                }
            }
            return new Tuple<Entity, Guid>(retrievedProcessInstance,_activeStageId);
        }
    }

}
