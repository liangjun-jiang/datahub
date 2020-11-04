package com.linkedin.metadata.kafka.spline;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.DataProcessUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.DataProcessAspect;
import com.linkedin.metadata.aspect.DatasetAspect;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.snapshot.DataProcessSnapshot;
import com.linkedin.metadata.snapshot.DatasetSnapshot;
import java.util.Date;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.springframework.beans.factory.annotation.Qualifier;


public class SplineRestService {
  SplineModelDTO _splineModelDTO;

  private EbeanLocalDAO<DatasetAspect, DatasetUrn> _datasetEbeanDAO;

  private EbeanLocalDAO<DataProcessAspect, DataProcessUrn> _dataProcessEbeanDAO;

  private ExecutionPlan _executionPlan;

  public SplineRestService(
      @Qualifier(DatasetDaoFactory.DATASET_DAO_NAME) EbeanLocalDAO<DatasetAspect, DatasetUrn> datasetEbeanDAO,
      @Qualifier(DataProcessDAOFactory.DATA_PROCESS_DAO_NAME) EbeanLocalDAO<DataProcessAspect, DataProcessUrn> dataProcessEbeanDAO
  ) {
    _datasetEbeanDAO = datasetEbeanDAO;
    _dataProcessEbeanDAO = dataProcessEbeanDAO;
  }

  public void setExecutionPlan(ExecutionPlan executionPlan) {
    _executionPlan = executionPlan;
  }

  public void persistEntities() {
    _splineModelDTO = new SplineModelDTO(_executionPlan);
    persistDatasetSnapshots(_splineModelDTO.getDatasetSnapshotList());
    persistDataProcessSnapshots(_splineModelDTO.getDataProcessSnapshotList());
  }

  private void persistDatasetSnapshots(List<DatasetSnapshot> datasetSnapshotList) {
    for (DatasetSnapshot datasetSnapshot: datasetSnapshotList) {
      persistDatasetSnapshot(datasetSnapshot);
    }
  }

  private void persistDataProcessSnapshots(List<DataProcessSnapshot> dataProcessSnapshotList) {
    for( DataProcessSnapshot dataProcessSnapshot: dataProcessSnapshotList) {
      persistDataProcessSnapshot(dataProcessSnapshot);
    }
  }

  private void persistDatasetSnapshot(DatasetSnapshot datasetSnapshot) {
    List<RecordTemplate> aspects = ModelUtils.getAspectsFromSnapshot(datasetSnapshot);
    for(RecordTemplate aspect: aspects) {
      _datasetEbeanDAO.add(datasetSnapshot.getUrn(), aspect,
          makeAuditStamp(new CorpuserUrn("datahub"), null, new Date().getTime())); //TODO: figure out this logic
    }
  }

  private void persistDataProcessSnapshot(DataProcessSnapshot dataProcessSnapshot) {
    List<RecordTemplate> aspects = ModelUtils.getAspectsFromSnapshot(dataProcessSnapshot);
    for(RecordTemplate aspect: aspects) {
      _dataProcessEbeanDAO.add(dataProcessSnapshot.getUrn(), aspect,
          makeAuditStamp(new CorpuserUrn("datahub"), null, new Date().getTime())); //TODO: figure out this logic
    }
  }

  @Nonnull
  public static AuditStamp makeAuditStamp(@Nonnull Urn actorUrn, @Nullable Urn impersonatorUrn, long time) {
    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(time);
    auditStamp.setActor(actorUrn);
    if (impersonatorUrn != null) {
      auditStamp.setImpersonator(impersonatorUrn);
    }
    return auditStamp;
  }
}
