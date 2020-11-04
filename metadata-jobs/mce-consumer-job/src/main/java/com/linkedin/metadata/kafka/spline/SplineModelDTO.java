package com.linkedin.metadata.kafka.spline;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.DatasetUrnArray;
import com.linkedin.common.FabricType;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.InstitutionalMemoryMetadata;
import com.linkedin.common.InstitutionalMemoryMetadataArray;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.url.Url;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DataProcessUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.dataprocess.DataProcessInfo;
import com.linkedin.dataset.DatasetLineageType;
import com.linkedin.dataset.Upstream;
import com.linkedin.dataset.UpstreamArray;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.metadata.aspect.DataProcessAspect;
import com.linkedin.metadata.aspect.DataProcessAspectArray;
import com.linkedin.metadata.aspect.DatasetAspect;
import com.linkedin.metadata.aspect.DatasetAspectArray;
import com.linkedin.metadata.snapshot.DataProcessSnapshot;
import com.linkedin.metadata.snapshot.DatasetSnapshot;
import com.linkedin.schema.ArrayType;
import com.linkedin.schema.BooleanType;
import com.linkedin.schema.BytesType;
import com.linkedin.schema.MapType;
import com.linkedin.schema.MySqlDDL;
import com.linkedin.schema.NumberType;
import com.linkedin.schema.RecordType;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaFieldDataType;
import com.linkedin.schema.SchemaMetadata;
import com.linkedin.schema.StringType;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;


public class SplineModelDTO {
  private List<DatasetSnapshot> datasetSnapshotList;
  private List<DataProcessSnapshot> dataProcessSnapshotList;
  private ExecutionPlan executionPlan;
  private List<java.util.Map<String, String>> dataTypeList;
  private List<java.util.Map<String, String>> attributeList;
  private java.util.Map<String, String> customConfig;
  private java.util.Map<String, java.util.Map<String, String>> dataTypeListMap;
  private java.util.Map<String, java.util.Map<String, String>> attributeListMap;
  private List<ReadOperation> readOperations;
  private WriteOperation writeOperation;
  private scala.collection.immutable.Map<String, Object> extraInfo;
  private SystemInfo systemInfo;
  private String dataPlatformName;
  private String appName;
  private List<DatasetSnapshot> readSnapshots;
  private DatasetSnapshot writeSnapshot;
  private AuditStamp auditStamp;

  public SplineModelDTO(ExecutionPlan executionPlan) {
    this.executionPlan = executionPlan;
    this.datasetSnapshotList = new ArrayList<>();
    this.dataProcessSnapshotList = new ArrayList<>();
    this.dataTypeList = new ArrayList<>();
    this.attributeList = new ArrayList<>();
    this.dataTypeListMap = new HashMap<>();
    this.attributeListMap = new HashMap<>();
    this.readOperations = scala.collection.JavaConverters.seqAsJavaList(this.executionPlan.operations().reads());
    this.writeOperation = this.executionPlan.operations().write();
    this.extraInfo = this.executionPlan.extraInfo();
    this.systemInfo = this.executionPlan.systemInfo();
    this.dataPlatformName = String.valueOf(systemInfo.name());
    this.appName = String.valueOf(extraInfo.get("appName"));

    Optional<$colon$colon> customConfigOptional = Optional.ofNullable(extraInfo.get("config").getOrElse(null));
    Optional<$colon$colon> dataTypesOptional = Optional.ofNullable(extraInfo.get("dataTypes").getOrElse(null));
    Optional<$colon$colon> attributesOptional = Optional.ofNullable(extraInfo.get("attributes").getOrElse(null));

    this.customConfig = scalaMapList2JavaMap(JavaConverters.seqAsJavaList(customConfigOptional.get()));
    this.dataTypeList = scalaMapList2JavaMapList(JavaConverters.seqAsJavaList(dataTypesOptional.get()));
    this.attributeList = scalaMapList2JavaMapList(JavaConverters.seqAsJavaList(attributesOptional.get()));
    this.dataTypeListMap = dataTypeIdMapDataField(dataTypeList);
    this.attributeListMap = schemaIdMapAttribute(attributeList);
    this.readSnapshots = new ArrayList<>();
    this.writeSnapshot = new DatasetSnapshot();
    init();
  }

  public void init() {
    setAuditStamp(this.customConfig.getOrDefault("owner", "datahub"));
    createDatasetSnapshotList();
    createDataProcessSnapshotList();
  }

  public List<DataProcessSnapshot> getDataProcessSnapshotList() {
    return this.dataProcessSnapshotList;
  }

  public List<DatasetSnapshot> getDatasetSnapshotList() {
    return this.datasetSnapshotList;
  }

  private void createDatasetSnapshotList() {
    this.readOperations.forEach( readOperation -> {
      this.readSnapshots.add(assembleReadDatasetSnapshot(readOperation));
    });

    this.readSnapshots.forEach( datasetSnapshot -> {
      this.datasetSnapshotList.add(datasetSnapshot);
    });

    this.datasetSnapshotList.add(assembleWriteDatasetSnapshot(this.writeOperation));
  }

  private void createDataProcessSnapshotList() {
    this.dataProcessSnapshotList.add(assembleDataProcessSnapshot());
  }

  private DatasetSnapshot assembleReadDatasetSnapshot(ReadOperation read) {
    DatasetSnapshot datasetSnapshot = new DatasetSnapshot();
    List<String> inputSources = JavaConverters.seqAsJavaList(read.inputSources());
    String sourceName = extractInputSourceToFileName(inputSources.get(0));
    DatasetUrn datasetUrn = assembleDatasetUrn(sourceName);
    datasetSnapshot.setUrn(datasetUrn);

    DatasetAspectArray datasetAspectArray = new DatasetAspectArray();

    datasetAspectArray.add(datasetOwnershipAspect());
    datasetAspectArray.add(institutionalMemoryDatasetAspect());

    Optional<$colon$colon> schemasOptional = Optional.ofNullable(read.schema().getOrElse(null));
    List<String> schemaList =  JavaConverters.seqAsJavaList(schemasOptional.get());

    datasetAspectArray.add(schemaMetadataDatasetAspect(datasetUrn, schemaList));

    datasetSnapshot.setAspects(datasetAspectArray);

    return datasetSnapshot;
  }

  private DatasetSnapshot assembleWriteDatasetSnapshot(WriteOperation write) {

    String sourceName = extractInputSourceToFileName(write.outputSource());

    DatasetUrn datasetUrn = assembleDatasetUrn(sourceName);
    this.writeSnapshot.setUrn(datasetUrn);

    DatasetAspectArray datasetAspectArray = new DatasetAspectArray();
    datasetAspectArray.add(datasetOwnershipAspect());

    datasetAspectArray.add(institutionalMemoryDatasetAspect());

    List<String> schemaList = new ArrayList<>();
    schemaList.addAll(this.attributeListMap.keySet());

    DatasetAspect schemaMetadataAspect = schemaMetadataDatasetAspect(datasetUrn, schemaList);
    datasetAspectArray.add(schemaMetadataAspect);

    datasetAspectArray.add(upstreamLineageDatasetAspect());

    SchemaFieldArray schemaFieldArray = schemaMetadataAspect.getSchemaMetadata().getFields();
    datasetAspectArray.add(fineGrainedUpstreamLineageDatasetAspect(schemaFieldArray));
    this.writeSnapshot.setAspects(datasetAspectArray);

    return this.writeSnapshot;
  }

  private DataProcessSnapshot assembleDataProcessSnapshot() {
    DataProcessSnapshot dataProcessSnapshot = new DataProcessSnapshot();
    dataProcessSnapshot.setUrn(assembleDataProcessUrn());
    DataProcessAspectArray dataProcessAspectArray = new DataProcessAspectArray();

    dataProcessAspectArray.add(dataProcessOwnership());
    dataProcessAspectArray.add(dataProcessInfoAspect());

    dataProcessSnapshot.setAspects(dataProcessAspectArray);
    return dataProcessSnapshot;
  }

  private DatasetUrn assembleDatasetUrn(String sourceName) {
    DataPlatformUrn dataPlatformUrn = new DataPlatformUrn(this.dataPlatformName);
    return new DatasetUrn(dataPlatformUrn, sourceName, FabricType.PROD);
  }

  private DataProcessUrn assembleDataProcessUrn() {
    return new DataProcessUrn(this.dataPlatformName, this.appName, FabricType.PROD);
  }

  private DatasetAspect datasetOwnershipAspect() {
    DatasetAspect ownershipAspect = new DatasetAspect();
    Ownership ownership = new Ownership();
    OwnerArray owners = new OwnerArray();
    Owner owner = new Owner();
    owner.setOwner(new CorpuserUrn(this.customConfig.getOrDefault("owner", "datahub")));
    owner.setType(OwnershipType.DEVELOPER);
    owners.add(owner);
    ownership.setOwners(owners);
    ownership.setLastModified(getAuditStamp());
    ownershipAspect.setOwnership(ownership);
    return ownershipAspect;
  }

  private DatasetAspect institutionalMemoryDatasetAspect() {
    DatasetAspect institutionalMemoryDatasetAspect = new DatasetAspect();
    InstitutionalMemory institutionalMemory = new InstitutionalMemory();
    InstitutionalMemoryMetadataArray institutionalMemoryMetadataArray = new InstitutionalMemoryMetadataArray();
    InstitutionalMemoryMetadata metadata = new InstitutionalMemoryMetadata();
    metadata.setUrl(new Url(this.customConfig.getOrDefault("source_code", "")));
    metadata.setDescription(this.customConfig.getOrDefault("description",""));

    metadata.setCreateStamp(getAuditStamp());
    institutionalMemoryMetadataArray.add(metadata);
    institutionalMemory.setElements(institutionalMemoryMetadataArray);
    institutionalMemoryDatasetAspect.setInstitutionalMemory(institutionalMemory);
    return institutionalMemoryDatasetAspect;
  }

  private DatasetAspect schemaMetadataDatasetAspect(DatasetUrn urn, List<String> schemaList) {
    DatasetAspect datasetAspect = new DatasetAspect();
    SchemaMetadata schemaMetadata = new SchemaMetadata();
    schemaMetadata.setDataset(urn);

    schemaMetadata.setSchemaName(urn.getDatasetNameEntity());
    schemaMetadata.setPlatform(urn.getPlatformEntity());

    schemaMetadata.setVersion(0);
    schemaMetadata.setCreated(getAuditStamp());
    schemaMetadata.setLastModified(getAuditStamp());
    schemaMetadata.setHash("");

    // TODO: make this concrete
    SchemaMetadata.PlatformSchema platformSchema = new SchemaMetadata.PlatformSchema();
    MySqlDDL mySqlDDL = new MySqlDDL();
    mySqlDDL.setTableSchema("table schema");
    mySqlDDL.setDdl("");
    platformSchema.setMySqlDDL(mySqlDDL);
    schemaMetadata.setPlatformSchema(platformSchema);

    SchemaFieldArray schemaFields = new SchemaFieldArray();

    for(String singleField: schemaList) {
      SchemaField schemaField = new SchemaField();
      java.util.Map<String, String> attribute = this.attributeListMap.get(singleField);
      schemaField.setFieldPath(attribute.get("name"));

      java.util.Map<String, String> dataType = this.dataTypeListMap.get(attribute.get("dataTypeId"));
      String dataTypeName = dataType.get("name");
      if(dataTypeName != null) {
        schemaField.setNativeDataType(dataTypeName);
        schemaField.setDescription(dataTypeName);
        schemaField.setType(toSchemaFieldType(dataTypeName));
        schemaFields.add(schemaField);
      }
    };
    if (schemaFields.size() > 0) {
      schemaMetadata.setFields(schemaFields);
    }
    datasetAspect.setSchemaMetadata(schemaMetadata);
    return datasetAspect;
  }

  private DatasetAspect upstreamLineageDatasetAspect() {
    DatasetAspect datasetAspect = new DatasetAspect();
    UpstreamLineage upstreamLineage = new UpstreamLineage();
    UpstreamArray upstreamArray = new UpstreamArray();
    for(DatasetSnapshot datasetSnapshot: this.readSnapshots) {
      Upstream upstream = new Upstream();
      upstream.setAuditStamp(getAuditStamp());
      upstream.setDataset(datasetSnapshot.getUrn());
      upstream.setType(DatasetLineageType.TRANSFORMED);
      upstreamArray.add(upstream);
    }

    upstreamLineage.setUpstreams(upstreamArray);
    datasetAspect.setUpstreamLineage(upstreamLineage);

    return datasetAspect;
  }

  private DatasetAspect fineGrainedUpstreamLineageDatasetAspect(SchemaFieldArray schemaFieldArray) {
    DatasetAspect datasetAspect = new DatasetAspect();
    FineGrainedUpstreamArray fineGrainedUpstreamArray = new FineGrainedUpstreamArray();
    for(DatasetSnapshot datasetSnapshot : this.readSnapshots) {
      fineGrainedUpstreamArray.add(assembleFineGrainedUpstream(datasetSnapshot, schemaFieldArray));
    }

    FineGrainedUpstreamLineage fineGrainedUpstreamLineage = new FineGrainedUpstreamLineage();
    fineGrainedUpstreamLineage.setUpstreams(fineGrainedUpstreamArray);
    datasetAspect.setFineGrainedUpstreamLineage(fineGrainedUpstreamLineage);
    return datasetAspect;
  }


  private FineGrainedUpstream assembleFineGrainedUpstream(DatasetSnapshot rDatasetSnapshot, SchemaFieldArray wSchemaFieldArray) {
    FineGrainedUpstream fineGrainedUpstream = new FineGrainedUpstream();
    fineGrainedUpstream.setAuditStamp(getAuditStamp());
    fineGrainedUpstream.setDataset(rDatasetSnapshot.getUrn());
    FineGrainedUpstreamFieldMappingArray fineGrainedUpstreamFieldMappings = new FineGrainedUpstreamFieldMappingArray();
    FineGrainedUpstreamFieldMapping fineGrainedUpstreamFieldMapping = new FineGrainedUpstreamFieldMapping();
    DatasetAspectArray readAspectArray = rDatasetSnapshot.getAspects();

    for(DatasetAspect rDatasetAspect: readAspectArray) {
      if (rDatasetAspect.getSchemaMetadata() != null) {
        Map<String, SchemaField> rSchemaFieldHashMap = schemaFields2Map(rDatasetAspect);
        for(SchemaField wSchemaField: wSchemaFieldArray) {
          if (rSchemaFieldHashMap.get(wSchemaField.getFieldPath()) != null) {
            fineGrainedUpstreamFieldMappings.add(extractFieldMapping(rSchemaFieldHashMap.get(wSchemaField.getFieldPath()), wSchemaField));
          }
        }
      }
    }

    fineGrainedUpstreamFieldMappings.add(fineGrainedUpstreamFieldMapping);
    fineGrainedUpstream.setFields(fineGrainedUpstreamFieldMappings);
    return fineGrainedUpstream;
  }

  private DataProcessAspect dataProcessOwnership() {
    DataProcessAspect ownershipAspect = new DataProcessAspect();
    Ownership ownership = new Ownership();
    OwnerArray owners = new OwnerArray();
    Owner owner = new Owner();
    owner.setOwner(new CorpuserUrn(this.customConfig.getOrDefault("owner", "datahub")));
    owner.setType(OwnershipType.DEVELOPER);
    owners.add(owner);
    ownership.setOwners(owners);
    ownership.setLastModified(getAuditStamp());
    ownershipAspect.setOwnership(ownership);
    return ownershipAspect;
  }

  private DataProcessAspect dataProcessInfoAspect() {
    DataProcessAspect dataProcessAspect = new DataProcessAspect();
    DataProcessInfo dataProcessInfo = new DataProcessInfo();

    DatasetUrnArray inputs = new DatasetUrnArray();
    this.readSnapshots.forEach( datasetSnapshot -> {
      inputs.add(datasetSnapshot.getUrn());
    });
    dataProcessInfo.setInputs(inputs);

    DatasetUrnArray outputs = new DatasetUrnArray();
    outputs.add(this.writeSnapshot.getUrn());
    dataProcessInfo.setOutputs(outputs);

    dataProcessAspect.setDataProcessInfo(dataProcessInfo);
    return dataProcessAspect;
  }

  public void setAuditStamp(String ldapAccount) {
    this.auditStamp = new AuditStamp();
    this.auditStamp.setTime(new Date().getTime());
    this.auditStamp.setActor(new CorpuserUrn(ldapAccount));
  }

  public AuditStamp getAuditStamp() {
    return this.auditStamp;
  }

  private String extractInputSourceToFileName(String inputSource) {
    String[] sourceParts = inputSource.split("/");
    return sourceParts[sourceParts.length-1];
  }

  private java.util.Map<String, SchemaField> schemaFields2Map(DatasetAspect datasetAspect) {
    java.util.Map<String, SchemaField> schemaFieldHashMap = new java.util.HashMap<>();
    if (datasetAspect.getSchemaMetadata() != null) {
      for (SchemaField schemaField : datasetAspect.getSchemaMetadata().getFields()) {
        schemaFieldHashMap.put(schemaField.getFieldPath(), schemaField);
      }
    }
    return schemaFieldHashMap;
  }

  private FineGrainedUpstreamFieldMapping extractFieldMapping(SchemaField rSchemaField, SchemaField wSchemaField) {
    FineGrainedUpstreamFieldMapping fineGrainedUpstreamFieldMapping = new FineGrainedUpstreamFieldMapping();

    if (rSchemaField.getFieldPath().equals(wSchemaField.getFieldPath())) {
      fineGrainedUpstreamFieldMapping.setDestinationField(rSchemaField);
      fineGrainedUpstreamFieldMapping.setSourceField(wSchemaField);
      fineGrainedUpstreamFieldMapping.setType(DatasetLineageType.TRANSFORMED);
    }
    return fineGrainedUpstreamFieldMapping;
  }

  private java.util.Map<String, String> scalaMapList2JavaMap(List<scala.collection.immutable.Map<String, String>> config) {
    HashMap<String, String> res = new HashMap<>();
    if (config == null) return res;
    for(scala.collection.immutable.Map<String, String> object : config) {
      java.util.Map<String, String> o = JavaConverters.mapAsJavaMap(object);
      for(java.util.Map.Entry<String, String> entry: o.entrySet()) {
        res.put(entry.getKey(), entry.getValue());
      }
    }
    return res;
  }

  private List<java.util.Map<String, String>> scalaMapList2JavaMapList(List<scala.collection.immutable.Map<String, String>> dataTypes) {
    List<java.util.Map<String, String>> res = new ArrayList<>();
    if (dataTypes == null) return res;
    for(scala.collection.immutable.Map<String, String> object : dataTypes) {
      res.add(JavaConverters.mapAsJavaMap(object));
    }
    return res;
  }

  private java.util.Map<String, java.util.Map<String, String>> dataTypeIdMapDataField(List<java.util.Map<String, String>> dataTypes) {
    java.util.Map<String, java.util.Map<String, String>> dataTypeHashMap = new java.util.HashMap<>();
    if (dataTypes == null) return dataTypeHashMap;
    for(java.util.Map<String, String> dataType : dataTypes) {
      dataTypeHashMap.put(String.valueOf(dataType.get("id")), dataType);
    }
    return dataTypeHashMap;
  }

  private java.util.Map<String, java.util.Map<String, String>> schemaIdMapAttribute(List<java.util.Map<String, String>> attributes) {
    java.util.Map<String, java.util.Map<String, String>> attributeMap = new java.util.HashMap<>();
    if (attributes == null) return attributeMap;
    for(java.util.Map<String, String> attribute : attributes) {
      attributeMap.put(String.valueOf(attribute.get("id")), attribute);
    }
    return attributeMap;
  }

  private SchemaFieldDataType toSchemaFieldType(String type) {
    SchemaFieldDataType schemaFieldDataType = new SchemaFieldDataType();
    SchemaFieldDataType.Type schemaFieldType = new SchemaFieldDataType.Type();
    switch (type.toLowerCase()) {
      case "byte":
      case "long":
      case "short":
      case "integer":
      case "float":
      case "double":
      case "decimal":
      case "bigdecimal":
        schemaFieldType.setNumberType(new NumberType());
        break;
      case "string":
        schemaFieldType.setStringType(new StringType());
        break;
      case "boolean":
        schemaFieldType.setBooleanType(new BooleanType());
        break;
      case "array":
        schemaFieldType.setArrayType(new ArrayType());
        break;
      case "bytes":
        schemaFieldType.setBytesType(new BytesType());
        break;
      case "map":
        schemaFieldType.setMapType(new MapType());
        break;
      case "timestamp":
        schemaFieldType.setRecordType(new RecordType());
        break;
      default:
        schemaFieldType.setStringType(new StringType());
        break;
    }
    schemaFieldDataType.setType(schemaFieldType);
    return schemaFieldDataType;
  }
}
