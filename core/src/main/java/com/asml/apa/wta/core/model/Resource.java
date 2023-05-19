package com.asml.apa.wta.core.model;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * Resource class corresponding to WTA format.
 *
 * @author  Lohithsai Yadala Chanchu
 * @since 1.0.0
 */
@Data
@Builder
public class Resource implements BaseTraceObject {

  private static final long serialVersionUID = 3002249398331752973L;

  @Getter(value = AccessLevel.NONE)
  private final String schemaVersion = this.getSchemaVersion();

  private final long id;

  private final String type;

  private final double numResources;

  private final String procModel;

  private final long memory;

  private final long diskSpace;

  private final long networkSpeed;

  private final String os;

  private final String details;

  private static Schema resourceSchema = SchemaBuilder.record("resource")
      .namespace("com.asml.apa.wta.core.model")
      .fields()
      .name("id")
      .type()
      .longType()
      .noDefault()
      .name("type")
      .type()
      .nullable()
      .stringType()
      .stringDefault("test")
      .name("numResources")
      .type()
      .doubleType()
      .doubleDefault(0.0)
      .name("procModel")
      .type()
      .nullable()
      .stringType()
      .stringDefault("test")
      .name("memory")
      .type()
      .longType()
      .longDefault(0)
      .name("diskSpace")
      .type()
      .longType()
      .longDefault(0)
      .name("networkSpeed")
      .type()
      .longType()
      .longDefault(0)
      .name("os")
      .type()
      .nullable()
      .stringType()
      .stringDefault("test")
      .name("details")
      .type()
      .nullable()
      .stringType()
      .stringDefault("test")
      .endRecord();

  /**
   * convert resource object to record.
   *
   * @param resource resource
   * @return record
   * @since 1.0.0
   * @author Tianchen Qu
   */
  public static GenericRecord convertResourceToRecord(Resource resource) {
    GenericData.Record record = new GenericData.Record(resourceSchema);
    record.put("id", resource.getId());
    record.put("type", resource.getType());
    record.put("numResources", resource.getNumResources());
    record.put("procModel", resource.getProcModel());
    record.put("memory", resource.getMemory());
    record.put("diskSpace", resource.getDiskSpace());
    record.put("networkSpeed", resource.getNetworkSpeed());
    record.put("os", resource.getOs());
    record.put("details", resource.getDetails());
    return record;
  }

  public static Schema getResourceSchema() {
    return resourceSchema;
  }
}
