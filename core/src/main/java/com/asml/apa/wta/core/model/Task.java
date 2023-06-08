package com.asml.apa.wta.core.model;

import com.asml.apa.wta.core.io.ParquetSchema;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * Task class corresponding to WTA format.
 *
 * @author  Lohithsai Yadala Chanchu
 * @since 1.0.0
 */
@Data
@Builder
public class Task implements BaseTraceObject {

  private static final long serialVersionUID = -1372345471722101373L;

  @Getter(value = AccessLevel.NONE)
  private final String schemaVersion = this.getSchemaVersion();

  private final long id;

  private final String type;

  private final long submitTime;

  private final int submissionSite;

  private final long runtime;

  private final String resourceType;

  private final double resourceAmountRequested;

  @Setter
  private long[] parents;

  @Setter
  private long[] children;

  private final int userId;

  private final int groupId;

  private final String nfrs;

  private final long workflowId;

  private final long waitTime;

  private final String params;

  private final double memoryRequested;

  private final long networkIoTime;

  private final long diskIoTime;

  private final double diskSpaceRequested;

  private final double energyConsumption;

  private final long resourceUsed;

  /**
   * Converts the POJO object into record object, enabling it to be written by Avro.
   * It will put all fields allowed by the checker into the record.
   *
   * @param schema schema
   * @return record
   * @since 1.0.0
   * @author Atour Mousavi Gourabi
   */
  @Override
  public GenericRecord convertToRecord(ParquetSchema schema) {
    return schema.convertFromPojo(this, Task.class);
  }
}
