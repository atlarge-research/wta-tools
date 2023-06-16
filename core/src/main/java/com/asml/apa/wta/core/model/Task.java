package com.asml.apa.wta.core.model;

import com.asml.apa.wta.core.io.ParquetSchema;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;

/**
 * Task class corresponding to WTA format.
 *
 * @author  Lohithsai Yadala Chanchu
 * @since 1.0.0
 */
@Data
@Builder
@Slf4j
@SuppressWarnings("VisibilityModifier")
public class Task implements BaseTraceObject {

  private static final long serialVersionUID = -1372345471722101373L;

  public final long id;

  public final String type;

  public final long tsSubmit;

  public final int submissionSite;

  public final long runtime;

  public final String resourceType;

  public final double resourceAmountRequested;

  public long[] parents;

  public long[] children;

  public final int userId;

  public final int groupId;

  public final String nfrs;

  public final long workflowId;

  public final long waitTime;

  public final String params;

  public final double memoryRequested;

  public final long diskIoTime;

  public final double diskSpaceRequested;

  public final double energyConsumption;

  public final long networkIoTime;

  public final long resourceUsed;

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
    log.trace("Converting Task with id {} to record", this.id);
    return schema.convertFromPojo(this, Task.class);
  }
}
