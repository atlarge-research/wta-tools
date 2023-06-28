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
public class Task implements BaseTraceObject {

  private static final long serialVersionUID = -1372345471722101373L;

  private final long id;

  @Builder.Default
  private final String type = "";

  @Builder.Default
  private final long tsSubmit = -1L;

  @Builder.Default
  private final long workflowId = -1L;

  @Builder.Default
  private final int submissionSite = -1;

  @Builder.Default
  private final long runtime = -1L;

  @Builder.Default
  private String resourceType = "N/A";

  @Builder.Default
  private double resourceAmountRequested = -1.0;

  @Builder.Default
  private long[] parents = new long[0];

  @Builder.Default
  private long[] children = new long[0];

  @Builder.Default
  private final int userId = -1;

  @Builder.Default
  private final int groupId = -1;

  @Builder.Default
  private final String nfrs = "";

  @Builder.Default
  private final long waitTime = -1L;

  @Builder.Default
  private final String params = "";

  @Builder.Default
  private final double memoryRequested = -1.0;

  @Builder.Default
  private final long diskIoTime = -1L;

  @Builder.Default
  private final double diskSpaceRequested = -1.0;

  @Builder.Default
  private final double energyConsumption = -1L;

  @Builder.Default
  private final long networkIoTime = -1L;

  @Builder.Default
  private final long resourceUsed = -1L;

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
