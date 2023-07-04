package com.asml.apa.wta.core.model;

import com.asml.apa.wta.core.io.ParquetSchema;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;

/**
 * Workflow class corresponding to WTA format.
 *
 * @author Lohithsai Yadala Chanchu
 * @since 1.0.0
 */
@Data
@Builder
@Slf4j
public class Workflow implements BaseTraceObject {

  private static final long serialVersionUID = 9065743819019553490L;

  private final long id;

  private final Domain domain;

  @Builder.Default
  private final long tsSubmit = -1L;

  @Builder.Default
  private final Long[] taskIds = new Long[0];

  @Builder.Default
  private final long taskCount = -1L;

  @Builder.Default
  private long criticalPathLength = -1L;

  @Builder.Default
  private final long criticalPathTaskCount = -1L;

  @Builder.Default
  private final int maxConcurrentTasks = -1;

  @Builder.Default
  private final String nfrs = "";

  @Builder.Default
  private final String scheduler = "";

  @Builder.Default
  private final String applicationName = "";

  @Builder.Default
  private final String applicationField = "ETL";

  @Builder.Default
  private double totalResources = -1.0;

  @Builder.Default
  private final double totalMemoryUsage = -1.0;

  @Builder.Default
  private final long totalNetworkUsage = -1L;

  @Builder.Default
  private final double totalDiskSpaceUsage = -1.0;

  @Builder.Default
  private final double totalEnergyConsumption = -1.0;

  /**
   * Converts the POJO object into record object, enabling it to be written by Avro.
   * It will put all fields allowed by the checker into the record.
   *
   * @param schema The parquet schema
   * @return record The record representing a row in Parquet
   */
  @Override
  public GenericRecord convertToRecord(ParquetSchema schema) {
    log.trace("Converting Workflow with id {} to record", this.id);
    return schema.convertFromPojo(this, Workflow.class);
  }
}
