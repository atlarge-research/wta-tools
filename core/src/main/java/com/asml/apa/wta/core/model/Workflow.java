package com.asml.apa.wta.core.model;

import com.asml.apa.wta.core.io.ParquetSchema;
import lombok.Builder;
import lombok.Data;
import org.apache.avro.generic.GenericRecord;

/**
 * Workflow class corresponding to WTA format.
 *
 * @author Lohithsai Yadala Chanchu
 * @since 1.0.0
 */
@Data
@Builder
public class Workflow implements BaseTraceObject {

  private static final long serialVersionUID = 9065743819019553490L;

  private final long id;

  private final long tsSubmit;

  private final Long[] taskIds;

  private final long taskCount;

  private final long criticalPathLength;

  private final long criticalPathTaskCount;

  private final int maxConcurrentTasks;

  private final String nfrs;

  private final String scheduler;

  private final Domain domain;

  private final String applicationName;

  private final String applicationField;

  private double totalResources;

  private final double totalMemoryUsage;

  private final long totalNetworkUsage;

  private final double totalDiskSpaceUsage;

  private final double totalEnergyConsumption;

  /**
   * Converts the POJO object into record object, enabling it to be written by Avro.
   * It will put all fields allowed by the checker into the record.
   *
   * @param schema The parquet schema
   * @return record The record representing a row in Parquet
   * @author Tianchen Qu
   * @since 1.0.0
   */
  @Override
  public GenericRecord convertToRecord(ParquetSchema schema) {
    return schema.convertFromPojo(this, Workflow.class);
  }
}
