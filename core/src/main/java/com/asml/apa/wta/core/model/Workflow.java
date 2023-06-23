package com.asml.apa.wta.core.model;

import com.asml.apa.wta.core.io.ParquetSchema;
import com.asml.apa.wta.core.model.enums.Domain;
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

  private final long tsSubmit;

  private final Task[] tasks;

  private final int taskCount;

  private long criticalPathLength;

  private int criticalPathTaskCount;

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
    log.trace("Converting Workflow with id {} to record", this.id);
    return schema.convertFromPojo(this, Workflow.class);
  }
}
