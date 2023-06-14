package com.asml.apa.wta.core.model;

import com.asml.apa.wta.core.io.ParquetSchema;
import com.asml.apa.wta.core.model.enums.Domain;
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
@SuppressWarnings("VisibilityModifier")
public class Workflow implements BaseTraceObject {

  private static final long serialVersionUID = 9065743819019553490L;

  public final long id;

  public final long tsSubmit;

  public final Task[] tasks;

  public final int taskCount;

  public final long criticalPathLength;

  public final int criticalPathTaskCount;

  public final int maxConcurrentTasks;

  public final String nfrs;

  public final String scheduler;

  private final Domain domain;

  public final String applicationName;

  public final String applicationField;

  public final double totalResources;

  public final double totalMemoryUsage;

  public final long totalNetworkUsage;

  public final double totalDiskSpaceUsage;

  public final double totalEnergyConsumption;

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
