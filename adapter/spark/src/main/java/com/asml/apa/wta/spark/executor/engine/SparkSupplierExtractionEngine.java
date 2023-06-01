package com.asml.apa.wta.spark.executor.engine;

import com.asml.apa.wta.core.dto.BaseSupplierDto;
import com.asml.apa.wta.core.supplier.SupplierExtractionEngine;
import com.asml.apa.wta.spark.dto.SparkBaseSupplierWrapperDto;
import org.apache.spark.api.plugin.PluginContext;

/**
 * Extracts resource utilization information whilst augmenting it with Spark information.
 *
 * @author Henry Page
 * @since 1.0.0
 */
public class SparkSupplierExtractionEngine extends SupplierExtractionEngine<SparkBaseSupplierWrapperDto> {

  private final PluginContext pluginContext;

  /**
   * Specialised extraction engine for Spark.
   *
   * @param pluginContext The plugin context.
   * @author Henry Page
   * @since 1.0.0
   */
  public SparkSupplierExtractionEngine(PluginContext pluginContext) {
    this.pluginContext = pluginContext;
  }

  /**
   * Augments the base supplier Dto with Spark information.
   *
   * @param record The {@link BaseSupplierDto} to transform
   * @return A {@link SparkBaseSupplierWrapperDto} containing information pertaining to Spark
   * @author Henry Page
   * @since 1.0.0
   */
  @Override
  public SparkBaseSupplierWrapperDto transform(BaseSupplierDto record) {
    return SparkBaseSupplierWrapperDto.builder()
        .executorId(pluginContext.executorID())
        .timestamp(record.getTimestamp())
        .osInfoDto(record.getOsInfoDto())
        .iostatDto(record.getIostatDto())
        .build();
  }
}
