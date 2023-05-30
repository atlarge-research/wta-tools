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
   */
  public SparkSupplierExtractionEngine(PluginContext pluginContext) {
    super();
    this.pluginContext = pluginContext;
  }

  /**
   *
   * @param record The resource metrics record to transform
   * @return A
   */
  @Override
  public SparkBaseSupplierWrapperDto transform(BaseSupplierDto record) {
    return SparkBaseSupplierWrapperDto.builder()
        .executorId(pluginContext.executorID())
        .osInfoDto(record.getOsInfoDto())
        .iostatDto(record.getIostatDto())
        .build();
  }
}
