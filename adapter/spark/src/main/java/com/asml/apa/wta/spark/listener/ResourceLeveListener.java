package com.asml.apa.wta.spark.listener;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.model.Resource;
import com.asml.apa.wta.spark.dto.SparkBaseSupplierWrapperDto;
import com.asml.apa.wta.spark.streams.MetricStreamingEngine;
import java.util.List;
import java.util.function.Function;

import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerExecutorRemoved;

public class ResourceLeveListener extends AbstractListener<Resource> {

  private final MetricStreamingEngine metricStreamingEngine;

  public ResourceLeveListener(
      SparkContext sparkContext, RuntimeConfig config, MetricStreamingEngine metricStreamingEngine) {
    super(sparkContext, config);
    this.metricStreamingEngine = metricStreamingEngine;
  }

  @Override
  public void onExecutorRemoved(SparkListenerExecutorRemoved executorRemoved) {
    final String id = executorRemoved.executorId();
    final String type = "cluster node";
    List<SparkBaseSupplierWrapperDto> infoList =
        metricStreamingEngine.getResourceStream().onKey(id).toList();
    final String os = getConstantInfo(infoList, snapshot -> snapshot.getOsInfoDto().getOs(), "Unknown");
    final String procModel = getConstantInfo(infoList, snapshot -> snapshot.getOsInfoDto().getArchitecture(), "Unknown");
    final long memory = getConstantInfo(infoList, snapshot -> snapshot.getOsInfoDto().getTotalPhysicalMemorySize(), -1L);


  }


  private static <R> R getConstantInfo(List<SparkBaseSupplierWrapperDto> infoList, Function<SparkBaseSupplierWrapperDto,R> mapper, R defaultValue) {
    return infoList.stream().map(mapper).distinct().findFirst().orElse(defaultValue);
  }
}
