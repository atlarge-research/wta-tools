package com.asml.apa.wta.spark.streams;

import com.asml.apa.wta.core.dto.BaseSupplierDto;
import com.asml.apa.wta.core.dto.JvmFileDto;
import com.asml.apa.wta.core.dto.OsInfoDto;
import com.asml.apa.wta.core.model.Resource;
import com.asml.apa.wta.core.model.ResourceState;
import com.asml.apa.wta.core.streams.KeyedStream;
import com.asml.apa.wta.spark.dto.ResourceAndStateWrapper;
import com.asml.apa.wta.spark.dto.SparkBaseSupplierWrapperDto;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Getter;

/**
 * Facade that maintains the resource and task streams.
 *
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
@Getter
public class MetricStreamingEngine {

  private final KeyedStream<String, SparkBaseSupplierWrapperDto> executorResourceStream;

  private final KeyedStream<TaskKey, TaskMetricsRecord> taskStream;

  private static final long bytesToGbDenom = 1073741824;

  /**
   * Initializes the streams.
   *
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public MetricStreamingEngine() {
    executorResourceStream = new KeyedStream<>();
    taskStream = new KeyedStream<>();
  }

  /**
   * Adds resource metrics to the resource stream.
   *
   * @param resourceKey A {@link String} identifying the resource. This is usually the executorID.
   * @param record the {@link com.asml.apa.wta.spark.datasource.SparkDataSource} containing metrics.
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public void addToResourceStream(String resourceKey, SparkBaseSupplierWrapperDto record) {
    executorResourceStream.addToStream(resourceKey, record);
  }

  /**
   * Adds task metrics to the task stream.
   *
   * @param task the {@link com.asml.apa.wta.spark.streams.TaskKey} of the task
   * @param record the {@link TaskMetricsRecord} containing the metrics
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public void addToTaskStream(TaskKey task, TaskMetricsRecord record) {
    taskStream.addToStream(task, record);
  }

  /**
   * Consumes all resource related streams and aggregates them into objects.
   *
   * @return a list of {@link ResourceAndStateWrapper} objects containing the resource and its states
   * @author Henry Page
   * @since 1.0.0
   */
  public List<ResourceAndStateWrapper> processResources() {
    List<ResourceAndStateWrapper> result = new ArrayList<>();
    Map<String, List<SparkBaseSupplierWrapperDto>> allPings = executorResourceStream.collectAll();

    return allPings.entrySet().stream()
        .map(entry -> {
          long transformedId = Math.abs(entry.getKey().hashCode());
          Resource resource = produceResourceFromExecutorInfo(transformedId, entry.getValue());
          List<ResourceState> states = produceResourceStatesFromExecutorInfo(resource, entry.getValue());
          return new ResourceAndStateWrapper(transformedId, resource, states);
        })
        .collect(Collectors.toList());
  }

  private List<ResourceState> produceResourceStatesFromExecutorInfo(
      Resource associatedResource, List<SparkBaseSupplierWrapperDto> pings) {
    return pings.stream()
        .map(ping -> {
          final long timestamp = ping.getTimestamp();
          final String eventType = "resource active";
          final long platformId = -1L;
          final double availableResources = ping.getOsInfoDto()
              .map(OsInfoDto::getAvailableProcessors)
              .orElse(-1);
          final double availableMemory = ping.getOsInfoDto()
              .map(x -> (x.getFreePhysicalMemorySize() / (bytesToGbDenom)))
              .orElse(-1L);

          // TODO(#144): Fill lohit's code here

          return ResourceState.builder()
              .resourceId(associatedResource)
              .timestamp(timestamp)
              .eventType(eventType)
              .platformId(platformId)
              .availableResources(availableResources)
              .availableMemory(availableMemory)
              .build();
        })
        .collect(Collectors.toList());
  }

  private Resource produceResourceFromExecutorInfo(long executorId, List<SparkBaseSupplierWrapperDto> pings) {

    Optional<OsInfoDto> sampleOsInfo = getFirstAvailable(pings, BaseSupplierDto::getOsInfoDto);
    Optional<JvmFileDto> sampleJvmInfo = getFirstAvailable(pings, BaseSupplierDto::getJvmFileDto);

    final String type = "cluster node";
    final String os = sampleOsInfo.map(OsInfoDto::getOs).orElse("unknown");
    final double numResources =
        sampleOsInfo.map(OsInfoDto::getAvailableProcessors).orElse(-1);

    final long memory = sampleOsInfo
        .map(x -> (x.getTotalPhysicalMemorySize() / (bytesToGbDenom)))
        .orElse(-1L);

    final long diskSpace = sampleJvmInfo
        .map(jvmDto -> (jvmDto.getTotalSpace() / bytesToGbDenom))
        .orElse(-1L);

    // TODO(#144): Fill lohit's code here

    return Resource.builder()
        .id(executorId)
        .type(type)
        .numResources(numResources)
        .memory(memory)
        .diskSpace(diskSpace)
        .procModel("TO BE FILLED")
        .os(os)
        .network(-1L)
        .build();
  }

  /**
   * Used for getting information across all pings that is constant.
   *
   * @param pings A list of pings to analyse
   * @param mapper The mapping function that should map the dto to an optional data supplier object
   * @param <R> The type of the data supplier object
   * @return The constant information that is requested
   */
  private <R> Optional<R> getFirstAvailable(
      List<SparkBaseSupplierWrapperDto> pings, Function<SparkBaseSupplierWrapperDto, Optional<R>> mapper) {
    return pings.stream().map(mapper).flatMap(Optional::stream).findFirst();
  }
}
