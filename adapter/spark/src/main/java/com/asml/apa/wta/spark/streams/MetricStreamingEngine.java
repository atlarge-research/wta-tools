package com.asml.apa.wta.spark.streams;

import com.asml.apa.wta.core.dto.BaseSupplierDto;
import com.asml.apa.wta.core.dto.IostatDto;
import com.asml.apa.wta.core.dto.JvmFileDto;
import com.asml.apa.wta.core.dto.OsInfoDto;
import com.asml.apa.wta.core.dto.ProcDto;
import com.asml.apa.wta.core.model.Resource;
import com.asml.apa.wta.core.model.ResourceState;
import com.asml.apa.wta.core.streams.KeyedStream;
import com.asml.apa.wta.core.streams.Stream;
import com.asml.apa.wta.spark.dto.ResourceAndStateWrapper;
import com.asml.apa.wta.spark.dto.SparkBaseSupplierWrapperDto;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import lombok.Getter;

/**
 * Facade that maintains the resource and task streams.
 *
 * @author Atour Mousavi Gourabi
 * @author Henry Page
 * @since 1.0.0
 */
@Getter
public class MetricStreamingEngine {

  private final KeyedStream<String, SparkBaseSupplierWrapperDto> executorResourceStream;

  private final KeyedStream<TaskKey, TaskMetricsRecord> taskStream;

  private static final long bytesToGbDenom = 1073741824;

  private static final long kBpsToGbpsDenom = 125000;

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
  public List<ResourceAndStateWrapper> collectResourceInformation() {
    return executorResourceStream.mapKeyList((key, value) -> {
      long transformedId = Math.abs(key.hashCode());
      Resource resource = produceResourceFromExecutorInfo(transformedId, value);
      Stream<ResourceState> states = produceResourceStatesFromExecutorInfo(resource, value);
      return new ResourceAndStateWrapper(resource, states);
    });
  }

  /**
   * Constructs a resource from a stream of pings.
   *
   * @param executorId The transformed id of the executor
   * @param pings The stream of pings that correspond to this executor
   * @return A Resource object that is constructed from the given information
   * @author Henry Page
   * @since 1.0.0
   */
  private Resource produceResourceFromExecutorInfo(long executorId, Stream<SparkBaseSupplierWrapperDto> pings) {

    Optional<OsInfoDto> sampleOsInfo = getFirstAvailable(pings.copy(), BaseSupplierDto::getOsInfoDto);
    Optional<JvmFileDto> sampleJvmInfo = getFirstAvailable(pings.copy(), BaseSupplierDto::getJvmFileDto);
    // do not sample proc info, later pings might actually have useful information

    final String type = "cluster node";
    final String os = sampleOsInfo.map(OsInfoDto::getOs).orElse("unknown");

    StringBuilder processorInformation = new StringBuilder();

    final String processorModel = pings.copy()
        .map(BaseSupplierDto::getProcDto)
        .filter(Objects::nonNull)
        .map(ProcDto::getCpuModel)
        .filter(pModel -> pModel != null && !pModel.equals("unknown"))
        .findFirst()
        .orElse("unknown");

    processorInformation.append(processorModel);
    if (sampleOsInfo.map(OsInfoDto::getArchitecture).isPresent()) {
      processorInformation
          .append(" / ")
          .append(sampleOsInfo.map(OsInfoDto::getArchitecture).get());
    }

    final double numResources =
        sampleOsInfo.map(OsInfoDto::getAvailableProcessors).orElse(-1);
    final long memory = sampleOsInfo
        .map(pg -> pg.getTotalPhysicalMemorySize() / bytesToGbDenom)
        .orElse(-1L);

    final long diskSpace = sampleJvmInfo
        .map(jvmDto -> jvmDto.getTotalSpace() / bytesToGbDenom)
        .orElse(-1L);

    return Resource.builder()
        .id(executorId)
        .type(type)
        .numResources(numResources)
        .memory(memory)
        .diskSpace(diskSpace)
        .procModel(processorInformation.toString())
        .os(os)
        .network(-1L)
        .build();
  }

  /**
   * Constructs a list of resource states from a stream of pings.
   *
   * @param associatedResource The associated resource object
   * @param pings The stream of pings that are to be transformed to states
   * @return A list of resource states that is constructed from the given information
   * @author Henry Page
   * @since 1.0.0
   */
  private Stream<ResourceState> produceResourceStatesFromExecutorInfo(
      Resource associatedResource, Stream<SparkBaseSupplierWrapperDto> pings) {
    return pings.map(ping -> {
      final long timestamp = ping.getTimestamp();
      final String eventType = "resource active";
      final long platformId = -1L;
      final double availableResources = Optional.ofNullable(ping.getOsInfoDto())
          .map(pg -> (double) pg.getAvailableProcessors())
          .orElse(-1.0);
      final double availableMemory = Optional.ofNullable(ping.getOsInfoDto())
          .map(pg -> (double) pg.getFreePhysicalMemorySize() / bytesToGbDenom)
          .orElse(-1.0);
      final double availableDiskSpace = Optional.ofNullable(ping.getJvmFileDto())
          .map(pg -> (double) pg.getUsableSpace() / bytesToGbDenom)
          .orElse(-1.0);

      double availableDiskIoBandwith = -1.0;

      if (ping.getIostatDto() != null) {
        final IostatDto iostatDto = ping.getIostatDto();
        availableDiskIoBandwith = iostatDto.getKiloByteReadPerSec() / kBpsToGbpsDenom
            + iostatDto.getKiloByteWrtnPerSec() / kBpsToGbpsDenom;
      }

      final double availableNetworkBandwidth = -1.0;

      final double numCores = Optional.ofNullable(ping.getOsInfoDto())
          .map(OsInfoDto::getAvailableProcessors)
          .orElse(-1);

      final double averageUtilization1Minute = Optional.ofNullable(ping.getProcDto())
          .map(ProcDto::getLoadAvgOneMinute)
          .filter(loadAvg -> loadAvg != -1)
          .map(loadAvg -> loadAvg / numCores)
          .orElse(-1.0);

      final double averageUtilization5Minute = Optional.ofNullable(ping.getProcDto())
          .map(ProcDto::getLoadAvgFiveMinutes)
          .filter(loadAvg -> loadAvg != -1)
          .map(loadAvg -> loadAvg / numCores)
          .orElse(-1.0);

      final double averageUtilization15Minute = Optional.ofNullable(ping.getProcDto())
          .map(ProcDto::getLoadAvgFifteenMinutes)
          .filter(loadAvg -> loadAvg != -1)
          .map(loadAvg -> loadAvg / numCores)
          .orElse(-1.0);

      return ResourceState.builder()
          .resourceId(associatedResource)
          .timestamp(timestamp)
          .eventType(eventType)
          .platformId(platformId)
          .availableResources(availableResources)
          .availableDiskSpace(availableDiskSpace)
          .availableMemory(availableMemory)
          .availableDiskIoBandwidth(availableDiskIoBandwith)
          .availableNetworkBandwidth(availableNetworkBandwidth)
          .averageUtilization1Minute(averageUtilization1Minute)
          .averageUtilization5Minute(averageUtilization5Minute)
          .averageUtilization15Minute(averageUtilization15Minute)
          .build();
    });
  }

  /**
   * Used for getting information across all pings that is constant.
   *
   * @param pings A list of pings to analyse
   * @param mapper The mapping function that should map the dto to an optional data supplier object
   * @param <R> The type of the data supplier object
   * @return The constant information that is requested
   */
  private <R extends Serializable> Optional<R> getFirstAvailable(
      Stream<SparkBaseSupplierWrapperDto> pings, Function<SparkBaseSupplierWrapperDto, R> mapper) {
    return pings.map(mapper).filter(Objects::nonNull).findFirst();
  }
}
