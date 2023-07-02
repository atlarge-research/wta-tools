package com.asml.apa.wta.spark.stream;

import com.asml.apa.wta.core.dto.BaseSupplierDto;
import com.asml.apa.wta.core.dto.IostatDto;
import com.asml.apa.wta.core.dto.JvmFileDto;
import com.asml.apa.wta.core.dto.OsInfoDto;
import com.asml.apa.wta.core.dto.ProcDto;
import com.asml.apa.wta.core.model.Resource;
import com.asml.apa.wta.core.model.ResourceState;
import com.asml.apa.wta.core.stream.KeyedStream;
import com.asml.apa.wta.core.stream.Stream;
import com.asml.apa.wta.spark.datasource.SparkDataSource;
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

  private static final long bytesToGb = 1073741824;

  private static final long kBpsToGbps = 125000;

  /**
   * Initializes the streams.
   */
  public MetricStreamingEngine() {
    executorResourceStream = new KeyedStream<>();
  }

  /**
   * Adds resource metrics to the resource stream.
   *
   * @param resourceKey       {@link String} identifying the resource. This is usually the executorID
   * @param record            {@link SparkDataSource} containing metrics
   */
  public void addToResourceStream(String resourceKey, SparkBaseSupplierWrapperDto record) {
    executorResourceStream.addToStream(resourceKey, record);
  }

  /**
   * Consumes all resource related streams and aggregates them into objects.
   *
   * @return      list of {@link ResourceAndStateWrapper} objects containing the resource and its states
   */
  public List<ResourceAndStateWrapper> collectResourceInformation() {
    return executorResourceStream.mapKeyList((key, value) -> {
      long transformedId = Math.abs(key.hashCode());
      Resource resource = produceResourceFromExecutorInfo(transformedId, value);
      Stream<ResourceState> states = produceResourceStatesFromExecutorInfo(resource.getId(), value);
      return new ResourceAndStateWrapper(resource, states);
    });
  }

  /**
   * Constructs a resource from a stream of pings.
   *
   * @param executorId      transformed id of the executor
   * @param pings           stream of pings that correspond to this executor
   * @return                {@link Resource} object that is constructed from the given information
   */
  private Resource produceResourceFromExecutorInfo(long executorId, Stream<SparkBaseSupplierWrapperDto> pings) {
    Optional<OsInfoDto> sampleOsInfo = getFirstAvailable(pings.copy(), BaseSupplierDto::getOsInfoDto);
    Optional<JvmFileDto> sampleJvmInfo = getFirstAvailable(pings.copy(), BaseSupplierDto::getJvmFileDto);

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
        .map(pg -> pg.getTotalPhysicalMemorySize() / bytesToGb)
        .orElse(-1L);

    final long diskSpace =
        sampleJvmInfo.map(jvmDto -> jvmDto.getTotalSpace() / bytesToGb).orElse(-1L);

    return Resource.builder()
        .id(executorId)
        .numResources(numResources)
        .memory(memory)
        .diskSpace(diskSpace)
        .procModel(processorInformation.toString())
        .os(os)
        .build();
  }

  /**
   * Constructs a list of resource states from a stream of pings.
   *
   * @param resourceId      associated resource id
   * @param pings           stream of pings that are to be transformed to states
   * @return                list of resource states that is constructed from the given information
   */
  private Stream<ResourceState> produceResourceStatesFromExecutorInfo(
      long resourceId, Stream<SparkBaseSupplierWrapperDto> pings) {
    return pings.map(ping -> {
      final long timestamp = ping.getTimestamp();
      final String eventType = "resource active";
      final double availableResources = Optional.ofNullable(ping.getOsInfoDto())
          .map(pg -> (double) pg.getAvailableProcessors())
          .orElse(-1.0);
      final double availableMemory = Optional.ofNullable(ping.getOsInfoDto())
          .map(pg -> (double) pg.getFreePhysicalMemorySize() / bytesToGb)
          .orElse(-1.0);
      final double availableDiskSpace = Optional.ofNullable(ping.getJvmFileDto())
          .map(pg -> (double) pg.getUsableSpace() / bytesToGb)
          .orElse(-1.0);

      double availableDiskIoBandwidth;

      if (ping.getIostatDto() != null) {
        final IostatDto iostatDto = ping.getIostatDto();
        availableDiskIoBandwidth =
            iostatDto.getKiloByteReadPerSec() / kBpsToGbps + iostatDto.getKiloByteWrtnPerSec() / kBpsToGbps;
      } else {
        availableDiskIoBandwidth = -1.0;
      }

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
          .resourceId(resourceId)
          .timestamp(timestamp)
          .eventType(eventType)
          .availableResources(availableResources)
          .availableDiskSpace(availableDiskSpace)
          .availableMemory(availableMemory)
          .availableDiskIoBandwidth(availableDiskIoBandwidth)
          .averageUtilization1Minute(averageUtilization1Minute)
          .averageUtilization5Minute(averageUtilization5Minute)
          .averageUtilization15Minute(averageUtilization15Minute)
          .build();
    });
  }

  /**
   * Used for getting information across all pings that is constant.
   *
   * @param pings       list of pings to analyse
   * @param mapper      mapping function that should map the dto to an optional data supplier object
   * @param <R>         type of the data supplier object
   * @return            constant information that is requested
   */
  private <R extends Serializable> Optional<R> getFirstAvailable(
      Stream<SparkBaseSupplierWrapperDto> pings, Function<SparkBaseSupplierWrapperDto, R> mapper) {
    return pings.map(mapper).filter(Objects::nonNull).findFirst();
  }
}
