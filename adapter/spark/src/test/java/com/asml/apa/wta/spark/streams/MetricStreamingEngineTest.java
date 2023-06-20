package com.asml.apa.wta.spark.streams;

import static org.assertj.core.api.Assertions.assertThat;

import com.asml.apa.wta.core.dto.IostatDto;
import com.asml.apa.wta.core.dto.JvmFileDto;
import com.asml.apa.wta.core.dto.OsInfoDto;
import com.asml.apa.wta.core.dto.ProcDto;
import com.asml.apa.wta.core.model.Resource;
import com.asml.apa.wta.core.model.ResourceState;
import com.asml.apa.wta.spark.dto.ResourceAndStateWrapper;
import com.asml.apa.wta.spark.dto.SparkBaseSupplierWrapperDto;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MetricStreamingEngineTest {

  MetricStreamingEngine sut;

  SparkBaseSupplierWrapperDto s1;
  SparkBaseSupplierWrapperDto s2;
  SparkBaseSupplierWrapperDto s3;

  @BeforeEach
  void setup() {
    sut = new MetricStreamingEngine();

    s1 = SparkBaseSupplierWrapperDto.builder()
        .timestamp(234232L)
        .executorId("executor1")
        .dstatDto(null)
        .iostatDto(IostatDto.builder()
            .kiloByteWrtnPerSec(39321321321321L)
            .kiloByteReadPerSec(432423432432L)
            .build())
        .jvmFileDto(JvmFileDto.builder()
            .freeSpace(50000000000000L)
            .totalSpace(5000000000000342L)
            .totalSpace(56732423432423432L)
            .build())
        .procDto(ProcDto.builder().build())
        .build();

    s2 = SparkBaseSupplierWrapperDto.builder()
        .timestamp(23423439L)
        .executorId("executor1")
        .jvmFileDto(JvmFileDto.builder()
            .freeSpace(50000000000000L)
            .totalSpace(5000000000000342L)
            .totalSpace(56732423432423432L)
            .build())
        .osInfoDto(OsInfoDto.builder()
            .os("Mac OS X")
            .architecture("x64")
            .availableProcessors(8)
            .committedVirtualMemorySize(36L)
            .freePhysicalMemorySize(12000000000L)
            .processCpuLoad(50L)
            .totalPhysicalMemorySize(120000000000L)
            .build())
        .procDto(ProcDto.builder()
            .cpuModel("Ryzen 7 Over 90000")
            .loadAvgOneMinute(1.36)
            .loadAvgFiveMinutes(1.76)
            .loadAvgFifteenMinutes(1.96)
            .build())
        .build();

    s3 = SparkBaseSupplierWrapperDto.builder()
        .timestamp(23423439L)
        .executorId("executor2")
        .iostatDto(IostatDto.builder()
            .kiloByteDscd(26L)
            .kiloByteRead(36L)
            .kiloByteWrtnPerSec(3913213L)
            .kiloByteReadPerSec(932423432423L)
            .build())
        .jvmFileDto(JvmFileDto.builder()
            .freeSpace(50000000000000L)
            .totalSpace(5000000000000342L)
            .totalSpace(56732423432423432L)
            .build())
        .osInfoDto(OsInfoDto.builder()
            .os("Mac OS X")
            .architecture("x64")
            .availableProcessors(8)
            .committedVirtualMemorySize(36L)
            .freePhysicalMemorySize(12000000000L)
            .processCpuLoad(50L)
            .totalPhysicalMemorySize(120000000000L)
            .build())
        .procDto(ProcDto.builder().cpuModel("Ryzen 7 Over 90000").build())
        .build();
  }

  @Test
  void gettingMetricsWithEmptyResultsDoesNotCrashPlugin() {
    assertThat(sut.collectResourceInformation()).isEmpty();
    sut.addToResourceStream(s1.getExecutorId(), s1);

    ResourceAndStateWrapper stateWrapperResult =
        sut.collectResourceInformation().get(0);

    assertThat(sut.collectResourceInformation().get(0).getStates()).isEmpty();

    Resource result = stateWrapperResult.getResource();
    assertThat(result.getOs()).isEqualTo("unknown");
    assertThat(result.getId()).isPositive();
    assertThat(result.getMemory()).isEqualTo(-1L);
    assertThat(result.getDiskSpace()).isPositive();
  }

  @Test
  void gettingMetricsWorksAsExpected() {
    sut.addToResourceStream(s1.getExecutorId(), s1);
    sut.addToResourceStream(s2.getExecutorId(), s2);
    sut.addToResourceStream(s3.getExecutorId(), s3);

    List<ResourceAndStateWrapper> result = sut.collectResourceInformation();

    // size assertions
    assertThat(result).hasSize(2);

    ResourceAndStateWrapper executor2 = result.stream()
        .filter(r ->
            r.getResource().getId() == Math.abs(s2.getExecutorId().hashCode()))
        .findFirst()
        .get();
    ResourceAndStateWrapper executor3 = result.stream()
        .filter(r ->
            r.getResource().getId() == Math.abs(s3.getExecutorId().hashCode()))
        .findFirst()
        .get();

    assertThat(executor2.getResource())
        .isEqualTo(Resource.builder()
            .id(Math.abs(s1.getExecutorId().hashCode()))
            .type("cluster node")
            .numResources(8.0)
            .memory(111L)
            .diskSpace(52836186L)
            .network(-1L)
            .procModel("Ryzen 7 Over 90000 / x64")
            .os("Mac OS X")
            .build());

    ResourceState state1 = executor2.getStates().get(0);
    ResourceState state2 = executor2.getStates().get(1);

    assertThat(executor2.getStates()).hasSize(2);
    assertThat(state1.getTimestamp()).isLessThan(state2.getTimestamp());
    assertThat(state1.getResourceId()).isEqualTo(state2.getResourceId());
    assertThat(state1.getAvailableDiskIoBandwidth()).isGreaterThan(0);
    assertThat(state2.getAvailableDiskIoBandwidth()).isEqualTo(-1.0);

    assertThat(state1.getTimestamp()).isEqualTo(s1.getTimestamp());
    assertThat(state1.getAvailableResources()).isEqualTo(-1.0);
    assertThat(state1.getAverageUtilization1Minute()).isEqualTo(-1.0);
    assertThat(state1.getAverageUtilization5Minute()).isEqualTo(-1.0);
    assertThat(state1.getAverageUtilization15Minute()).isEqualTo(-1.0);

    assertThat(state2.getAverageUtilization1Minute()).isGreaterThan(0.0);
    assertThat(state2.getAverageUtilization5Minute()).isGreaterThan(0.0);
    assertThat(state2.getAverageUtilization15Minute()).isGreaterThan(0.0);

    assertThat(executor3.getResource()).isNotEqualTo(executor2.getResource());

    assertThat(executor2.getStates()).hasSize(2);
    assertThat(executor3.getStates()).hasSize(1);
    assertThat(sut.collectResourceInformation().get(0).getStates()).isEmpty();
    assertThat(sut.collectResourceInformation().get(0).getResource()).isNotNull();
  }

  @Test
  void filteringThroughAStreamReturnsTheFirstValueWhereTheOptionalIsPresent() {
    OsInfoDto modifiedDto = s3.getOsInfoDto();
    modifiedDto.setOs("asfasdfjasfsadfasfasdfsa");
    s3.setOsInfoDto(modifiedDto);
    s3.setExecutorId(s1.getExecutorId());

    sut.addToResourceStream(s1.getExecutorId(), s1);
    sut.addToResourceStream(s3.getExecutorId(), s3);

    List<ResourceAndStateWrapper> result = sut.collectResourceInformation();
    assertThat(result).hasSize(1);
    assertThat(result.get(0).getResource().getOs()).isEqualTo("asfasdfjasfsadfasfasdfsa");
  }
}
