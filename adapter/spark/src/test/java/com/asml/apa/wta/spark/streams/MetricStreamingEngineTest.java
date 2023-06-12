package com.asml.apa.wta.spark.streams;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import com.asml.apa.wta.core.dto.JvmFileDto;
import com.asml.apa.wta.core.dto.ProcDto;
import com.asml.apa.wta.core.model.Resource;
import com.asml.apa.wta.spark.dto.ResourceAndStateWrapper;
import com.asml.apa.wta.spark.dto.SparkBaseSupplierWrapperDto;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MetricStreamingEngineTest {

  MetricStreamingEngine sut;

  SparkBaseSupplierWrapperDto s1;
  SparkBaseSupplierWrapperDto s2;
  SparkBaseSupplierWrapperDto s3;
  SparkBaseSupplierWrapperDto s4;
  SparkBaseSupplierWrapperDto s5;

  @BeforeEach
  void setup() {
    sut = new MetricStreamingEngine();

    s1 = SparkBaseSupplierWrapperDto.builder()
        .timestamp(23423432L)
        .executorId("executor1")
        .dstatDto(Optional.empty())
        .iostatDto(Optional.empty())
        .jvmFileDto(Optional.of(JvmFileDto.builder()
            .freeSpace(50000000000000L)
            .totalSpace(5000000000000342L)
            .totalSpace(56732423432423432L)
            .build()))
        .procDto(Optional.of(ProcDto.builder().build()))
        .build();
  }

  @Test
  void gettingAllMetricsWorksAsintended() {
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
}
