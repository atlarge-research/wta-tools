package com.asml.apa.wta.spark.executor.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import com.asml.apa.wta.core.dto.BaseSupplierDto;
import com.asml.apa.wta.core.dto.IostatDto;
import com.asml.apa.wta.core.dto.OsInfoDto;
import com.asml.apa.wta.spark.dto.SparkBaseSupplierWrapperDto;
import java.time.LocalDateTime;
import org.apache.spark.api.plugin.PluginContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SparkSupplierExtractionEngineTest {

  PluginContext mockPluginContext;
  SparkSupplierExtractionEngine sutSupplierExtractionEngine;

  @BeforeEach
  void setup() {
    mockPluginContext = mock(PluginContext.class);
    when(mockPluginContext.executorID()).thenReturn("test-executor-id");

    sutSupplierExtractionEngine = spy(new SparkSupplierExtractionEngine(mockPluginContext));
  }

  @AfterEach
  void killScheduler() {
    sutSupplierExtractionEngine.stopPinging();
  }

  @Test
  void correctDtoGetsReturnedWhenBaseInformationIsTransformed() {
    OsInfoDto fakeOsInfo = OsInfoDto.builder().availableProcessors(1).build();
    IostatDto fakeIoStatDto = IostatDto.builder().kiloByteRead(40).build();

    LocalDateTime fakeTime = LocalDateTime.of(2000, 1, 1, 0, 0);

    BaseSupplierDto baseSupplierDto = new BaseSupplierDto(fakeTime, fakeOsInfo, fakeIoStatDto);

    SparkBaseSupplierWrapperDto result = sutSupplierExtractionEngine.transform(baseSupplierDto);

    assertThat(result)
        .isEqualTo(SparkBaseSupplierWrapperDto.builder()
            .timestamp(fakeTime)
            .osInfoDto(fakeOsInfo)
            .iostatDto(fakeIoStatDto)
            .executorId("test-executor-id")
            .build());
  }
}
