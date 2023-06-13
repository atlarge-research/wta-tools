package com.asml.apa.wta.spark.executor.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.*;

import com.asml.apa.wta.core.dto.BaseSupplierDto;
import com.asml.apa.wta.core.dto.DstatDto;
import com.asml.apa.wta.core.dto.IostatDto;
import com.asml.apa.wta.core.dto.OsInfoDto;
import com.asml.apa.wta.core.dto.PerfDto;
import com.asml.apa.wta.core.dto.ProcDto;
import com.asml.apa.wta.spark.dto.SparkBaseSupplierWrapperDto;
import java.time.LocalDateTime;
import java.util.Optional;
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

    sutSupplierExtractionEngine = spy(new SparkSupplierExtractionEngine(1000, mockPluginContext, 2000));
  }

  @AfterEach
  void killScheduler() {
    sutSupplierExtractionEngine.stopPinging();
  }

  @Test
  void correctDtoGetsReturnedWhenBaseInformationIsTransformed() {
    OsInfoDto fakeOsInfo = OsInfoDto.builder().availableProcessors(1).build();
    IostatDto fakeIoStatDto = IostatDto.builder().kiloByteRead(40).build();
    DstatDto fakeDstatDto = DstatDto.builder().netSend(1).build();
    ProcDto fakeProcDto = ProcDto.builder().active(Optional.of(1L)).build();
    PerfDto fakePerfDto = PerfDto.builder().watt(30.12).build();

    LocalDateTime fakeTime = LocalDateTime.of(2000, 1, 1, 0, 0);

    BaseSupplierDto baseSupplierDto =
        new BaseSupplierDto(fakeTime, fakeOsInfo, fakeIoStatDto, fakeDstatDto, fakeProcDto, fakePerfDto);

    SparkBaseSupplierWrapperDto result = sutSupplierExtractionEngine.transform(baseSupplierDto);

    assertDoesNotThrow(() -> sutSupplierExtractionEngine.pingAndBuffer().join());

    assertThat(result)
        .isEqualTo(SparkBaseSupplierWrapperDto.builder()
            .timestamp(fakeTime)
            .osInfoDto(fakeOsInfo)
            .iostatDto(fakeIoStatDto)
            .dstatDto(fakeDstatDto)
            .perfDto(fakePerfDto)
            .procDto(fakeProcDto)
            .executorId("test-executor-id")
            .build());
  }
}
