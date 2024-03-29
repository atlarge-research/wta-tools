package com.asml.apa.wta.spark.executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.*;

import com.asml.apa.wta.core.dto.BaseSupplierDto;
import com.asml.apa.wta.core.dto.DstatDto;
import com.asml.apa.wta.core.dto.IostatDto;
import com.asml.apa.wta.core.dto.JvmFileDto;
import com.asml.apa.wta.core.dto.OsInfoDto;
import com.asml.apa.wta.core.dto.PerfDto;
import com.asml.apa.wta.core.dto.ProcDto;
import com.asml.apa.wta.spark.dto.SparkBaseSupplierWrapperDto;
import org.apache.spark.api.plugin.PluginContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SparkSupplierExtractionEngineTest {

  private PluginContext mockPluginContext;

  private SparkSupplierExtractionEngine sutSupplierExtractionEngine;

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
    PerfDto fakePerfDto = PerfDto.builder().watt(30.12).build();
    ProcDto fakeProcDto = ProcDto.builder().active(1L).build();
    JvmFileDto fakeJvmFileDto = JvmFileDto.builder().freeSpace(11L).build();

    long fakeTime = System.currentTimeMillis();

    BaseSupplierDto baseSupplierDto = new BaseSupplierDto(
        fakeTime, fakeOsInfo, fakeIoStatDto, fakeDstatDto, fakePerfDto, fakeJvmFileDto, fakeProcDto);

    SparkBaseSupplierWrapperDto result = sutSupplierExtractionEngine.transform(baseSupplierDto);

    assertDoesNotThrow(() -> sutSupplierExtractionEngine.pingAndBuffer().join());

    assertThat(result)
        .isEqualTo(SparkBaseSupplierWrapperDto.builder()
            .timestamp(fakeTime)
            .osInfoDto(fakeOsInfo)
            .iostatDto(fakeIoStatDto)
            .dstatDto(fakeDstatDto)
            .perfDto(fakePerfDto)
            .jvmFileDto(fakeJvmFileDto)
            .procDto(fakeProcDto)
            .executorId("test-executor-id")
            .build());
  }
}
