package com.asml.apa.wta.spark.executor.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.asml.apa.wta.core.dto.BaseSupplierDto;
import com.asml.apa.wta.core.dto.IostatDto;
import com.asml.apa.wta.core.dto.OsInfoDto;
import com.asml.apa.wta.spark.dto.SparkBaseSupplierWrapperDto;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.spark.api.plugin.PluginContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class SparkSupplierExtractionEngineTest {

  PluginContext mockPluginContext;
  SparkSupplierExtractionEngine sut;

  @BeforeEach
  void setup() {
    mockPluginContext = mock(PluginContext.class);
    when(mockPluginContext.executorID()).thenReturn("test-executor-id");

    sut = spy(new SparkSupplierExtractionEngine(mockPluginContext));
  }

  @AfterEach
  void killScheduler() {
    sut.stopPinging();
  }

  @Test
  void correctDtoGetsReturnedWhenBaseInformationIsTransformed() {
    OsInfoDto fakeOsInfo = OsInfoDto.builder().availableProcessors(1).build();
    IostatDto fakeIoStatDto = IostatDto.builder().kiloByteRead(40).build();

    LocalDateTime fakeTime = LocalDateTime.of(2000, 1, 1, 0, 0);

    BaseSupplierDto baseSupplierDto = new BaseSupplierDto(fakeTime, fakeOsInfo, fakeIoStatDto);

    SparkBaseSupplierWrapperDto result = sut.transform(baseSupplierDto);

    assertThat(result)
        .isEqualTo(SparkBaseSupplierWrapperDto.builder()
            .timestamp(fakeTime)
            .osInfoDto(fakeOsInfo)
            .iostatDto(fakeIoStatDto)
            .executorId("test-executor-id")
            .build());
  }

  @Test
  void startAndStopPingingWorksAsIntended() {
    sut.startPinging(1000);

    verify(sut, timeout(10000L).atLeast(3)).ping();

    assertThat(sut.getBuffer()).hasSize(3);
  }

  @Test
  @Timeout(value = 1000L, unit = TimeUnit.MILLISECONDS)
  void pingWorksAsIntended() {
    CompletableFuture<Void> result = sut.ping();

    result.join();

    List<SparkBaseSupplierWrapperDto> buffer = sut.getAndClear();
    assertThat(buffer).hasSize(1);
    assertThat(sut.getBuffer()).hasSize(0);

    SparkBaseSupplierWrapperDto testObj = buffer.get(0);

    assertThat(testObj.getExecutorId()).isEqualTo("test-executor-id");
    assertThat(testObj.getOsInfoDto().getAvailableProcessors()).isGreaterThanOrEqualTo(1);
  }
}
