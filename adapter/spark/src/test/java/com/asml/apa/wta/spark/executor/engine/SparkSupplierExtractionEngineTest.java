package com.asml.apa.wta.spark.executor.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.asml.apa.wta.core.dto.BaseSupplierDto;
import com.asml.apa.wta.core.dto.IostatDto;
import com.asml.apa.wta.core.dto.OsInfoDto;
import com.asml.apa.wta.spark.dto.SparkBaseSupplierWrapperDto;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.spark.api.plugin.PluginContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

class SparkSupplierExtractionEngineTest {

  /**
   * We enforce a max timeout to prevent the test from hanging indefinitely.
   * This needs to be modified as dependencies take longer to resolve.
   */
  private static final long maxTimeout = 30000L;

  PluginContext mockPluginContext;
  SparkSupplierExtractionEngine sut;

  @BeforeEach
  void setup() {
    mockPluginContext = mock(PluginContext.class);
    when(mockPluginContext.executorID()).thenReturn("test-executor-id");

    sut = new SparkSupplierExtractionEngine(mockPluginContext);
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
  @EnabledOnOs(OS.LINUX)
  void startAndStopPingingWorksAsIntended() {
    sut.startPinging(1000);

    await().atMost(4, TimeUnit.SECONDS).until(sut.getBuffer()::size, greaterThanOrEqualTo(3));

    sut.stopPinging();

    int maintainedSize = sut.getBuffer().size();

    await().during(3, TimeUnit.SECONDS)
        .atMost(4, TimeUnit.SECONDS)
        .until(() -> sut.getBuffer().size() == maintainedSize);
  }

  @Test
  @EnabledOnOs(OS.LINUX)
  void pingWorksAsIntended() {
    sut.ping();

    await().atMost(maxTimeout, TimeUnit.MILLISECONDS).until(sut.getBuffer()::size, greaterThanOrEqualTo(1));

    List<SparkBaseSupplierWrapperDto> buffer = sut.getAndClear();
    assertThat(buffer).hasSize(1);
    assertThat(sut.getBuffer()).hasSize(0);

    SparkBaseSupplierWrapperDto testObj = buffer.get(0);

    assertThat(testObj.getExecutorId()).isEqualTo("test-executor-id");

    assertThat(testObj.getOsInfoDto().getAvailableProcessors()).isGreaterThanOrEqualTo(1);
  }
}
