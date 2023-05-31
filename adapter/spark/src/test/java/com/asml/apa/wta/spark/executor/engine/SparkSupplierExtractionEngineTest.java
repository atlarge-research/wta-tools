package com.asml.apa.wta.spark.executor.engine;

import com.asml.apa.wta.core.dto.BaseSupplierDto;
import com.asml.apa.wta.core.dto.IostatDto;
import com.asml.apa.wta.core.dto.OsInfoDto;
import com.asml.apa.wta.spark.dto.SparkBaseSupplierWrapperDto;
import org.apache.spark.api.plugin.PluginContext;
import org.apache.spark.internal.plugin.PluginContextImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.*;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


class SparkSupplierExtractionEngineTest {

    /**
     * We enforce a max timeout to prevent the test from hanging indefinitely.
     * This needs to be modified as dependencies take longer to resolve.
     */
    private static final long maxTimeout = 150L;

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

        BaseSupplierDto baseSupplierDto = new BaseSupplierDto(fakeOsInfo, fakeIoStatDto);


        SparkBaseSupplierWrapperDto result = sut.transform(baseSupplierDto);

        assertThat(result).isEqualTo(SparkBaseSupplierWrapperDto.builder().osInfoDto(fakeOsInfo).iostatDto(fakeIoStatDto).executorId("test-executor-id").build());


    }

    @Test
    @EnabledOnOs(OS.LINUX)
    void startAndStopPingingWorksAsIntended() {
        sut.startPinging();

        await().atMost(4, TimeUnit.SECONDS)
                        .until(sut.getBuffer()::size, greaterThanOrEqualTo(3));

        sut.stopPinging();

        int maintainedSize = sut.getBuffer().size();

        await()
                .during(3, TimeUnit.SECONDS)
                .atMost(4, TimeUnit.SECONDS)
                .until(() -> sut.getBuffer().size() == maintainedSize);

    }

    @Test
    @EnabledOnOs(OS.LINUX)
    void pingWorksAsIntended() {
        sut.ping();

        await().atMost(maxTimeout, TimeUnit.MILLISECONDS)
                .until(sut.getBuffer()::size, greaterThanOrEqualTo(1));

        List<SparkBaseSupplierWrapperDto> buffer = sut.getAndClear();
        assertThat(buffer).hasSize(1);
        assertThat(sut.getBuffer()).hasSize(0);

        SparkBaseSupplierWrapperDto testObj = buffer.get(0);

        assertThat(testObj.getExecutorId()).isEqualTo("test-executor-id");

        assertThat(testObj.getOsInfoDto().getAvailableProcessors()).isGreaterThanOrEqualTo(1);
    }

    @Test
    @EnabledOnOs(OS.LINUX)
    void oneThreadDoesNotDelayTheOtherThread() {
        sut.ping();
        sut.startPinging();

        await().atMost(maxTimeout, TimeUnit.MILLISECONDS)
                .until(sut.getBuffer()::size, greaterThanOrEqualTo(1));

        sut.getAndClear();

        await().atMost(3L, TimeUnit.SECONDS)
                .until(sut.getBuffer()::size, greaterThanOrEqualTo(3));
    }







}