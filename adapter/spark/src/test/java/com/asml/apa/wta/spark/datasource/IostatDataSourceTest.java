package com.asml.apa.wta.spark.datasource;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.asml.apa.wta.spark.datasource.dto.IostatDataSourceDto;
import com.asml.apa.wta.spark.datasource.iodependencies.IostatDataSource;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;

public class IostatDataSourceTest {
  @Mock
  Logger log;

  @Test
  public void getAllMetricsReturnsIostatDto() throws IOException, InterruptedException {
    IostatDataSource sut = Mockito.spy(new IostatDataSource());

    Mockito.doReturn(CompletableFuture.completedFuture("1.0"))
        .when(sut)
        .executeCommand("iostat -d | awk '$1 == \"sdc\"' | awk '{print $2}'");
    Mockito.doReturn(CompletableFuture.completedFuture("2.0"))
        .when(sut)
        .executeCommand("iostat -d | awk '$1 == \"sdc\"' | awk '{print $3}'");
    Mockito.doReturn(CompletableFuture.completedFuture("3.0"))
        .when(sut)
        .executeCommand("iostat -d | awk '$1 == \"sdc\"' | awk '{print $4}'");
    Mockito.doReturn(CompletableFuture.completedFuture("4.0"))
        .when(sut)
        .executeCommand("iostat -d | awk '$1 == \"sdc\"' | awk '{print $5}'");
    Mockito.doReturn(CompletableFuture.completedFuture("5.0"))
        .when(sut)
        .executeCommand("iostat -d | awk '$1 == \"sdc\"' | awk '{print $6}'");
    Mockito.doReturn(CompletableFuture.completedFuture("6.0"))
        .when(sut)
        .executeCommand("iostat -d | awk '$1 == \"sdc\"' | awk '{print $7}'");
    Mockito.doReturn(CompletableFuture.completedFuture("7.0"))
        .when(sut)
        .executeCommand("iostat -d | awk '$1 == \"sdc\"' | awk '{print $8}'");

    IostatDataSourceDto expected = IostatDataSourceDto.builder()
        .tps(1.0)
        .kiloByteReadPerSec(2.0)
        .kiloByteWrtnPerSec(3.0)
        .kiloByteDscdPerSec(4.0)
        .kiloByteRead(5.0)
        .kiloByteWrtn(6.0)
        .kiloByteDscd(7.0)
        .executorId("driver")
        .build();

    IostatDataSourceDto result = sut.getAllMetrics("driver");

    // Assert everything field other than the timestamp is the same
    assertEquals(expected.getTps(), result.getTps());
    assertEquals(expected.getKiloByteReadPerSec(), result.getKiloByteReadPerSec());
    assertEquals(expected.getKiloByteWrtnPerSec(), result.getKiloByteWrtnPerSec());
    assertEquals(expected.getKiloByteDscdPerSec(), result.getKiloByteDscdPerSec());
    assertEquals(expected.getKiloByteRead(), result.getKiloByteRead());
    assertEquals(expected.getKiloByteWrtn(), result.getKiloByteWrtn());
    assertEquals(expected.getKiloByteDscd(), result.getKiloByteDscd());
  }
}
