package com.asml.apa.wta.core.datasource;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.asml.apa.wta.core.datasource.iodependencies.BashUtils;
import com.asml.apa.wta.core.datasource.iodependencies.IostatDataSource;
import com.asml.apa.wta.core.dto.IostatDataSourceDto;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class IostatDataSourceTest {

  @Test
  public void getAllMetricsReturnsIostatDto() throws IOException, InterruptedException, ExecutionException {
    BashUtils bashUtils = Mockito.mock(BashUtils.class);
    IostatDataSource sut = Mockito.spy(new IostatDataSource(bashUtils));

    Mockito.doReturn(CompletableFuture.completedFuture("str 1.0 2.0 3.0 4.0 5.0 6.0 7.0"))
        .when(bashUtils)
        .executeCommand("iostat -d | awk '$1 == \"sdc\"'");

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
