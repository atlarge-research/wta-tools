package com.asml.apa.wta.core.supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.asml.apa.wta.core.dto.IostatDto;
import com.asml.apa.wta.core.utils.BashUtils;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class IostatSupplierTest {

  BashUtils bashUtils;
  IostatSupplier sut;

  @BeforeEach
  void setup() {
    bashUtils = Mockito.mock(BashUtils.class);
    Mockito.doReturn(CompletableFuture.completedFuture("str"))
        .when(bashUtils)
        .executeCommand("iostat");
    sut = Mockito.spy(new IostatSupplier(bashUtils));
  }

  @Test
  public void getSnapshotReturnsIostatDto() {
    Mockito.doReturn(CompletableFuture.completedFuture("str 1.0 2.0 3.0 4.0 5.0 6.0 7.0"))
        .when(bashUtils)
        .executeCommand("iostat -d | awk '$1 == \"sdc\"'");

    IostatDto expected = IostatDto.builder()
        .tps(1.0)
        .kiloByteReadPerSec(2.0)
        .kiloByteWrtnPerSec(3.0)
        .kiloByteDscdPerSec(4.0)
        .kiloByteRead(5.0)
        .kiloByteWrtn(6.0)
        .kiloByteDscd(7.0)
        .build();

    IostatDto result = sut.getSnapshot().join();

    assertEquals(expected, result);
  }

  @Test
  public void getSnapshotThrowsException() {
    Mockito.doReturn(CompletableFuture.completedFuture("str 1.0 2.0 3.0 4.0 5.0 6.0"))
        .when(bashUtils)
        .executeCommand("iostat -d | awk '$1 == \"sdc\"'");

    assertThrows(CompletionException.class, () -> {
      sut.getSnapshot().join();
    });
  }
}
