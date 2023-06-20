package com.asml.apa.wta.core.supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;

import com.asml.apa.wta.core.dto.IostatDto;
import com.asml.apa.wta.core.utils.ShellUtils;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class IostatSupplierTest {

  ShellUtils shellUtils;
  IostatSupplier sut;

  @BeforeEach
  void setup() {
    shellUtils = Mockito.mock(ShellUtils.class);
    doReturn(CompletableFuture.completedFuture("str")).when(shellUtils).executeCommand("iostat", true);
    sut = Mockito.spy(new IostatSupplier(shellUtils));
  }

  @Test
  public void getSnapshotReturnsIostatDto() {
    doReturn(
            CompletableFuture.completedFuture(
                "Device             tps    kB_read/s    kB_wrtn/s    kB_dscd/s    kB_read    kB_wrtn    kB_dscd\n"
                    + "sda               0,01         0.54         0.00         0.00      70941          0          0\n"
                    + "str               1.0          2.0          3.0          4.0       5.0        6.0        7.0"))
        .when(shellUtils)
        .executeCommand("iostat -d", false);

    IostatDto expected = IostatDto.builder()
        .tps(1.01)
        .kiloByteReadPerSec(2.54)
        .kiloByteWrtnPerSec(3.0)
        .kiloByteDscdPerSec(4.0)
        .kiloByteRead(70946.0)
        .kiloByteWrtn(6.0)
        .kiloByteDscd(7.0)
        .build();

    Optional<IostatDto> result = sut.getSnapshot().join();

    assertEquals(expected, result.get());
  }

  @Test
  public void aggregateIostatWorksCorrectlyWithZeroRows() {
    doReturn(CompletableFuture.completedFuture("")).when(shellUtils).executeCommand("iostat -d", false);

    IostatDto expected = IostatDto.builder().build();

    Optional<IostatDto> result = sut.getSnapshot().join();

    assertEquals(expected, result.get());
  }

  @Test
  public void aggregateIostatWorksCorrectlyWithDiffColNumber() {
    doReturn(CompletableFuture.completedFuture(
            "Device             tps    kB_read/s    kB_wrtn/s    kB_dscd/s    kB_read    kB_wrtn\n"
                + "sda               0,01         0.54         0.00         0.00      70941          0\n"
                + "str               1.0          2.0          3.0          4.0       5.0        6.0"))
        .when(shellUtils)
        .executeCommand("iostat -d", false);

    IostatDto expected = IostatDto.builder().build();

    Optional<IostatDto> result = sut.getSnapshot().join();

    assertEquals(expected, result.get());
  }
}
