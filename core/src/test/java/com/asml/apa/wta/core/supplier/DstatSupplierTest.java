package com.asml.apa.wta.core.supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.asml.apa.wta.core.dto.DstatDto;
import com.asml.apa.wta.core.util.ShellRunner;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DstatSupplierTest {
  @Test
  void getSnapshotReturnsDstatDto() {
    ShellRunner shellRunner = mock(ShellRunner.class);
    if (System.getProperty("os.name").toLowerCase().contains("linux")) {
      when(shellRunner.executeCommand("dstat -cdngy 1 1", false))
          .thenReturn(CompletableFuture.completedFuture(
              "----total-usage---- -dsk/total- -net/total- ---paging-- ---system--\n"
                  + "usr sys idl wai stl| read  writ| recv  send|  in   out | int   csw\n"
                  + "  0   1M  98k   0   0|   0     0 |   10B     0 |   0B     0B | 516G  2116"));

      when(shellRunner.executeCommand("dstat -cdngy 1 1", true))
          .thenReturn(CompletableFuture.completedFuture(
              "----total-usage---- -dsk/total- -net/total- ---paging-- ---system--\n"
                  + "usr sys idl wai stl| read  writ| recv  send|  in   out | int   csw\n"
                  + "  0   1M  98k   0   0|   0     0 |   10B     0 |   0B     0B | 516G  2116"));
    }
    DstatSupplier sut = spy(new DstatSupplier(shellRunner));

    Optional<DstatDto> actual = sut.getSnapshot().join();

    DstatDto expected = DstatDto.builder()
        .totalUsageUsr(0L)
        .totalUsageSys(1000000L)
        .totalUsageIdl(98000L)
        .totalUsageWai(0L)
        .totalUsageStl(0L)
        .dskRead(0L)
        .dskWrite(0L)
        .netRecv(10L)
        .netSend(0L)
        .pagingIn(0L)
        .pagingOut(0L)
        .systemInt(516000000000L)
        .systemCsw(2116L)
        .build();

    if (sut.isAvailable()) {
      assertEquals(expected, actual.get());
    } else {
      assertEquals(Optional.empty(), actual);
    }
  }

  @Test
  void dstatNotAvailable() {
    ShellRunner shellRunner = mock(ShellRunner.class);
    if (System.getProperty("os.name").toLowerCase().contains("linux")) {
      when(shellRunner.executeCommand("dstat -cdngy 1 1", true))
          .thenReturn(CompletableFuture.completedFuture(null));
    }
    DstatSupplier sut = spy(new DstatSupplier(shellRunner));

    Optional<DstatDto> actual = sut.getSnapshot().join();

    assertEquals(Optional.empty(), actual);
  }

  @Test
  void getSnapshotWithDifferentOutputReturnsEmptyDstatDto() {
    ShellRunner shellRunner = mock(ShellRunner.class);
    if (System.getProperty("os.name").toLowerCase().contains("linux")) {
      when(shellRunner.executeCommand("dstat -cdngy 1 1", false))
          .thenReturn(CompletableFuture.completedFuture(
              "----total-usage---- -dsk/total- -net/total- ---paging-- ---system--\n"
                  + "usr sys idl wai stl|  recv  send|  in   out | int   csw\n"
                  + "  0   1M  98k   0   0|  10B     0 |   0B     0B | 516G  2116"));

      when(shellRunner.executeCommand("dstat -cdngy 1 1", true))
          .thenReturn(CompletableFuture.completedFuture(
              "----total-usage---- -dsk/total- -net/total- ---paging-- ---system--\n"
                  + "usr sys idl wai stl|  recv  send|  in   out | int   csw\n"
                  + "  0   1M  98k   0   0|  10B     0 |   0B     0B | 516G  2116"));
    }
    DstatSupplier sut = spy(new DstatSupplier(shellRunner));

    Optional<DstatDto> actual = sut.getSnapshot().join();

    DstatDto expected = DstatDto.builder()
        .totalUsageUsr(0L)
        .totalUsageSys(1000000L)
        .totalUsageIdl(98000L)
        .totalUsageWai(0L)
        .totalUsageStl(0L)
        .dskRead(0L)
        .dskWrite(0L)
        .netRecv(10L)
        .netSend(0L)
        .pagingIn(0L)
        .pagingOut(0L)
        .systemInt(516000000000L)
        .systemCsw(2116L)
        .build();

    assertEquals(Optional.empty(), actual);
  }
}
