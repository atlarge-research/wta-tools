package com.asml.apa.wta.core.supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import com.asml.apa.wta.core.dto.DstatDto;
import com.asml.apa.wta.core.utils.ShellUtils;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DstatSupplierTest {
  @Test
  void getSnapshotReturnsDstatDto() {
    ShellUtils shellUtils = mock(ShellUtils.class);
    doReturn(CompletableFuture.completedFuture(
            "----total-usage---- -dsk/total- -net/total- ---paging-- ---system--\n"
                + "usr sys idl wai stl| read  writ| recv  send|  in   out | int   csw\n"
                + "  0   1M  98k   0   0|   0     0 |   0     0 |   0B     0B | 516G  2116"))
        .when(shellUtils)
        .executeCommand("dstat -cdngy 1 1");
    DstatSupplier sut = spy(new DstatSupplier(shellUtils));

    Optional<DstatDto> actual = sut.getSnapshot().join();

    DstatDto expected = DstatDto.builder()
        .totalUsageUsr(0L)
        .totalUsageSys(1000000L)
        .totalUsageIdl(98000L)
        .totalUsageWai(0L)
        .totalUsageStl(0L)
        .dskRead(0L)
        .dskWrite(0L)
        .netRecv(0L)
        .netSend(0L)
        .pagingIn(0L)
        .pagingOut(0L)
        .systemInt(516000000000L)
        .systemCsw(2116L)
        .build();

    assertEquals(expected, actual.get());
  }
}
