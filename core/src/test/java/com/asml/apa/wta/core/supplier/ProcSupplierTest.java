package com.asml.apa.wta.core.supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;

import com.asml.apa.wta.core.dto.ProcDto;
import com.asml.apa.wta.core.util.ShellRunner;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ProcSupplierTest {
  @Test
  void getSnapshotReturnsProcDto() {
    ShellRunner shellRunner = Mockito.mock(ShellRunner.class);
    doReturn(CompletableFuture.completedFuture("8       0 sda 1114 437 141266 153\n"
            + "8      16 sdb 103 0 4712 174\n" + "8      32 sdc 77636 10312 5307586 5345"))
        .when(shellRunner)
        .executeCommand("cat /proc/diskstats", false);
    doReturn(CompletableFuture.completedFuture("8       0 sda 1114 437 141266 153\n"
            + "8      16 sdb 103 0 4712 174\n" + "8      32 sdc 77636 10312 5307586 5345"))
        .when(shellRunner)
        .executeCommand("cat /proc/diskstats", true);

    doReturn(CompletableFuture.completedFuture("MemTotal:       10118252 kB\n" + "MemFree:         1921196 kB\n"
            + "MemAvailable:    5470300 kB\n"
            + "Buffers:          239068 kB\n"))
        .when(shellRunner)
        .executeCommand("cat /proc/meminfo", false);

    doReturn(CompletableFuture.completedFuture("MemTotal:       10118252 kB\n" + "MemFree:         1921196 kB\n"
            + "MemAvailable:    5470300 kB\n"
            + "Buffers:          239068 kB\n"))
        .when(shellRunner)
        .executeCommand("cat /proc/meminfo", true);

    doReturn(CompletableFuture.completedFuture("Intel(R) Core(TM) i7-10750H CPU @ 2.60GHz"))
        .when(shellRunner)
        .executeCommand(
            "grep -m 1 \"model name\" /proc/cpuinfo | awk -F: '{print $2}' | sed 's/^[ \\t]*//'", false);

    doReturn(CompletableFuture.completedFuture("Intel(R) Core(TM) i7-10750H CPU @ 2.60GHz"))
        .when(shellRunner)
        .executeCommand(
            "grep -m 1 \"model name\" /proc/cpuinfo | awk -F: '{print $2}' | sed 's/^[ \\t]*//'", true);

    doReturn(CompletableFuture.completedFuture("0,62 1.23 1.02 1/479 278339"))
        .when(shellRunner)
        .executeCommand("cat /proc/loadavg", false);
    doReturn(CompletableFuture.completedFuture("0,62 1.23 1.02 1/479 278339"))
        .when(shellRunner)
        .executeCommand("cat /proc/loadavg", true);
    ProcSupplier sut = new ProcSupplier(shellRunner);
    sut.setProcAvailable(true);

    ProcDto expected = ProcDto.builder()
        .readsCompleted(78853L)
        .readsMerged(10749L)
        .sectorsRead(5453564L)
        .timeSpentReading(5672L)
        .memTotal(10118252L)
        .memFree(1921196L)
        .memAvailable(5470300L)
        .buffers(239068L)
        .cpuModel("Intel(R) Core(TM) i7-10750H CPU @ 2.60GHz")
        .loadAvgOneMinute(0.62)
        .loadAvgFiveMinutes(1.23)
        .loadAvgFifteenMinutes(1.02)
        .numberOfExecutingKernelSchedulingEntities(1.0)
        .numberOfExistingKernelSchedulingEntities(479.0)
        .pIdOfMostRecentlyCreatedProcess(278339.0)
        .build();

    assertEquals(expected, sut.getSnapshot().join().get());
  }

  @Test
  void emptyOutputReturnEmptyProcDto() {
    ShellRunner shellRunner = Mockito.mock(ShellRunner.class);
    doReturn(CompletableFuture.completedFuture("")).when(shellRunner).executeCommand("cat /proc/diskstats", false);
    doReturn(CompletableFuture.completedFuture("")).when(shellRunner).executeCommand("cat /proc/diskstats", true);

    doReturn(CompletableFuture.completedFuture("")).when(shellRunner).executeCommand("cat /proc/meminfo", false);
    doReturn(CompletableFuture.completedFuture("")).when(shellRunner).executeCommand("cat /proc/meminfo", true);

    doReturn(CompletableFuture.completedFuture(""))
        .when(shellRunner)
        .executeCommand(
            "grep -m 1 \"model name\" /proc/cpuinfo | awk -F: '{print $2}' | sed 's/^[ \\t]*//'", false);
    doReturn(CompletableFuture.completedFuture(""))
        .when(shellRunner)
        .executeCommand(
            "grep -m 1 \"model name\" /proc/cpuinfo | awk -F: '{print $2}' | sed 's/^[ \\t]*//'", true);

    doReturn(CompletableFuture.completedFuture("")).when(shellRunner).executeCommand("cat /proc/loadavg", false);
    doReturn(CompletableFuture.completedFuture("")).when(shellRunner).executeCommand("cat /proc/loadavg", true);
    ProcSupplier sut = new ProcSupplier(shellRunner);
    sut.setProcAvailable(true);

    ProcDto expected = ProcDto.builder().build();

    assertEquals(expected, sut.getSnapshot().join().get());
  }

  @Test
  void testNoFilesInsideProc() {
    ShellRunner shellRunner = Mockito.mock(ShellRunner.class);
    doReturn(CompletableFuture.completedFuture(null))
        .when(shellRunner)
        .executeCommand("cat /proc/diskstats", false);
    doReturn(CompletableFuture.completedFuture(null)).when(shellRunner).executeCommand("cat /proc/diskstats", true);

    doReturn(CompletableFuture.completedFuture(null)).when(shellRunner).executeCommand("cat /proc/meminfo", false);
    doReturn(CompletableFuture.completedFuture(null)).when(shellRunner).executeCommand("cat /proc/meminfo", true);

    doReturn(CompletableFuture.completedFuture(null))
        .when(shellRunner)
        .executeCommand(
            "grep -m 1 \"model name\" /proc/cpuinfo | awk -F: '{print $2}' | sed 's/^[ \\t]*//'", false);
    doReturn(CompletableFuture.completedFuture(null))
        .when(shellRunner)
        .executeCommand(
            "grep -m 1 \"model name\" /proc/cpuinfo | awk -F: '{print $2}' | sed 's/^[ \\t]*//'", true);

    doReturn(CompletableFuture.completedFuture(null)).when(shellRunner).executeCommand("cat /proc/loadavg", false);
    doReturn(CompletableFuture.completedFuture(null)).when(shellRunner).executeCommand("cat /proc/loadavg", true);
    ProcSupplier sut = new ProcSupplier(shellRunner);
    sut.setProcAvailable(true);

    ProcDto expected = ProcDto.builder().build();

    assertEquals(expected, sut.getSnapshot().join().get());
  }
}
