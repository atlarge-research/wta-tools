package com.asml.apa.wta.core.supplier;

import com.asml.apa.wta.core.dto.ProcDto;
import com.asml.apa.wta.core.utils.BashUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Scanner;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.SystemUtils;

/**
 * ProcSupplier class.
 *
 * @author Lohithsai Yadala Chanchu
 * @since 1.0.0
 */
public class ProcSupplier implements InformationSupplier<ProcDto> {
  private BashUtils bashUtils;
  private boolean isProcAvailable;

  public ProcSupplier(BashUtils bashUtils) {
    this.bashUtils = bashUtils;
    this.isProcAvailable = isAvailable();
  }

  /**
   * Checks if the proc directory is available.
   *
   * @return A boolean that represents if the proc directory is available
   * @author Lohithsai Yadala Chanchu
   * @since 1.0.0
   */
  @Override
  public boolean isAvailable() {
    return SystemUtils.IS_OS_LINUX;
  }

  /**
   * Gets information from proc directory to get disk and memory metrics.
   *
   * @return CompletableFuture&lt;ProcDto&gt; that will be sent to the driver
   * @author Lohithsai Yadala Chanchu
   * @since 1.0.0
   */
  @Override
  public CompletableFuture<ProcDto> getSnapshot() {
    if (isProcAvailable) {
      CompletableFuture<Optional<Long>[]> diskStats = getDiskMetrics();
      CompletableFuture<Optional<Long>[]> memStats = getMemMetrics();
      CompletableFuture<Optional<String>> cpuModel = getCpuModel();

      return diskStats
          .thenCombine(
              memStats,
              (diskResult, memResult) -> cpuModel.thenApply(cpuModelResult -> ProcDto.builder()
                  .readsCompleted(diskResult[0])
                  .readsMerged(diskResult[1])
                  .sectorsRead(diskResult[2])
                  .timeSpentReading(diskResult[3])
                  .writesCompleted(diskResult[4])
                  .writesMerged(diskResult[5])
                  .sectorsWritten(diskResult[6])
                  .timeSpentWriting(diskResult[7])
                  .iosInProgress(diskResult[8])
                  .timeSpentDoingIos(diskResult[9])
                  .weightedTimeSpentDoingIos(diskResult[9])
                  .discardsCompleted(diskResult[10])
                  .discardsMerged(diskResult[11])
                  .sectorsDiscarded(diskResult[12])
                  .timeSpentDiscarding(diskResult[13])
                  .flushReqCompleted(diskResult[14])
                  .timeSpentFlushing(diskResult[15])
                  .memTotal(memResult[0])
                  .memFree(memResult[1])
                  .memAvailable(memResult[2])
                  .buffers(memResult[3])
                  .cached(memResult[4])
                  .swapCached(memResult[5])
                  .active(memResult[6])
                  .inactive(memResult[7])
                  .activeAnon(memResult[8])
                  .inactiveAnon(memResult[9])
                  .activeFile(memResult[10])
                  .inactiveFile(memResult[11])
                  .unevictable(memResult[12])
                  .mLocked(memResult[13])
                  .swapTotal(memResult[14])
                  .swapFree(memResult[15])
                  .dirty(memResult[16])
                  .writeback(memResult[17])
                  .anonPages(memResult[18])
                  .mapped(memResult[19])
                  .shmem(memResult[20])
                  .kReclaimable(memResult[21])
                  .slab(memResult[22])
                  .sReclaimable(memResult[23])
                  .sUnreclaim(memResult[24])
                  .kernelStack(memResult[25])
                  .pageTables(memResult[26])
                  .nfsUnstable(memResult[27])
                  .bounce(memResult[28])
                  .writebackTmp(memResult[29])
                  .commitLimit(memResult[30])
                  .committedAs(memResult[31])
                  .vMallocTotal(memResult[32])
                  .vMallocUsed(memResult[33])
                  .vMallocChunk(memResult[34])
                  .percpu(memResult[35])
                  .anonHugePages(memResult[36])
                  .shmemHugePages(memResult[37])
                  .shmemPmdMapped(memResult[38])
                  .fileHugePages(memResult[39])
                  .filePmdMapped(memResult[40])
                  .hugePagesTotal(memResult[41])
                  .hugePagesFree(memResult[42])
                  .hugePagesRsvd(memResult[43])
                  .hugePagesSurp(memResult[44])
                  .hugePageSize(memResult[45])
                  .hugetlb(memResult[46])
                  .directMap4k(memResult[47])
                  .directMap2M(memResult[48])
                  .directMap1G(memResult[49])
                  .cpuModel(cpuModelResult)
                  .build()))
          .thenCompose(Function.identity());
    }
    return notAvailableResult();
  }

  /**
   * Get contents of /proc/meminfo.
   *
   * @return CompletableFuture&lt;Optional&lt;Long&gt;[]&gt; of the parsed numbers from the /proc/meminfo file
   * @author Lohithsai Yadala Chanchu
   * @since 1.0.0
   */
  private CompletableFuture<Optional<Long>[]> getMemMetrics() {
    CompletableFuture<String> memMetrics = bashUtils.executeCommand("cat /proc/meminfo");

    return memMetrics.thenApply(result -> {
      Optional<Long>[] agg = (Optional<Long>[]) new Optional<?>[50];
      if (result != null) {
        List<Long> parsedList = parseMemMetrics(result);
        Arrays.fill(agg, Optional.empty());
        for (int i = 0; i < parsedList.size(); i++) {
          agg[i] = Optional.of(parsedList.get(i));
        }
      }
      return agg;
    });
  }

  /**
   * Get contents /proc/diskstats.
   *
   * @return CompletableFuture&lt;Optional&lt;Long&gt;[]&gt; of the parsed numbers from the /proc/diskstats file
   * @author Lohithsai Yadala Chanchu
   * @since 1.0.0
   */
  private CompletableFuture<Optional<Long>[]> getDiskMetrics() {
    CompletableFuture<String> diskMetrics = bashUtils.executeCommand("cat /proc/diskstats");

    return diskMetrics.thenApply(result -> {
      Optional<Long>[] agg = (Optional<Long>[]) new Optional<?>[17];
      if (result != null) {
        List<List<String>> parsedList = parseDiskMetrics(result);
        Arrays.fill(agg, Optional.empty());
        for (int outer = 0; outer < parsedList.size(); outer++) {
          for (int inner = 3; inner < parsedList.get(0).size(); inner++) {
            agg[inner - 3] = Optional.of(agg[inner - 3].orElse(0L)
                + Long.parseLong(parsedList.get(outer).get(inner)));
          }
        }
      }
      return agg;
    });
  }

  /**
   * Get cpu model from /proc/cpuinfo.
   *
   * @return CompletableFuture&lt;Optional&lt;Long&gt;&gt; of the parsed cpu model from the /proc/cpuinfo file
   * @author Lohithsai Yadala Chanchu
   * @since 1.0.0
   */
  private CompletableFuture<Optional<String>> getCpuModel() {
    CompletableFuture<String> cpuMetrics = bashUtils.executeCommand("cat /proc/cpuinfo");
    Pattern pattern = Pattern.compile("model name\\s+:\\s+([^\\n]+)");

    return cpuMetrics.thenApply(result -> {
      if (result != null) {
        Matcher matcher = pattern.matcher(result);

        if (matcher.find()) {
          return Optional.of(matcher.group(1));
        }
      }
      return Optional.empty();
    });
  }

  /**
   * Parse /proc/diskstats.
   *
   * @return List&lt;List&lt;String&gt;&gt; of the parsed numbers from the /proc/diskstats file
   * @author Lohithsai Yadala Chanchu
   * @since 1.0.0
   */
  private List<List<String>> parseDiskMetrics(String input) {
    List<List<String>> result = new ArrayList<>();
    Scanner scanner = new Scanner(input);

    while (scanner.hasNextLine()) {
      String line = scanner.nextLine().trim();
      String[] tokens = line.split("\\s+");

      List<String> sublist = new ArrayList<>();
      for (String token : tokens) {
        sublist.add(token);
      }
      result.add(sublist);
    }

    return result;
  }

  /**
   * Parse /proc/diskstats.
   *
   * @return List&lt;Long&gt; of the parsed numbers from the /proc/meminfo file
   * @author Lohithsai Yadala Chanchu
   * @since 1.0.0
   */
  private List<Long> parseMemMetrics(String input) {
    List<Long> numbersList = new ArrayList<>();

    String[] lines = input.split("\n");
    Pattern pattern = Pattern.compile("\\b\\d+\\b");
    for (String line : lines) {
      Matcher matcher = pattern.matcher(line);
      while (matcher.find()) {
        long number = Long.parseLong(matcher.group());
        numbersList.add(number);
      }
    }
    return numbersList;
  }
}
