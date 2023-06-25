package com.asml.apa.wta.core.supplier;

import com.asml.apa.wta.core.dto.ProcDto;
import com.asml.apa.wta.core.utils.ShellUtils;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * ProcSupplier class.
 *
 * @author Lohithsai Yadala Chanchu
 * @since 1.0.0
 */
@Slf4j
public class ProcSupplier implements InformationSupplier<ProcDto> {
  private final ShellUtils shell;

  @Setter
  private boolean isProcAvailable;

  private final boolean isDiskMetricsAvailable;
  private final boolean isMemMetricsAvailable;
  private final boolean isCpuMetricsAvailable;
  private final boolean isLoadAvgMetricsAvailable;

  public ProcSupplier(ShellUtils shellUtils) {
    shell = shellUtils;
    isProcAvailable = isAvailable();
    this.isDiskMetricsAvailable =
        shell.executeCommand("cat /proc/diskstats", true).join() != null;
    this.isMemMetricsAvailable =
        shell.executeCommand("cat /proc/meminfo", true).join() != null;
    this.isCpuMetricsAvailable = shell.executeCommand(
                "grep -m 1 \"model name\" /proc/cpuinfo | awk -F: '{print $2}' | sed 's/^[ \\t]*//'",
                true)
            .join()
        != null;
    this.isLoadAvgMetricsAvailable =
        shell.executeCommand("cat /proc/loadavg", true).join() != null;
  }

  /**
   * Checks if the system runs Linux.
   *
   * @return a {@code boolean} that represents if the proc directory is available
   * @author Lohithsai Yadala Chanchu
   * @since 1.0.0
   */
  @Override
  public boolean isAvailable() {
    return System.getProperty("os.name").toLowerCase().contains("linux");
  }

  /**
   * Gets information from proc directory to get disk and memory metrics.
   *
   * @return CompletableFuture&lt;ProcDto&gt; that will be sent to the driver
   * @author Lohithsai Yadala Chanchu
   * @since 1.0.0
   */
  public CompletableFuture<Optional<ProcDto>> getSnapshot() {
    if (!isProcAvailable) {
      return notAvailableResult();
    }

    CompletableFuture<Optional<Long>[]> diskStatsFuture;
    CompletableFuture<Optional<Long>[]> memStatsFuture;
    CompletableFuture<Optional<String>> cpuModelFuture;
    CompletableFuture<Optional<Double>[]> loadAvgStatsFuture;
    if (this.isDiskMetricsAvailable) {
      diskStatsFuture = getDiskMetrics();
    } else {
      diskStatsFuture = CompletableFuture.completedFuture(
          Stream.generate(Optional::empty).limit(17).toArray(Optional[]::new));
    }
    if (this.isMemMetricsAvailable) {
      memStatsFuture = getMemMetrics();
    } else {
      memStatsFuture = CompletableFuture.completedFuture(
          Stream.generate(Optional::empty).limit(60).toArray(Optional[]::new));
    }
    if (this.isCpuMetricsAvailable) {
      cpuModelFuture = getCpuModel();
    } else {
      cpuModelFuture = CompletableFuture.completedFuture(Optional.empty());
    }
    if (this.isLoadAvgMetricsAvailable) {
      loadAvgStatsFuture = getLoadAvgMetrics();
    } else {
      loadAvgStatsFuture = CompletableFuture.completedFuture(
          Stream.generate(Optional::empty).limit(6).toArray(Optional[]::new));
    }

    CompletableFuture<Optional<String>> finalCpuModelFuture = cpuModelFuture;
    CompletableFuture<Optional<Double>[]> finalLoadAvgStatsFuture = loadAvgStatsFuture;
    return CompletableFuture.allOf(diskStatsFuture, memStatsFuture, cpuModelFuture, loadAvgStatsFuture)
        .thenApply((v) -> {
          Optional<Long>[] diskResult = diskStatsFuture.join();
          Optional<Long>[] memResult = memStatsFuture.join();
          Optional<String> cpuModel = finalCpuModelFuture.join();
          Optional<Double>[] loadAvgResult = finalLoadAvgStatsFuture.join();

          return Optional.of(ProcDto.builder()
              .readsCompleted(diskResult[0].orElse(-1L))
              .readsMerged(diskResult[1].orElse(-1L))
              .sectorsRead(diskResult[2].orElse(-1L))
              .timeSpentReading(diskResult[3].orElse(-1L))
              .writesCompleted(diskResult[4].orElse(-1L))
              .writesMerged(diskResult[5].orElse(-1L))
              .sectorsWritten(diskResult[6].orElse(-1L))
              .timeSpentWriting(diskResult[7].orElse(-1L))
              .iosInProgress(diskResult[8].orElse(-1L))
              .timeSpentDoingIos(diskResult[9].orElse(-1L))
              .weightedTimeSpentDoingIos(diskResult[9].orElse(-1L))
              .discardsCompleted(diskResult[10].orElse(-1L))
              .discardsMerged(diskResult[11].orElse(-1L))
              .sectorsDiscarded(diskResult[12].orElse(-1L))
              .timeSpentDiscarding(diskResult[13].orElse(-1L))
              .flushReqCompleted(diskResult[14].orElse(-1L))
              .timeSpentFlushing(diskResult[15].orElse(-1L))
              .memTotal(memResult[0].orElse(-1L))
              .memFree(memResult[1].orElse(-1L))
              .memAvailable(memResult[2].orElse(-1L))
              .buffers(memResult[3].orElse(-1L))
              .cached(memResult[4].orElse(-1L))
              .swapCached(memResult[5].orElse(-1L))
              .active(memResult[6].orElse(-1L))
              .inactive(memResult[7].orElse(-1L))
              .activeAnon(memResult[8].orElse(-1L))
              .inactiveAnon(memResult[9].orElse(-1L))
              .activeFile(memResult[10].orElse(-1L))
              .inactiveFile(memResult[11].orElse(-1L))
              .unevictable(memResult[12].orElse(-1L))
              .mLocked(memResult[13].orElse(-1L))
              .swapTotal(memResult[14].orElse(-1L))
              .swapFree(memResult[15].orElse(-1L))
              .dirty(memResult[16].orElse(-1L))
              .writeback(memResult[17].orElse(-1L))
              .anonPages(memResult[18].orElse(-1L))
              .mapped(memResult[19].orElse(-1L))
              .shmem(memResult[20].orElse(-1L))
              .kReclaimable(memResult[21].orElse(-1L))
              .slab(memResult[22].orElse(-1L))
              .sReclaimable(memResult[23].orElse(-1L))
              .sUnreclaim(memResult[24].orElse(-1L))
              .kernelStack(memResult[25].orElse(-1L))
              .pageTables(memResult[26].orElse(-1L))
              .nfsUnstable(memResult[27].orElse(-1L))
              .bounce(memResult[28].orElse(-1L))
              .writebackTmp(memResult[29].orElse(-1L))
              .commitLimit(memResult[30].orElse(-1L))
              .committedAs(memResult[31].orElse(-1L))
              .vMallocTotal(memResult[32].orElse(-1L))
              .vMallocUsed(memResult[33].orElse(-1L))
              .vMallocChunk(memResult[34].orElse(-1L))
              .percpu(memResult[35].orElse(-1L))
              .anonHugePages(memResult[36].orElse(-1L))
              .shmemHugePages(memResult[37].orElse(-1L))
              .shmemPmdMapped(memResult[38].orElse(-1L))
              .fileHugePages(memResult[39].orElse(-1L))
              .filePmdMapped(memResult[40].orElse(-1L))
              .hugePagesTotal(memResult[41].orElse(-1L))
              .hugePagesFree(memResult[42].orElse(-1L))
              .hugePagesRsvd(memResult[43].orElse(-1L))
              .hugePagesSurp(memResult[44].orElse(-1L))
              .hugePageSize(memResult[45].orElse(-1L))
              .hugetlb(memResult[46].orElse(-1L))
              .directMap4k(memResult[47].orElse(-1L))
              .directMap2M(memResult[48].orElse(-1L))
              .directMap1G(memResult[49].orElse(-1L))
              .cpuModel(cpuModel.orElse("unknown"))
              .loadAvgOneMinute(loadAvgResult[0].orElse(-1.0))
              .loadAvgFiveMinutes(loadAvgResult[1].orElse(-1.0))
              .loadAvgFifteenMinutes(loadAvgResult[2].orElse(-1.0))
              .numberOfExecutingKernelSchedulingEntities(loadAvgResult[3].orElse(-1.0))
              .numberOfExistingKernelSchedulingEntities(loadAvgResult[4].orElse(-1.0))
              .pIdOfMostRecentlyCreatedProcess(loadAvgResult[5].orElse(-1.0))
              .build());
        });
  }

  /**
   * Get contents of /proc/meminfo.
   *
   * @return CompletableFuture&lt;Optional&lt;Long&gt;[]&gt; of the parsed numbers from the /proc/meminfo file
   * @author Lohithsai Yadala Chanchu
   * @since 1.0.0
   */
  private CompletableFuture<Optional<Long>[]> getMemMetrics() {
    CompletableFuture<String> memMetrics = shell.executeCommand("cat /proc/meminfo", false);

    return memMetrics.thenApply(result -> {
      Optional<Long>[] agg = Stream.generate(Optional::empty).limit(60).toArray(Optional[]::new);

      if (result != null && !result.isEmpty() && !fileNotFound(result)) {
        List<Long> parsedList = parseMemMetrics(result);
        IntStream.range(0, parsedList.size()).forEach(i -> agg[i] = Optional.of(parsedList.get(i)));
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
    CompletableFuture<String> diskMetrics = shell.executeCommand("cat /proc/diskstats", false);

    return diskMetrics.thenApply(result -> {
      Optional<Long>[] agg = Stream.generate(Optional::empty).limit(17).toArray(Optional[]::new);
      if (result != null && !result.isEmpty() && !fileNotFound(result)) {
        List<OutputLine> parsedList = parseDiskMetrics(result);
        int rowLength = parsedList.get(0).getLineLength();
        for (OutputLine line : parsedList) {
          for (int inner = 3; inner < rowLength; inner++) {
            try {
              agg[inner - 3] =
                  Optional.of(agg[inner - 3].orElse(0L) + Long.parseLong(line.getElementAt(inner)));
            } catch (NumberFormatException e) {
              log.error("There was an error parsing the contents of the /proc/diskstats file");
            }
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
    CompletableFuture<String> cpuMetrics = shell.executeCommand(
        "grep -m 1 \"model name\" /proc/cpuinfo | awk -F: '{print $2}' | sed 's/^[ \\t]*//'", false);
    return cpuMetrics.thenApply(result -> {
      if (result != null && !result.isEmpty() && !fileNotFound(result)) {
        return Optional.of(result);
      }
      return Optional.empty();
    });
  }

  /**
   * Get loadAvgStats from /proc/loadavg.
   *
   * @return CompletableFuture of the parsed numbers from the /proc/loadavg file
   * @author Lohithsai Yadala Chanchu
   * @since 1.0.0
   */
  private CompletableFuture<Optional<Double>[]> getLoadAvgMetrics() {
    CompletableFuture<String> loadAvgMetrics = shell.executeCommand("cat /proc/loadavg", false);

    Pattern pattern = Pattern.compile("\\d+(?:[.,]\\d+)?");

    return loadAvgMetrics.thenApply(result -> {
      Optional<Double>[] agg = (Optional<Double>[]) new Optional<?>[6];
      Arrays.fill(agg, Optional.empty());
      if (result != null && !result.isEmpty() && !fileNotFound(result)) {
        Matcher matcher = pattern.matcher(result);
        for (int index = 0; index < agg.length && matcher.find(); index++) {
          try {
            agg[index] =
                Optional.of(Double.parseDouble(matcher.group().replace(',', '.')));
          } catch (NumberFormatException e) {
            log.error("There was an error parsing the contents of the /proc/loadavg file");
          }
        }
      }
      return agg;
    });
  }

  /**
   * Parse /proc/diskstats.
   *
   * @param input the input to be parsed
   * @return List&lt;List&lt;String&gt;&gt; of the parsed numbers from the /proc/diskstats file
   * @author Lohithsai Yadala Chanchu
   * @since 1.0.0
   */
  private List<OutputLine> parseDiskMetrics(String input) {
    List<OutputLine> result = new ArrayList<>();

    try (BufferedReader reader = new BufferedReader(new StringReader(input))) {
      String line;
      while ((line = reader.readLine()) != null) {
        String[] tokens = line.trim().split("\\s+");

        OutputLine outputLine = new OutputLine();
        for (String token : tokens) {
          outputLine.addToLine(token);
        }
        result.add(outputLine);
      }
    } catch (IOException e) {
      log.error("Something went wrong while parsing the contents of /proc/diskstats");
    }

    return result;
  }

  /**
   * Parse /proc/diskstats.
   *
   * @param input the input to be parsed
   * @return List&lt;Long&gt; of the parsed numbers from the /proc/meminfo file
   * @author Lohithsai Yadala Chanchu
   * @since 1.0.0
   */
  private List<Long> parseMemMetrics(String input) {
    String[] lines = input.split("\n");
    Pattern pattern = Pattern.compile("\\b\\d+\\b");

    List<Long> numbersList = Arrays.stream(lines)
        .flatMap(line -> pattern.matcher(line).results())
        .map(matchResult -> {
          try {
            return Long.parseLong(matchResult.group());
          } catch (NumberFormatException e) {
            log.error("There was an error parsing the contents of the /proc/meminfo file");
            return null;
          }
        })
        .collect(Collectors.toList());
    return numbersList;
  }

  /**
   * Checks the terminal output if the file that is going to be accessed exists.
   *
   * @param output the output of the cat terminal command
   * @return a boolean value that represents if the file was not found
   * @author Lohithsai Yadala Chanchu
   * @since 1.0.0
   */
  private boolean fileNotFound(String output) {
    return output.contains("No such file or directory");
  }

  /**
   * Container class to be used in nested String Lists.
   *
   * @author Lohithsai Yadala Chanchu
   * @since 1.0.0
   */
  @NoArgsConstructor
  @Data
  private class OutputLine {
    private List<String> outputLine = new ArrayList<>();

    /**
     * Add an element to the outputLine.
     *
     * @param element the string to be added to the outputLine
     * @author Lohithsai Yadala Chanchu
     * @since 1.0.0
     */
    public void addToLine(String element) {
      this.outputLine.add(element);
    }

    /**
     * Gets the length of the outputLine.
     *
     * @return the length of the outputLine
     * @author Lohithsai Yadala Chanchu
     * @since 1.0.0
     */
    public int getLineLength() {
      return this.outputLine.size();
    }

    /**
     * Gets the element at the specified index.
     *
     * @param index the index of the element we want to receive
     * @return the string that is at the specified index
     * @author Lohithsai Yadala Chanchu
     * @since 1.0.0
     */
    public String getElementAt(int index) {
      return this.outputLine.get(index);
    }
  }
}
