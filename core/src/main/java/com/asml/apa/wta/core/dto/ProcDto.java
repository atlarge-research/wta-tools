package com.asml.apa.wta.core.dto;

import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ProcDto implements SupplierDto {
  private static final long serialVersionUID = -8423791740389243058L;

  @Builder.Default
  private Optional<Long> readsCompleted = Optional.empty();

  @Builder.Default
  private Optional<Long> readsMerged = Optional.empty();

  @Builder.Default
  private Optional<Long> sectorsRead = Optional.empty();

  @Builder.Default
  private Optional<Long> timeSpentReading = Optional.empty();

  @Builder.Default
  private Optional<Long> writesCompleted = Optional.empty();

  @Builder.Default
  private Optional<Long> writesMerged = Optional.empty();

  @Builder.Default
  private Optional<Long> sectorsWritten = Optional.empty();

  @Builder.Default
  private Optional<Long> timeSpentWriting = Optional.empty();

  @Builder.Default
  private Optional<Long> iosInProgress = Optional.empty();

  @Builder.Default
  private Optional<Long> timeSpentDoingIos = Optional.empty();

  @Builder.Default
  private Optional<Long> weightedTimeSpentDoingIos = Optional.empty();

  @Builder.Default
  private Optional<Long> discardsCompleted = Optional.empty();

  @Builder.Default
  private Optional<Long> discardsMerged = Optional.empty();

  @Builder.Default
  private Optional<Long> sectorsDiscarded = Optional.empty();

  @Builder.Default
  private Optional<Long> timeSpentDiscarding = Optional.empty();

  @Builder.Default
  private Optional<Long> flushReqCompleted = Optional.empty();

  @Builder.Default
  private Optional<Long> timeSpentFlushing = Optional.empty();

  @Builder.Default
  private Optional<Long> hugePagesTotal = Optional.empty();

  @Builder.Default
  private Optional<Long> hugePagesFree = Optional.empty();

  @Builder.Default
  private Optional<Long> hugePagesRsvd = Optional.empty();

  @Builder.Default
  private Optional<Long> hugePagesSurp = Optional.empty();

  // Units for below metrics are all in kB

  @Builder.Default
  private Optional<Long> memTotal = Optional.empty();

  @Builder.Default
  private Optional<Long> memFree = Optional.empty();

  @Builder.Default
  private Optional<Long> memAvailable = Optional.empty();

  @Builder.Default
  private Optional<Long> buffers = Optional.empty();

  @Builder.Default
  private Optional<Long> cached = Optional.empty();

  @Builder.Default
  private Optional<Long> swapCached = Optional.empty();

  @Builder.Default
  private Optional<Long> active = Optional.empty();

  @Builder.Default
  private Optional<Long> inactive = Optional.empty();

  @Builder.Default
  private Optional<Long> activeAnon = Optional.empty();

  @Builder.Default
  private Optional<Long> inactiveAnon = Optional.empty();

  @Builder.Default
  private Optional<Long> activeFile = Optional.empty();

  @Builder.Default
  private Optional<Long> inactiveFile = Optional.empty();

  @Builder.Default
  private Optional<Long> unevictable = Optional.empty();

  @Builder.Default
  private Optional<Long> mLocked = Optional.empty();

  @Builder.Default
  private Optional<Long> swapTotal = Optional.empty();

  @Builder.Default
  private Optional<Long> swapFree = Optional.empty();

  @Builder.Default
  private Optional<Long> dirty = Optional.empty();

  @Builder.Default
  private Optional<Long> writeback = Optional.empty();

  @Builder.Default
  private Optional<Long> anonPages = Optional.empty();

  @Builder.Default
  private Optional<Long> mapped = Optional.empty();

  @Builder.Default
  private Optional<Long> shmem = Optional.empty();

  @Builder.Default
  private Optional<Long> kReclaimable = Optional.empty();

  @Builder.Default
  private Optional<Long> slab = Optional.empty();

  @Builder.Default
  private Optional<Long> sReclaimable = Optional.empty();

  @Builder.Default
  private Optional<Long> sUnreclaim = Optional.empty();

  @Builder.Default
  private Optional<Long> kernelStack = Optional.empty();

  @Builder.Default
  private Optional<Long> pageTables = Optional.empty();

  @Builder.Default
  private Optional<Long> nfsUnstable = Optional.empty();

  @Builder.Default
  private Optional<Long> bounce = Optional.empty();

  @Builder.Default
  private Optional<Long> writebackTmp = Optional.empty();

  @Builder.Default
  private Optional<Long> commitLimit = Optional.empty();

  @Builder.Default
  private Optional<Long> committedAs = Optional.empty();

  @Builder.Default
  private Optional<Long> vMallocTotal = Optional.empty();

  @Builder.Default
  private Optional<Long> vMallocUsed = Optional.empty();

  @Builder.Default
  private Optional<Long> vMallocChunk = Optional.empty();

  @Builder.Default
  private Optional<Long> percpu = Optional.empty();

  @Builder.Default
  private Optional<Long> anonHugePages = Optional.empty();

  @Builder.Default
  private Optional<Long> shmemHugePages = Optional.empty();

  @Builder.Default
  private Optional<Long> shmemPmdMapped = Optional.empty();

  @Builder.Default
  private Optional<Long> fileHugePages = Optional.empty();

  @Builder.Default
  private Optional<Long> filePmdMapped = Optional.empty();

  @Builder.Default
  private Optional<Long> hugePageSize = Optional.empty();

  @Builder.Default
  private Optional<Long> hugetlb = Optional.empty();

  @Builder.Default
  private Optional<Long> directMap4k = Optional.empty();

  @Builder.Default
  private Optional<Long> directMap2M = Optional.empty();

  @Builder.Default
  private Optional<Long> directMap1G = Optional.empty();
}
