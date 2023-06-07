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

  private Optional<Long> readsCompleted = Optional.empty();

  private Optional<Long> readsMerged = Optional.empty();

  private Optional<Long> sectorsRead = Optional.empty();

  private Optional<Long> timeSpentReading = Optional.empty();

  private Optional<Long> writesCompleted = Optional.empty();

  private Optional<Long> writesMerged = Optional.empty();

  private Optional<Long> sectorsWritten = Optional.empty();

  private Optional<Long> timeSpentWriting = Optional.empty();

  private Optional<Long> iosInProgress = Optional.empty();

  private Optional<Long> timeSpentDoingIos = Optional.empty();

  private Optional<Long> weightedTimeSpentDoingIos = Optional.empty();

  private Optional<Long> discardsCompleted = Optional.empty();

  private Optional<Long> discardsMerged = Optional.empty();

  private Optional<Long> sectorsDiscarded = Optional.empty();

  private Optional<Long> timeSpentDiscarding = Optional.empty();

  private Optional<Long> flushReqCompleted = Optional.empty();

  private Optional<Long> timeSpentFlushing = Optional.empty();

  private Optional<Long> hugePagesTotal = Optional.empty();

  private Optional<Long> hugePagesFree = Optional.empty();

  private Optional<Long> hugePagesRsvd = Optional.empty();

  private Optional<Long> hugePagesSurp = Optional.empty();

  // Units for below metrics are all in kB

  private Optional<Long> memTotal = Optional.empty();

  private Optional<Long> memFree = Optional.empty();

  private Optional<Long> memAvailable = Optional.empty();

  private Optional<Long> buffers = Optional.empty();

  private Optional<Long> cached = Optional.empty();

  private Optional<Long> swapCached = Optional.empty();

  private Optional<Long> active = Optional.empty();

  private Optional<Long> inactive = Optional.empty();

  private Optional<Long> activeAnon = Optional.empty();

  private Optional<Long> inactiveAnon = Optional.empty();

  private Optional<Long> activeFile = Optional.empty();

  private Optional<Long> inactiveFile = Optional.empty();

  private Optional<Long> unevictable = Optional.empty();

  private Optional<Long> mLocked = Optional.empty();

  private Optional<Long> swapTotal = Optional.empty();

  private Optional<Long> swapFree = Optional.empty();

  private Optional<Long> dirty = Optional.empty();

  private Optional<Long> writeback = Optional.empty();

  private Optional<Long> anonPages = Optional.empty();

  private Optional<Long> mapped = Optional.empty();

  private Optional<Long> shmem = Optional.empty();

  private Optional<Long> kReclaimable = Optional.empty();

  private Optional<Long> slab = Optional.empty();

  private Optional<Long> sReclaimable = Optional.empty();

  private Optional<Long> sUnreclaim = Optional.empty();

  private Optional<Long> kernelStack = Optional.empty();

  private Optional<Long> pageTables = Optional.empty();

  private Optional<Long> nfsUnstable = Optional.empty();

  private Optional<Long> bounce = Optional.empty();

  private Optional<Long> writebackTmp = Optional.empty();

  private Optional<Long> commitLimit = Optional.empty();

  private Optional<Long> committedAs = Optional.empty();

  private Optional<Long> vMallocTotal = Optional.empty();

  private Optional<Long> vMallocUsed = Optional.empty();

  private Optional<Long> vMallocChunk = Optional.empty();

  private Optional<Long> percpu = Optional.empty();

  private Optional<Long> anonHugePages = Optional.empty();

  private Optional<Long> shmemHugePages = Optional.empty();

  private Optional<Long> shmemPmdMapped = Optional.empty();

  private Optional<Long> fileHugePages = Optional.empty();

  private Optional<Long> filePmdMapped = Optional.empty();

  private Optional<Long> hugePageSize = Optional.empty();

  private Optional<Long> hugetlb = Optional.empty();

  private Optional<Long> directMap4k = Optional.empty();

  private Optional<Long> directMap2M = Optional.empty();

  private Optional<Long> directMap1G = Optional.empty();
}
