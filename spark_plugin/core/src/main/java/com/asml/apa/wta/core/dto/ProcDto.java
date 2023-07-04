package com.asml.apa.wta.core.dto;

import com.asml.apa.wta.core.supplier.ProcSupplier;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Data transfer object for the {@link ProcSupplier}.
 *
 * @author Lohithsai Yadala Chanchu
 * @since 1.0.0
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ProcDto implements SupplierDto {

  private static final long serialVersionUID = -5587452226552818612L;

  @Builder.Default
  private long readsCompleted = -1;

  @Builder.Default
  private long readsMerged = -1;

  @Builder.Default
  private long sectorsRead = -1;

  @Builder.Default
  private long timeSpentReading = -1;

  @Builder.Default
  private long writesCompleted = -1;

  @Builder.Default
  private long writesMerged = -1;

  @Builder.Default
  private long sectorsWritten = -1;

  @Builder.Default
  private long timeSpentWriting = -1;

  @Builder.Default
  private long iosInProgress = -1;

  @Builder.Default
  private long timeSpentDoingIos = -1;

  @Builder.Default
  private long weightedTimeSpentDoingIos = -1;

  @Builder.Default
  private long discardsCompleted = -1;

  @Builder.Default
  private long discardsMerged = -1;

  @Builder.Default
  private long sectorsDiscarded = -1;

  @Builder.Default
  private long timeSpentDiscarding = -1;

  @Builder.Default
  private long flushReqCompleted = -1;

  @Builder.Default
  private long timeSpentFlushing = -1;

  @Builder.Default
  private long hugePagesTotal = -1;

  @Builder.Default
  private long hugePagesFree = -1;

  @Builder.Default
  private long hugePagesRsvd = -1;

  @Builder.Default
  private long hugePagesSurp = -1;

  // Units for below metrics are all in kB
  @Builder.Default
  private long memTotal = -1;

  @Builder.Default
  private long memFree = -1;

  @Builder.Default
  private long memAvailable = -1;

  @Builder.Default
  private long buffers = -1;

  @Builder.Default
  private long cached = -1;

  @Builder.Default
  private long swapCached = -1;

  @Builder.Default
  private long active = -1;

  @Builder.Default
  private long inactive = -1;

  @Builder.Default
  private long activeAnon = -1;

  @Builder.Default
  private long inactiveAnon = -1;

  @Builder.Default
  private long activeFile = -1;

  @Builder.Default
  private long inactiveFile = -1;

  @Builder.Default
  private long unevictable = -1;

  @Builder.Default
  private long mLocked = -1;

  @Builder.Default
  private long swapTotal = -1;

  @Builder.Default
  private long swapFree = -1;

  @Builder.Default
  private long dirty = -1;

  @Builder.Default
  private long writeback = -1;

  @Builder.Default
  private long anonPages = -1;

  @Builder.Default
  private long mapped = -1;

  @Builder.Default
  private long shmem = -1;

  @Builder.Default
  private long kReclaimable = -1;

  @Builder.Default
  private long slab = -1;

  @Builder.Default
  private long sReclaimable = -1;

  @Builder.Default
  private long sUnreclaim = -1;

  @Builder.Default
  private long kernelStack = -1;

  @Builder.Default
  private long pageTables = -1;

  @Builder.Default
  private long nfsUnstable = -1;

  @Builder.Default
  private long bounce = -1;

  @Builder.Default
  private long writebackTmp = -1;

  @Builder.Default
  private long commitLimit = -1;

  @Builder.Default
  private long committedAs = -1;

  @Builder.Default
  private long vMallocTotal = -1;

  @Builder.Default
  private long vMallocUsed = -1;

  @Builder.Default
  private long vMallocChunk = -1;

  @Builder.Default
  private long percpu = -1;

  @Builder.Default
  private long anonHugePages = -1;

  @Builder.Default
  private long shmemHugePages = -1;

  @Builder.Default
  private long shmemPmdMapped = -1;

  @Builder.Default
  private long fileHugePages = -1;

  @Builder.Default
  private long filePmdMapped = -1;

  @Builder.Default
  private long hugePageSize = -1;

  @Builder.Default
  private long hugetlb = -1;

  @Builder.Default
  private long directMap4k = -1;

  @Builder.Default
  private long directMap2M = -1;

  @Builder.Default
  private long directMap1G = -1;

  @Builder.Default
  private String cpuModel = "unknown";

  @Builder.Default
  private double loadAvgOneMinute = -1.0;

  @Builder.Default
  private double loadAvgFiveMinutes = -1.0;

  @Builder.Default
  private double loadAvgFifteenMinutes = -1.0;

  @Builder.Default
  private double numberOfExecutingKernelSchedulingEntities = -1.0;

  @Builder.Default
  private double numberOfExistingKernelSchedulingEntities = -1.0;

  @Builder.Default
  private double pIdOfMostRecentlyCreatedProcess = -1.0;
}
