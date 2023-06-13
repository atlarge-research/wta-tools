package com.asml.apa.wta.core.supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.spy;

import com.asml.apa.wta.core.dto.OsInfoDto;
import org.junit.jupiter.api.Test;

class OperatingSystemSupplierTest {

  OperatingSystemSupplier sut = spy(OperatingSystemSupplier.class);

  @Test
  void getCommittedVirtualMemorySize() {
    assertThat(sut.isAvailable()).isTrue();
    assertThat(sut.getCommittedVirtualMemorySize()).isGreaterThanOrEqualTo(-1);
  }

  @Test
  void getFreePhysicalMemorySize() {
    assertThat(sut.isAvailable()).isTrue();
    assertThat(sut.getFreePhysicalMemorySize()).isGreaterThanOrEqualTo(0);
  }

  @Test
  void getProcessCpuLoad() {
    assertThat(sut.isAvailable()).isTrue();
    double load = sut.getProcessCpuLoad();
    assertThat(load).isGreaterThanOrEqualTo(0.0);
    assertThat(load).isLessThanOrEqualTo(1.0);
  }

  @Test
  void getProcessCpuTime() {
    assertThat(sut.isAvailable()).isTrue();
    assertThat(sut.getProcessCpuTime()).isGreaterThanOrEqualTo(-1);
  }

  @Test
  void getTotalPhysicalMemorySize() {
    assertThat(sut.isAvailable()).isTrue();
    assertThat(sut.getTotalPhysicalMemorySize()).isGreaterThanOrEqualTo(0);
  }

  @Test
  void getAvailableProcessors() {
    assertThat(sut.isAvailable()).isTrue();
    assertThat(sut.getAvailableProcessors()).isGreaterThanOrEqualTo(1);
  }

  @Test
  void getArchitecture() {
    assertThat(sut.getArch()).isNotBlank();
  }

  @Test
  void getSnapshot() {
    assertNotEquals(sut.getSnapshot(), null);
    assertTrue(sut.getSnapshot().join() instanceof OsInfoDto);
  }
}
