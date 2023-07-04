package com.asml.apa.wta.core.supplier;

import static org.assertj.core.api.Assertions.assertThat;

import com.asml.apa.wta.core.dto.JvmFileDto;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JavaFileSupplierIntegrationTest {

  private JavaFileSupplier sut;

  @BeforeEach
  void setup() {
    sut = new JavaFileSupplier();
  }

  @Test
  void testGenerationOfMetrics() {
    // sanity test to run on ancient systems
    assertThat(sut.isAvailable()).isTrue();
  }

  @Test
  void testGetMetrics() {
    CompletableFuture<Optional<JvmFileDto>> result = sut.getSnapshot();

    Optional<JvmFileDto> dto = result.join();

    assertThat(dto).isPresent();

    assertThat(dto.get().getTotalSpace()).isGreaterThan(0);
    assertThat(dto.get().getFreeSpace()).isGreaterThan(0);
    assertThat(dto.get().getUsableSpace()).isGreaterThan(0);
  }
}
