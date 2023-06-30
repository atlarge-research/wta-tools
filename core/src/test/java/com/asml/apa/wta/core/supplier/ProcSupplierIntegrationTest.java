package com.asml.apa.wta.core.supplier;

import static org.assertj.core.api.Assertions.assertThat;

import com.asml.apa.wta.core.dto.ProcDto;
import com.asml.apa.wta.core.util.ShellRunner;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class ProcSupplierIntegrationTest {
  @Test
  @Timeout(value = 10000L, unit = TimeUnit.MILLISECONDS)
  public void procSuccessfullyReturnsDtoObject() {
    ProcSupplier a = new ProcSupplier(new ShellRunner());
    Optional<ProcDto> actual = a.getSnapshot().join();
    if (a.isAvailable()) {
      assertThat(actual).isPresent();
      assertThat(actual.get().getMemTotal()).isNotNegative();
    } else {
      assertThat(actual).isEmpty();
    }
  }
}
