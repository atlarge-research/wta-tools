package com.asml.apa.wta.core.supplier;

import static org.assertj.core.api.Assertions.assertThat;

import com.asml.apa.wta.core.dto.ProcDto;
import com.asml.apa.wta.core.utils.ShellUtils;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class ProcSupplierIntegrationTest {
  @Test
  @Timeout(value = 10000L, unit = TimeUnit.MILLISECONDS)
  public void procSuccesfullyReturnsDtoObject() {
    ProcSupplier a = new ProcSupplier(new ShellUtils());
    Optional<ProcDto> actual = a.getSnapshot().join();
    if (a.isAvailable()) {
      assertThat(actual).isPresent();
      assertThat(actual.get().getMemTotal()).isNotNegative();
    } else {
      assertThat(actual).isEmpty();
    }
  }
}
