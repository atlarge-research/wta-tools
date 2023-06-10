package com.asml.apa.wta.core.supplier;

import com.asml.apa.wta.core.dto.SupplierDto;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface InformationSupplier<T extends SupplierDto> {

  /**
   * Corresponds to whether a supplier is available, and able to give information.
   * Implementations should be inexpensive to call.
   *
   * @return true iff it is available, false otherwise
   */
  boolean isAvailable();

  /**
   * Gets a snapshot of the information in an async manner provided by the supplier.
   *
   * @return A {@link CompletableFuture} containing the snapshot of the information
   */
  CompletableFuture<Optional<T>> getSnapshot();
}
