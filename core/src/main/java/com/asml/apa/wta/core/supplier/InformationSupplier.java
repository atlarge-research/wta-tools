package com.asml.apa.wta.core.supplier;

import com.asml.apa.wta.core.dto.SupplierDto;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Interface for a supplier of information.
 *
 * @param <T> The type of dto that this supplier gives
 * @author Henry Page
 * @since 1.0.0
 */
public interface InformationSupplier<T extends SupplierDto> {

  /**
   * Corresponds to whether a supplier is available, and able to give information.
   * Implementations should be inexpensive to call.
   *
   * @return true iff it is available, false otherwise
   * @author Henry Page
   * @since 1.0.0
   */
  boolean isAvailable();

  /**
   * Gets a snapshot of the information in an async manner provided by the supplier.
   *
   * @return A {@link CompletableFuture} containing the snapshot of the information
   * @author Henry Page
   * @since 1.0.0
   */
  CompletableFuture<Optional<T>> getSnapshot();

  /**
   * Returns a {@link CompletableFuture} resolved with an empty {@link Optional}.
   *
   * @return A {@link CompletableFuture} with an empty Optional
   * @author Henry Page
   * @since 1.0.0
   */
  default CompletableFuture<Optional<T>> notAvailableResult() {
    return CompletableFuture.completedFuture(Optional.empty());
  }
}
