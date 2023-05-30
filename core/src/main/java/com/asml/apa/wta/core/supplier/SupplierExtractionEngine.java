package com.asml.apa.wta.core.supplier;

import com.asml.apa.wta.core.dto.BigSupplierDto;
import com.asml.apa.wta.core.dto.IostatDto;
import com.asml.apa.wta.core.dto.OsInfoDto;
import java.util.concurrent.CompletableFuture;
import lombok.Getter;

/**
 * <h1>Resource Extraction Engine</h1>
 * <p>Used to extract resources from dependencies. Should be instantiated once per executor/node</p>
 *
 * @author Henry Page
 * @since 1.0.0
 */
public class SupplierExtractionEngine {

  protected final OperatingSystemSupplier operatingSystemSupplier;

  protected final IostatSupplier iostatSupplier;

  @Getter
  protected final Collection<BigSupplierDto> buffer = Collections.synchronizedCollection(new LinkedList<>());

  /**
   * Constructor for the resource extraction engine.
   * Suppliers should be injected here.
   */
  public SupplierExtractionEngine() {
    this.operatingSystemSupplier = new OperatingSystemSupplier();
    this.iostatSupplier = new IostatSupplier();
  }

  /**
   * Ping the suppliers and add the results to the buffer.
   *
   * @author Henry Page
   * @since 1.0.0
   */
  public void ping() {
    CompletableFuture<OsInfoDto> osInfoDtoCompletableFuture = this.operatingSystemSupplier.getSnapshot();
    CompletableFuture<IostatDto> iostatDtoCompletableFuture = this.iostatSupplier.getSnapshot();

    CompletableFuture.allOf(osInfoDtoCompletableFuture, iostatDtoCompletableFuture)
        .thenRunAsync(() -> {
          OsInfoDto osInfoDto = osInfoDtoCompletableFuture.join();
          IostatDto iostatDto = iostatDtoCompletableFuture.join();

          BigSupplierDto resourceMetricsRecord = new BigSupplierDto(osInfoDto, iostatDto);
          buffer.add(transform(resourceMetricsRecord));
        });
  }

  /**
   * Transform the resource metrics record.
   * This method can be overridden by extending classes to add additional information.
   *
   * @param resourceMetricsRecord The resource metrics record to transform
   * @return The transformed resource metrics record, with additional information
   */
  public BigSupplierDto transform(BigSupplierDto resourceMetricsRecord) {
    return resourceMetricsRecord;
  }

  /**
   * Get and clear the buffer.
   *
   * @return The buffer contents as a list.
   * @author Henry Page
   * @since 1.0.0
   */
  public List<BigSupplierDto> getAndClear() {
    List<BigSupplierDto> result = new ArrayList<>(buffer);
    buffer.clear();
    return result;
  }
}
