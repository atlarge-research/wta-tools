package com.asml.apa.wta.core.supplier;

import com.asml.apa.wta.core.dto.BaseSupplierDto;
import com.asml.apa.wta.core.dto.IostatDto;
import com.asml.apa.wta.core.dto.OsInfoDto;
import com.asml.apa.wta.core.dto.PerfDto;
import com.asml.apa.wta.core.utils.BashUtils;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.Getter;

/**
 * Used to extract resources from dependencies. Should be instantiated once per executor/node.
 *
 * @author Henry Page
 * @since 1.0.0
 */
public abstract class SupplierExtractionEngine<T extends BaseSupplierDto> {

  private final OperatingSystemSupplier operatingSystemSupplier;

  private final IostatSupplier iostatSupplier;

  private final PerfSupplier perfSupplier;

  private final int resourcePingInterval;

  @Getter
  private final Collection<T> buffer = new ArrayList<>();

  private final ScheduledExecutorService resourcePinger = Executors.newScheduledThreadPool(1);

  /**
   * Constructor for the resource extraction engine.
   * Suppliers should be injected here.
   *
   * @param resourcePingInterval How often to ping the suppliers, in milliseconds
   * @author Henry Page
   * @since 1.0.0
   */
  public SupplierExtractionEngine(int resourcePingInterval) {
    BashUtils bashUtils = new BashUtils();
    this.resourcePingInterval = resourcePingInterval;
    this.operatingSystemSupplier = new OperatingSystemSupplier();
    this.iostatSupplier = new IostatSupplier(bashUtils);
    this.perfSupplier = new PerfSupplier(bashUtils);
  }

  /**
   * Ping the suppliers and add the results to the buffer.
   * Developers are encouraged to override this to add/remove additional information.
   *
   * @return A {@link CompletableFuture} that completes when the result has been resolved
   * @author Henry Page
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  protected CompletableFuture<T> ping() {
    CompletableFuture<OsInfoDto> osInfoDtoCompletableFuture = this.operatingSystemSupplier.getSnapshot();
    CompletableFuture<IostatDto> iostatDtoCompletableFuture = this.iostatSupplier.getSnapshot();
    CompletableFuture<PerfDto> perfDtoCompletableFuture = this.perfSupplier.getSnapshot();

    return CompletableFuture.allOf(osInfoDtoCompletableFuture, iostatDtoCompletableFuture)
        .thenCompose((v) -> {
          LocalDateTime timestamp = LocalDateTime.now();
          OsInfoDto osInfoDto = osInfoDtoCompletableFuture.join();
          IostatDto iostatDto = iostatDtoCompletableFuture.join();
          PerfDto perfDto = perfDtoCompletableFuture.join();
          return CompletableFuture.completedFuture(
              transform(new BaseSupplierDto(timestamp, osInfoDto, iostatDto, perfDto)));
        });
  }

  /**
   * Ping the suppliers and add the results to the buffer. This is done asynchronously.
   *
   * @return A {@link CompletableFuture} representing the result of the ping operation
   */
  public CompletableFuture<Void> pingAndBuffer() {
    return ping().thenAcceptAsync(buffer::add);
  }

  /**
   * Starts pinging the suppliers at a fixed rate.
   *
   * @author Henry Page
   * @since 1.0.0
   */
  public void startPinging() {
    resourcePinger.scheduleAtFixedRate(this::pingAndBuffer, 0, resourcePingInterval, TimeUnit.MILLISECONDS);
  }

  /**
   * Stops pinging the suppliers.
   *
   * @author Henry Page
   * @since 1.0.0
   */
  public void stopPinging() {
    resourcePinger.shutdown();
  }

  /**
   * Transform the resource metrics record.
   * This method can be overridden by extending classes to add additional information.
   *
   * @param record The 'bare-bones' information that needs to be augmented
   * @return The transformed resource metrics record, with additional information
   * @author Henry Page
   * @since 1.0.0
   */
  public abstract T transform(BaseSupplierDto record);

  /**
   * Get and clear the buffer.
   *
   * @return The buffer contents as a list
   * @author Henry Page
   * @since 1.0.0
   */
  public List<T> getAndClear() {
    List<T> result = new ArrayList<>(buffer);
    buffer.clear();
    return result;
  }
}
