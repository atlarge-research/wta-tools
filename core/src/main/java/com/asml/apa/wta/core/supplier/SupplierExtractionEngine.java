package com.asml.apa.wta.core.supplier;

import com.asml.apa.wta.core.dto.BaseSupplierDto;
import com.asml.apa.wta.core.dto.DstatDto;
import com.asml.apa.wta.core.dto.IostatDto;
import com.asml.apa.wta.core.dto.JvmFileDto;
import com.asml.apa.wta.core.dto.OsInfoDto;
import com.asml.apa.wta.core.dto.PerfDto;
import com.asml.apa.wta.core.dto.ProcDto;
import com.asml.apa.wta.core.utils.ShellUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Used to extract resources from dependencies. Should be instantiated once per executor/node.
 *
 * @author Henry Page
 * @since 1.0.0
 */
@Slf4j
public abstract class SupplierExtractionEngine<T extends BaseSupplierDto> {

  private final OperatingSystemSupplier operatingSystemSupplier;

  private final IostatSupplier iostatSupplier;

  private final DstatSupplier dstatSupplier;

  private final ProcSupplier procSupplier;

  private final PerfSupplier perfSupplier;

  private final JavaFileSupplier javaFileSupplier;

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
   * @author Lohithsai Yadala Chanchu
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  public SupplierExtractionEngine(int resourcePingInterval) {
    ShellUtils shellUtils = new ShellUtils();
    this.resourcePingInterval = resourcePingInterval;
    this.operatingSystemSupplier = new OperatingSystemSupplier();
    this.javaFileSupplier = new JavaFileSupplier();
    this.iostatSupplier = new IostatSupplier(shellUtils);
    this.dstatSupplier = new DstatSupplier(shellUtils);
    this.procSupplier = new ProcSupplier(shellUtils);
    this.perfSupplier = new PerfSupplier(shellUtils);
  }

  /**
   * Ping the suppliers and add the results to the buffer.
   * Developers are encouraged to override this to add/remove additional information.
   *
   * @return A {@link CompletableFuture} that completes when the result has been resolved
   * @author Henry Page
   * @since 1.0.0
   */
  protected CompletableFuture<T> ping() {
    CompletableFuture<Optional<OsInfoDto>> osInfoDtoCompletableFuture = this.operatingSystemSupplier.getSnapshot();
    CompletableFuture<Optional<IostatDto>> iostatDtoCompletableFuture = this.iostatSupplier.getSnapshot();
    CompletableFuture<Optional<DstatDto>> dstatDtoCompletableFuture = this.dstatSupplier.getSnapshot();
    CompletableFuture<Optional<PerfDto>> perfDtoCompletableFuture = this.perfSupplier.getSnapshot();
    CompletableFuture<Optional<JvmFileDto>> jvmFileDtoCompletableFuture = this.javaFileSupplier.getSnapshot();
    CompletableFuture<Optional<ProcDto>> procDtoCompletableFuture = this.procSupplier.getSnapshot();

    return CompletableFuture.allOf(
            osInfoDtoCompletableFuture,
            iostatDtoCompletableFuture,
            dstatDtoCompletableFuture,
            perfDtoCompletableFuture,
            jvmFileDtoCompletableFuture,
            procDtoCompletableFuture)
        .thenCompose((v) -> {
          long timestamp = System.currentTimeMillis();
          Optional<OsInfoDto> osInfoDto = osInfoDtoCompletableFuture.join();
          Optional<IostatDto> iostatDto = iostatDtoCompletableFuture.join();
          Optional<DstatDto> dstatDto = dstatDtoCompletableFuture.join();
          Optional<PerfDto> perfDto = perfDtoCompletableFuture.join();
          Optional<JvmFileDto> jvmFileDto = jvmFileDtoCompletableFuture.join();
          Optional<ProcDto> procDto = procDtoCompletableFuture.join();
          return CompletableFuture.completedFuture(transform(new BaseSupplierDto(
              timestamp, osInfoDto, iostatDto, dstatDto, perfDto, jvmFileDto, procDto)));
        });
  }

  /**
   * Ping the suppliers and add the results to the buffer. This is done asynchronously.
   *
   * @return A {@link CompletableFuture} representing the result of the ping operation
   */
  public CompletableFuture<Void> pingAndBuffer() {
    log.trace("Pinging suppliers and buffering results.");
    return ping().thenAcceptAsync(buffer::add);
  }

  /**
   * Starts pinging the suppliers at a fixed rate.
   *
   * @author Henry Page
   * @since 1.0.0
   */
  public void startPinging() {
    log.trace("Starting to ping suppliers.");
    resourcePinger.scheduleAtFixedRate(this::pingAndBuffer, 0, resourcePingInterval, TimeUnit.MILLISECONDS);
  }

  /**
   * Stops pinging the suppliers.
   *
   * @author Henry Page
   * @since 1.0.0
   */
  public void stopPinging() {
    log.trace("Stopping to ping suppliers.");
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
    log.trace("Getting and clearing buffer.");
    List<T> result = new ArrayList<>(buffer);
    buffer.clear();
    return result;
  }
}
