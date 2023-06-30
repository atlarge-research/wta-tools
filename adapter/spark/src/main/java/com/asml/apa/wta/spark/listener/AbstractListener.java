package com.asml.apa.wta.spark.listener;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.model.BaseTraceObject;
import com.asml.apa.wta.core.stream.Stream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListener;

/**
 * This class is an abstract listener that can be used to implement listeners for different domain objects.
 *
 * @param <T> The domain object generic
 * @author Henry Page
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
@RequiredArgsConstructor
@SuppressWarnings("VisibilityModifier")
public abstract class AbstractListener<T extends BaseTraceObject> extends SparkListener {

  /**
   * The current spark context.
   */
  @Getter
  private final SparkContext sparkContext;

  /**
   * The current runtime config.
   */
  @Getter
  private final RuntimeConfig config;

  /**
   * A list of processed domain objects.
   */
  private final Stream<T> processedObjects = new Stream<>();

  /**
   * The thread pool.
   */
  @Getter
  private static final ExecutorService threadPool = Executors.newSingleThreadExecutor();

  /**
   * Returns a clone of the processed objects {@link Stream}.
   *
   * @return a clone of the processed objects
   */
  public Stream<T> getProcessedObjects() {
    return processedObjects.copy();
  }

  /**
   * Adds a processed object to the {@link Stream} maintained by the listener.
   *
   * @param object the processed object to add
   */
  public void addProcessedObject(T object) {
    processedObjects.addToStream(object);
  }

  /**
   * Registers the listener to the current spark context.
   */
  public void register() {
    sparkContext.addSparkListener(this);
  }

  /**
   * Removes the listener to the current spark context.
   */
  public void remove() {
    sparkContext.removeSparkListener(this);
  }
}
