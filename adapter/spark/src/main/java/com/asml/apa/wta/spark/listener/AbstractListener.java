package com.asml.apa.wta.spark.listener;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.model.BaseTraceObject;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListener;

/**
 * This class is an abstract listener that can be used to implement listeners for different domain objects.
 *
 * @param <T> The domain object generic
 * @author Henry Page
 * @since 1.0.0
 */
@RequiredArgsConstructor
public class AbstractListener<T extends BaseTraceObject> extends SparkListener {

  /**
   * The current spark context.
   */
  protected final SparkContext sparkContext;

  /**
   * The current runtime config.
   */
  protected final RuntimeConfig config;

  /**
   * A list of processed domain objects.
   */
  @Getter
  protected final List<T> processedObjects = new LinkedList<>();

  /**
   * Filters the list of processed objects by the given condition.
   *
   * @param filterCondition A predicate that filtered objects have to fulfill
   * @return A list containing objects that match that condition
   * @author Henry Page
   * @since 1.0.0
   */
  protected List<T> getWithCondition(Predicate<T> filterCondition) {
    return processedObjects.stream().filter(filterCondition).collect(Collectors.toList());
  }

  /**
   * Registers the listener to the current spark context.
   *
   * @author Henry Page
   * @since 1.0.0
   */
  public void register() {
    sparkContext.addSparkListener(this);
  }

  /**
   * Removes the listener to the current spark context.
   *
   * @author Henry Page
   * @since 1.0.0
   */
  public void remove() {
    sparkContext.removeSparkListener(this);
  }
}
