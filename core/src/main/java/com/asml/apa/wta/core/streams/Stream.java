package com.asml.apa.wta.core.streams;

import java.util.function.BiFunction;
import java.util.function.Function;
import lombok.NonNull;

/**
 * Stream.
 *
 * @param <V> the record stored.
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
public class Stream<V extends StreamRecord<V>> {
  private V head;
  private V tail;

  /**
   * Constructs a stream with one element.
   *
   * @param record the initial stream element
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public Stream(V record) {
    head = record;
    tail = record;
  }

  /**
   * Adds a record to the stream.
   *
   * @param value the value to add to stream, should not be {@code null}
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public void addToStream(@NonNull V value) {
    tail = tail.setNext(value);
  }

  /**
   * Maps a stream.
   *
   * @param map the function to map the stream over, should not be {@code null}
   * @param <R> the return type of the function
   * @return the mapped stream
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public <R extends StreamRecord<R>> Stream<R> map(@NonNull Function<V, R> map) {
    Stream<R> stream = new Stream<>(map.apply(head));
    V next = head.getNext();
    while (next != null) {
      stream.addToStream(map.apply(next));
      next = next.getNext();
    }
    return stream;
  }

  /**
   * Folds left on the stream.
   *
   * @param init the initial value of the fold
   * @param op the fold operation
   * @param <R> the return type of the fold
   * @return the fold result
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public <R> R foldLeft(R init, @NonNull BiFunction<R, V, R> op) {
    R acc = init;
    V next = head;
    while (next != null) {
      acc = op.apply(acc, next);
      next = next.getNext();
    }
    return acc;
  }

  /**
   * Filters the stream.
   *
   * @param predicate the function to filter the stream with, should not be {@code null}
   * @return the filtered stream
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public Stream<V> filter(@NonNull Function<V, Boolean> predicate) {
    V next = head;
    while (!predicate.apply(next)) {
      next = next.getNext();
    }
    Stream<V> stream = new Stream<>(next);
    next = next.getNext();
    while (next != null) {
      if (predicate.apply(next)) {
        stream.addToStream(next);
      }
      next = next.getNext();
    }
    return stream;
  }
}
