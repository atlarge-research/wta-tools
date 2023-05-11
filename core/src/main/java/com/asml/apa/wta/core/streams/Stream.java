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
   * Constructs an empty stream.
   *
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public Stream() {
    head = null;
    tail = null;
  }

  /**
   * Constructs a stream with one element.
   *
   * @param record the initial stream element
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public Stream(@NonNull V record) {
    head = record;
    tail = record;
  }

  /**
   * Checks whether the {@link com.asml.apa.wta.core.streams.Stream} is empty.
   *
   * @return a {@code boolean} indicating whether the {@link com.asml.apa.wta.core.streams.Stream} is empty
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public boolean isEmpty() {
    return head == null;
  }

  /**
   * Returns the head of the stream.
   *
   * @return the head of the stream
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public final V head() {
    V ret = head;
    head = head.getNext();
    return ret;
  }

  /**
   * Returns the head of the stream.
   * Guarantees safety by forcing the catch of the exception.
   *
   * @return the head of the stream
   * @throws CannotConsumeEmptyStreamException when an empty stream is being consumed
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public final V safeHead() throws CannotConsumeEmptyStreamException {
    if (head == null) {
      throw new CannotConsumeEmptyStreamException();
    }
    return head();
  }

  /**
   * Adds a record to the stream.
   * As records themselves contain the pointers, it is possible to create circular streams.
   *
   * @param value the value to add to stream, should not be {@code null}
   * @return the stream the value was appended to
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public Stream<V> addToStream(@NonNull V value) {
    tail = tail.setNext(value);
    return this;
  }

  /**
   * Adds a record to a possibly empty stream.
   *
   * @param value the value to add to stream, should not be {@code null}
   * @return the stream the value was appended to
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public Stream<V> safeAddToStream(@NonNull V value) {
    if (tail == null) {
      head = value;
      tail = value;
    } else {
      this.addToStream(value);
    }
    return this;
  }

  /**
   * Maps a stream.
   * Does not maintain a circular stream.
   *
   * @param map the function to map the stream over, should not be {@code null}
   * @param <R> the return type of the function
   * @return the mapped stream
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public <R extends StreamRecord<R>> Stream<R> map(@NonNull Function<V, R> map) {
    Stream<R> stream = new Stream<>(map.apply(head));
    V prev = head;
    head = null;
    V next = prev.getNext();
    prev.setNext(null);
    while (next != null) {
      stream.addToStream(map.apply(next));
      prev = next;
      next = next.getNext();
      prev.setNext(null);
    }
    return stream;
  }

  /**
   * Folds left on the stream.
   * Does not maintain a circular stream.
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
    head = null;
    while (next != null) {
      acc = op.apply(acc, next);
      V prev = next;
      next = next.getNext();
      prev.setNext(null);
    }
    return acc;
  }

  /**
   * Filters the stream.
   * Does not maintain a circular stream.
   *
   * @param predicate the function to filter the stream with, should not be {@code null}
   * @return the filtered stream
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public Stream<V> filter(@NonNull Function<V, Boolean> predicate) {
    Stream<V> stream = new Stream<>();
    V next = head;
    head = null;
    while (next != null) {
      if (predicate.apply(next)) {
        stream.safeAddToStream(next);
      }
      V prev = next;
      next = next.getNext();
      prev.setNext(null);
    }
    return stream;
  }
}
