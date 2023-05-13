package com.asml.apa.wta.core.streams;

import java.util.NoSuchElementException;
import java.util.function.BiFunction;
import java.util.function.Function;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

/**
 * Message stream, used for processing incoming metrics.
 *
 * @param <V> the metrics class to hold.
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
public class Stream<V> {

  /**
   * Internal node of the {@link com.asml.apa.wta.core.streams.Stream}.
   *
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  @Getter
  private class StreamNode {

    private final V content;

    @Setter
    private StreamNode next;

    /**
     * Constructs a node.
     *
     * @param content the content of this {@link com.asml.apa.wta.core.streams.Stream.StreamNode}
     * @author Atour Mousavi Gourabi
     * @since 1.0.0
     */
    StreamNode(V content) {
      this.content = content;
    }
  }

  private StreamNode head;
  private StreamNode tail;

  /**
   * Constructs a stream with one element.
   *
   * @param content the element to hold in the {@link com.asml.apa.wta.core.streams.Stream}
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public Stream(V content) {
    head = new StreamNode(content);
    tail = head;
  }

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
   * Checks whether the stream is empty.
   *
   * @return {@code true} when this {@link com.asml.apa.wta.core.streams.Stream} is empty, {@code false} when it is not
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public boolean isEmpty() {
    return head == null;
  }

  /**
   * Retrieves the head of the stream.
   *
   * @return the head of the {@link com.asml.apa.wta.core.streams.Stream}
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public synchronized V head() {
    if (head == null) {
      throw new NoSuchElementException();
    }
    V ret = head.getContent();
    head = head.getNext();
    if (head == null) {
      tail = null;
    }
    return ret;
  }

  /**
   * Adds content to the stream.
   *
   * @param content the content to add to this {@link com.asml.apa.wta.core.streams.Stream}
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public synchronized void addToStream(V content) {
    if (head == null) {
      head = new StreamNode(content);
      tail = head;
    } else {
      tail.setNext(new StreamNode(content));
      tail = tail.getNext();
    }
  }

  /**
   * Maps the stream.
   *
   * @param op the operation to perform over the {@link com.asml.apa.wta.core.streams.Stream}
   * @param <R> generic return type of the mapping operation
   * @return the mapped stream
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public synchronized <R> Stream<R> map(@NonNull Function<V, R> op) {
    StreamNode next = head;
    Stream<R> ret = new Stream<>();
    while (next != null) {
      ret.addToStream(op.apply(next.getContent()));
      next = next.getNext();
    }
    return ret;
  }

  /**
   * Filters the stream.
   *
   * @param predicate the predicate used for filtering, elements that return false get filtered out
   * @return the filtered {@link com.asml.apa.wta.core.streams.Stream}
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public synchronized Stream<V> filter(@NonNull Function<V, Boolean> predicate) {
    StreamNode next = head;
    Stream<V> ret = new Stream<>();
    while (next != null) {
      if (predicate.apply(next.getContent())) {
        ret.addToStream(next.getContent());
      }
      next = next.getNext();
    }
    return ret;
  }

  /**
   * Fold the stream.
   *
   * @param init the initial value
   * @param op the fold operation to perform over the {@link com.asml.apa.wta.core.streams.Stream}
   * @param <R> generic return type of the fold operation
   * @return the resulting accumulator
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public synchronized <R> R foldLeft(R init, @NonNull BiFunction<R, V, R> op) {
    R acc = init;
    StreamNode next = head;
    while (next != null) {
      acc = op.apply(acc, next.getContent());
      next = next.getNext();
    }
    return acc;
  }
}
