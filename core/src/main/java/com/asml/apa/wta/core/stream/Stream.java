package com.asml.apa.wta.core.stream;

import com.asml.apa.wta.core.exception.FailedToDeserializeStreamException;
import com.asml.apa.wta.core.exception.FailedToSerializeStreamException;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * Message stream, used for processing incoming metrics.
 *
 * @param <V> the metrics class to hold, to extend {@link java.io.Serializable}
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
@Slf4j
public class Stream<V extends Serializable> implements Cloneable {

  private class FilteredStream extends Stream<V> {

    private final Predicate<V> predicate;
    private boolean applied;

    private FilteredStream(Predicate<V> filter) {
      super(
          diskLocations,
          additionsSinceLastWriteToDisk,
          serializationTrigger,
          deserializationStart,
          deserializationEnd,
          head,
          tail);
      applied = false;
      predicate = filter;
    }

    @Override
    public synchronized Stream<V> filter(@NonNull Predicate<V> filter) {
      if (applied) {
        return new FilteredStream(filter);
      }
      return new FilteredStream(predicate.and(filter));
    }

    @Override
    public synchronized V head() {
      V elem = super.head();
      while (!predicate.test(elem)) {
        elem = super.head();
      }
      return elem;
    }

    @Override
    public synchronized Optional<V> findFirst() {
      super.forceFilter(predicate);
      return super.findFirst();
    }

    @Override
    public synchronized V peek() {
      V elem = super.peek();
      while (!predicate.test(elem)) {
        super.head();
        elem = super.peek();
      }
      return elem;
    }

    @Override
    public synchronized long count() {
      return super.countFilter(predicate);
    }

    @Override
    public synchronized long countFilter(@NonNull Predicate<V> filter) {
      return super.countFilter(predicate.and(filter));
    }

    @Override
    public synchronized <R extends Serializable> Stream<R> map(@NonNull Function<V, R> mapper) {
      if (!applied) {
        super.forceFilter(predicate);
        applied = true;
      }
      return super.map(mapper);
    }

    @Override
    public synchronized void forEach(Consumer<? super V> consumer) {
      if (!applied) {
        super.forceFilter(predicate);
        applied = true;
      }
      super.forEach(consumer);
    }

    @Override
    public synchronized boolean isEmpty() {
      if (!applied) {
        super.forceFilter(predicate);
        applied = true;
      }
      return super.isEmpty();
    }

    @Override
    public synchronized void addToStream(V elem) {
      if (!applied) {
        super.forceFilter(predicate);
        applied = true;
      }
      super.addToStream(elem);
    }

    @Override
    public synchronized Optional<V> reduce(@NonNull BinaryOperator<V> accumulator) {
      if (!applied) {
        super.forceFilter(predicate);
        applied = true;
      }
      return super.reduce(accumulator);
    }

    @Override
    public synchronized List<V> toList() {
      if (!applied) {
        super.forceFilter(predicate);
        applied = true;
      }
      return super.toList();
    }

    @Override
    public synchronized <R> R foldLeft(R init, @NonNull BiFunction<R, V, R> op) {
      if (!applied) {
        super.forceFilter(predicate);
        applied = true;
      }
      return super.foldLeft(init, op);
    }
  }

  /**
   * Internal node of the {@link Stream}.
   *
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  @Getter
  private static class StreamNode<V> implements Serializable {

    private static final long serialVersionUID = -1846183914651125999L;

    private final V content;

    @Setter
    private transient StreamNode<V> next;

    /**
     * Constructs a node.
     *
     * @param content     content of this {@link StreamNode}
     */
    StreamNode(V content) {
      this.content = content;
    }
  }

  private static final String TEMP_SERIALIZATION_DIRECTORY = "tmp/wta/streams/" +  System.currentTimeMillis() + "/serialization/";

  private UUID id;

  private Queue<String> diskLocations;

  private int additionsSinceLastWriteToDisk;

  private int serializationTrigger;

  private StreamNode<V> deserializationStart;

  private StreamNode<V> deserializationEnd;

  private StreamNode<V> head;

  private StreamNode<V> tail;

  private Stream(
      Queue<String> diskQueue,
      int additionsSinceLastWrite,
      int trigger,
      StreamNode<V> start,
      StreamNode<V> end,
      StreamNode<V> headNode,
      StreamNode<V> tailNode) {
    id = UUID.randomUUID();
    diskLocations = diskQueue;
    additionsSinceLastWriteToDisk = additionsSinceLastWrite;
    serializationTrigger = trigger;
    deserializationStart = start;
    deserializationEnd = end;
    head = headNode;
    tail = tailNode;
  }

  /**
   * Constructs a stream with one element.
   *
   * @param content               element to hold in the {@link Stream}
   * @param serializationTrigger  amount of additions to the {@link Stream} after which serialization is triggered
   */
  public Stream(V content, int serializationTrigger) {
    new File(Stream.TEMP_SERIALIZATION_DIRECTORY).mkdirs();
    head = new StreamNode<>(content);
    tail = head;
    diskLocations = new ConcurrentLinkedDeque<>();
    deserializationStart = head;
    deserializationEnd = head;
    id = UUID.randomUUID();
    additionsSinceLastWriteToDisk = 0;
    this.serializationTrigger = serializationTrigger;
  }

  /**
   * Constructs a stream with one element.
   *
   * @param content       element to hold in the {@link Stream}
   */
  public Stream(V content) {
    this(content, 1800);
  }

  /**
   * Constructs an empty stream.
   */
  public Stream() {
    new File(Stream.TEMP_SERIALIZATION_DIRECTORY).mkdirs();
    deserializationStart = null;
    deserializationEnd = null;
    head = null;
    tail = null;
    diskLocations = new ConcurrentLinkedDeque<>();
    id = UUID.randomUUID();
    additionsSinceLastWriteToDisk = 0;
    serializationTrigger = 1800;
  }

  /**
   * Constructs a {@link Stream} out of a {@link Collection}.
   *
   * @param content     {@link Collection} to construct the {@link Stream} from
   */
  public Stream(@NonNull Collection<V> content) {
    this();
    for (V elem : content) {
      addToStream(elem);
    }
  }

  private void forceFilter(Predicate<V> predicate) {
    log.trace("Consuming and applying filter on Stream {}.", id);
    StreamNode<V> next = head;
    Stream<V> ret = new Stream<>();
    while (next != null) {
      if (next.getNext() == null) {
        deserializationStart = next;
        deserializationEnd = null;
      }
      if (next == deserializationStart && !diskLocations.isEmpty()) {
        head = next;
        deserializeInternals(diskLocations.poll());
      }
      if (predicate.test(next.getContent())) {
        ret.addToStream(next.getContent());
      }
      next = next.getNext();
    }
    head = ret.head;
    tail = ret.tail;
    deserializationStart = ret.deserializationStart;
    deserializationEnd = ret.deserializationEnd;
    diskLocations = ret.diskLocations;
    additionsSinceLastWriteToDisk = ret.additionsSinceLastWriteToDisk;
    serializationTrigger = ret.serializationTrigger;
  }

  /**
   * Serializes the internals of the stream.
   */
  private synchronized void serializeInternals() {
    log.trace("Serializing stream {}.", id);
    StreamNode<V> current;
    current = deserializationEnd;
    String filePath = Stream.TEMP_SERIALIZATION_DIRECTORY + id + "-" + System.currentTimeMillis() + "-"
        + Instant.now().getNano() + ".ser";
    List<StreamNode<V>> toSerialize = new ArrayList<>();
    while (current != tail && current != null) {
      toSerialize.add(current);
      current = current.getNext();
    }
    try (ObjectOutputStream objectOutputStream =
        new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(filePath)))) {
      objectOutputStream.writeObject(toSerialize);
    } catch (IOException e) {
      log.error("Failed to serialize stream internals to {}.", filePath);
      return;
    }
    deserializationStart.setNext(current);
    deserializationEnd = current;
    diskLocations.add(filePath);
    additionsSinceLastWriteToDisk = 0;
  }

  /**
   * Deserializes the internals of the stream on demand.
   *
   * @param filePath        chunk of internals to deserialize, to not be {@code null}
   * @throws FailedToDeserializeStreamException
   *                        if an exception occurred when deserializing this batch of the stream
   */
  private synchronized void deserializeInternals(String filePath) {
    log.trace("Deserializing stream internals from {}.", filePath);
    try (ObjectInputStream objectInputStream =
        new ObjectInputStream(new BufferedInputStream(new FileInputStream(filePath)))) {
      List<StreamNode<V>> nodes = (ArrayList<StreamNode<V>>) objectInputStream.readObject();
      head = deserializationStart;
      StreamNode<V> previous = null;
      for (StreamNode<V> node : nodes) {
        if (previous != null) {
          previous.setNext(node);
        } else {
          deserializationStart.setNext(node);
        }
        previous = node;
      }
      if (previous != null) {
        deserializationStart = previous;
        deserializationStart.setNext(deserializationEnd);
      }
    } catch (IOException | ClassNotFoundException | ClassCastException e) {
      log.error("Failed to deserialize stream internals from {}.", filePath);
      throw new FailedToDeserializeStreamException();
    }
  }

  /**
   * Clones the {@link Stream}. Creates a shallow copy, so it points to the same elements.
   *
   * @return      shallow copy of the current {@link Stream}
   */
  public synchronized Stream<V> copy() {
    try {
      Stream<V> clone = (Stream<V>) super.clone();
      clone.id = UUID.randomUUID();
      clone.diskLocations = new ConcurrentLinkedDeque<>(diskLocations);
      return clone;
    } catch (CloneNotSupportedException e) {
      log.error("Could not clone Stream because {}.", e.getMessage());
      throw new FailedToSerializeStreamException();
    }
  }

  /**
   * Checks whether the stream is empty.
   *
   * @return {@code true} when this {@link com.asml.apa.wta.core.stream.Stream} is empty, {@code false} when it is not
   */
  public boolean isEmpty() {
    log.trace("Checking whether Stream {} is empty.", id);
    return head == null;
  }

  /**
   * Gets an {@link Optional} containing the head of the {@link Stream} if present.
   * If not, it returns an empty {@link Optional}. Consumes the element it returns.
   *
   * @return        head of the {@link Stream} wrapped in an {@link Optional} if present
   */
  public synchronized Optional<V> findFirst() {
    if (head == null) {
      return Optional.empty();
    }
    return Optional.of(head());
  }

  /**
   * Retrieves the head of the stream, which is then removed.
   *
   * @return        head of the {@link Stream}
   * @throws FailedToDeserializeStreamException
   *                when some error occurred during routine deserialization of parts of the {@link Stream}
   * @throws NoSuchElementException
   *                when head is called on an empty {@link Stream}
   */
  public synchronized V head() {
    log.trace("Head of stream {} was requested.", id);
    if (head == null) {
      tail = null;
      deserializationStart = null;
      deserializationEnd = null;
      log.error("`Stream#head()` was called on an empty stream.");
      throw new NoSuchElementException();
    }
    additionsSinceLastWriteToDisk--;
    if (head == deserializationStart) {
      if (diskLocations.isEmpty()) {
        deserializationStart = head.getNext();
      } else {
        deserializeInternals(diskLocations.poll());
      }
    }
    V ret = head.getContent();
    head = head.getNext();
    if (head == null) {
      deserializationStart = null;
      deserializationEnd = null;
      tail = null;
    }
    return ret;
  }

  /**
   * Drops the specified amount of elements from the head of the {@link Stream}.
   * If the specified amount of elements is larger than the size of the {@link Stream},
   * the {@link Stream} will be fully emptied. Consumes the {@link Stream}.
   *
   * @param amount    amount of elements to drop from the {@link Stream}
   * @return          {@link Stream} after the elements were dropped
   */
  public synchronized Stream<V> drop(long amount) {
    for (int i = 0; i < amount; i++) {
      if (head == null) {
        tail = null;
        deserializationStart = null;
        deserializationEnd = null;
        log.error("Stream#drop called for {} elements, but only able to drop {}.", amount, i);
        break;
      }
      additionsSinceLastWriteToDisk--;
      if (head == deserializationStart) {
        if (diskLocations.isEmpty()) {
          deserializationStart = head.getNext();
        } else {
          deserializeInternals(diskLocations.poll());
        }
      }
      head = head.getNext();
    }
    return this;
  }

  /**
   * Peeks at the head of the stream.
   *
   * @return        head of the {@link Stream}
   * @throws NoSuchElementException
   *                when peek is called on an empty {@link Stream}
   */
  public synchronized V peek() {
    log.trace("Peeked at head of stream {}", this.id);
    if (head == null) {
      tail = null;
      deserializationStart = null;
      deserializationEnd = null;
      log.error("`Stream#peek()` was called on an empty stream.");
      throw new NoSuchElementException();
    }
    return head.getContent();
  }

  /**
   * Adds content to the stream.
   *
   * @param content       content to add to this {@link Stream}
   */
  public synchronized void addToStream(V content) {
    if (head == null) {
      head = new StreamNode<>(content);
      tail = head;
      deserializationStart = head;
      deserializationEnd = head;
    } else if (head == tail) {
      tail = new StreamNode<>(content);
      deserializationEnd = tail;
      head.setNext(tail);
    } else {
      tail.setNext(new StreamNode<>(content));
      tail = tail.getNext();
    }
    additionsSinceLastWriteToDisk++;
    if (additionsSinceLastWriteToDisk > serializationTrigger) {
      log.trace(
          "Serializing stream internals after {} additions since last write to disk.",
          additionsSinceLastWriteToDisk);
      serializeInternals();
    }
    log.trace("Added content to stream {}", id);
  }

  /**
   * Returns a stream that maps all elements in this stream using the given function. Consumes the
   * stream.
   *
   * @param op      operation to perform over the {@link Stream}
   * @param <R>     generic return type of the mapping operation
   * @return        mapped stream
   * @throws FailedToDeserializeStreamException
   *                when some error occurred during routine deserialization of parts
   */
  public synchronized <R extends Serializable> Stream<R> map(@NonNull Function<V, R> op) {
    log.trace("Consuming and applying map on stream {}", this.id);
    StreamNode<V> next = head;
    Stream<R> ret = new Stream<>();
    while (next != null) {
      if (next.getNext() == null) {
        deserializationStart = next;
        deserializationEnd = null;
      }
      if (next == deserializationStart && !diskLocations.isEmpty()) {
        head = next;
        deserializeInternals(diskLocations.poll());
      }
      ret.addToStream(op.apply(next.getContent()));
      next = next.getNext();
    }
    head = null;
    tail = null;
    deserializationStart = null;
    deserializationEnd = null;
    return ret;
  }

  /**
   * Returns a stream that filters all elements in this stream using the given predicate. Consumes
   * the stream.
   *
   * @param predicate       predicate used for filtering, elements that return false get filtered out
   * @return                filtered {@link Stream}
   * @throws FailedToDeserializeStreamException
   *                        when some error occurred during routine deserialization of parts
   */
  public synchronized Stream<V> filter(@NonNull Predicate<V> predicate) {
    Stream<V> ret = new FilteredStream(predicate);
    head = null;
    tail = null;
    deserializationStart = null;
    deserializationEnd = null;
    return ret;
  }

  /**
   * Counts the elements that satisfy the given {@link Predicate}.
   *
   * @param predicate       {@link Predicate} for which to run the count
   * @return                amount of elements that satisfy the {@link Predicate}
   */
  public synchronized long countFilter(@NonNull Predicate<V> predicate) {
    log.trace("Consuming and applying filtered count on Stream {}.", id);
    StreamNode<V> next = head;
    long ret = 0;
    while (next != null) {
      if (next.getNext() == null) {
        deserializationStart = next;
        deserializationEnd = null;
      }
      if (next == deserializationStart && !diskLocations.isEmpty()) {
        head = next;
        deserializeInternals(diskLocations.poll());
      }
      if (predicate.test(next.getContent())) {
        ret++;
      }
      next = next.getNext();
    }
    head = null;
    tail = null;
    deserializationStart = null;
    deserializationEnd = null;
    return ret;
  }

  /**
   * Returns a stream that aggregates all elements in this stream using the given function and initial value
   * using a left fold. Consumes the stream.
   *
   * @param init      initial value
   * @param op        fold operation to perform over the {@link Stream}
   * @param <R>       generic return type of the fold operation
   * @return          resulting accumulator
   * @throws FailedToDeserializeStreamException
   *                  when some error occurred during routine deserialization of parts of the {@link Stream}
   */
  public synchronized <R> R foldLeft(R init, @NonNull BiFunction<R, V, R> op) {
    log.trace("Consuming and applying left fold on stream {}", this.id);
    R acc = init;
    StreamNode<V> next = head;
    while (next != null) {
      if (next.getNext() == null) {
        deserializationStart = next;
        deserializationEnd = null;
      }
      if (next == deserializationStart && !diskLocations.isEmpty()) {
        head = next;
        deserializeInternals(diskLocations.poll());
      }
      acc = op.apply(acc, next.getContent());
      next = next.getNext();
    }
    head = null;
    tail = null;
    deserializationStart = null;
    deserializationEnd = null;
    return acc;
  }

  /**
   * Reduces the {@link Stream} with the given accumulator. Consumes the stream.
   *
   * @param accumulator     {@link BinaryOperator} to reduce the stream over
   * @return                result of the reduction, an empty {@link Optional} if the {@link Stream} was empty
   */
  public synchronized Optional<V> reduce(@NonNull BinaryOperator<V> accumulator) {
    if (head == null) {
      return Optional.empty();
    }
    additionsSinceLastWriteToDisk--;
    if (head == deserializationStart) {
      if (diskLocations.isEmpty()) {
        deserializationStart = head.getNext();
      } else {
        deserializeInternals(diskLocations.poll());
      }
    }
    V ret = head.getContent();
    head = head.getNext();
    if (head == null) {
      tail = null;
      deserializationStart = null;
      deserializationEnd = null;
    }
    return Optional.of(foldLeft(ret, accumulator));
  }

  /**
   * Converts the {@link Stream} to a {@link List}, and consumes the {@link Stream}.
   *
   * @return          {@link List} with the {@link Stream}s elements
   */
  public synchronized List<V> toList() {
    log.trace("Consuming stream {} to list", this.id);
    StreamNode<V> next = head;
    List<V> ret = new ArrayList<>();
    while (next != null) {
      if (next.getNext() == null) {
        deserializationStart = next;
        deserializationEnd = null;
      }
      if (next == deserializationStart && !diskLocations.isEmpty()) {
        head = next;
        deserializeInternals(diskLocations.poll());
      }
      ret.add(next.getContent());
      next = next.getNext();
    }
    head = null;
    tail = null;
    deserializationStart = null;
    deserializationEnd = null;
    return ret;
  }

  /**
   * Performs the action for each element in the {@link Stream}. Consumes the {@link Stream}.
   *
   * @param action      action to perform for all elements of the {@link Stream}
   */
  public synchronized void forEach(Consumer<? super V> action) {
    StreamNode<V> next = head;
    while (next != null) {
      if (next.getNext() == null) {
        deserializationStart = next;
        deserializationEnd = null;
      }
      if (next == deserializationStart && !diskLocations.isEmpty()) {
        head = next;
        deserializeInternals(diskLocations.poll());
      }
      action.accept(next.getContent());
      next = next.getNext();
    }
    head = null;
    tail = null;
    deserializationStart = null;
    deserializationEnd = null;
  }

  /**
   * Counts the number of elements in the {@link Stream}. Consumes the {@link Stream}.
   *
   * @return      size of the {@link Stream}
   */
  public synchronized long count() {
    StreamNode<V> next = head;
    long count = 0;
    while (next != null) {
      if (next.getNext() == null) {
        deserializationStart = next;
        deserializationEnd = null;
      }
      if (next == deserializationStart && !diskLocations.isEmpty()) {
        head = next;
        deserializeInternals(diskLocations.poll());
      }
      ++count;
      next = next.getNext();
    }
    head = null;
    tail = null;
    deserializationStart = null;
    deserializationEnd = null;
    return count;
  }

  /**
   * Converts the {@link Stream} to an array, and consumes the {@link Stream}.
   *
   * @param generator   generator of the array
   * @return            array with the {@link Stream}s elements
   */
  public synchronized V[] toArray(IntFunction<V[]> generator) {
    return toList().toArray(generator);
  }

  /**
   * Deletes all the generated stream files.
   */
  public static synchronized void deleteAllSerializedFiles() {
    try {
      Files.walk(Path.of(Stream.TEMP_SERIALIZATION_DIRECTORY))
          .sorted(java.util.Comparator.reverseOrder())
          .map(Path::toFile)
          .forEach(File::delete);
    } catch (IOException e) {
      log.error("Something went wrong while trying to delete the temporarily serialized files");
    }
  }
}
