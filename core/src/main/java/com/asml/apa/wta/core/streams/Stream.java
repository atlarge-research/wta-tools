package com.asml.apa.wta.core.streams;

import com.asml.apa.wta.core.exceptions.FailedToDeserializeStreamException;
import com.asml.apa.wta.core.exceptions.FailedToSerializeStreamException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
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
public class Stream<V extends Serializable> implements Serializable, Cloneable {

  private static final long serialVersionUID = 7414918352615022254L;

  /**
   * Internal node of the {@link com.asml.apa.wta.core.streams.Stream}.
   *
   * @param <V> the metrics class to hold, to extend {@link java.io.Serializable}
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  @Getter
  private static class StreamNode<V extends Serializable> implements Serializable {

    private static final long serialVersionUID = -1846183914651125999L;

    private final V content;

    @Setter
    private transient StreamNode<V> next;

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

  private static final String TEMP_SERIALIZATION_DIRECTORY = "tmp/wta/streams/serialization/";

  private UUID id;

  private final Queue<String> diskLocations;
  private int additionsSinceLastWriteToDisk;
  private int serializationTrigger;

  private StreamNode<V> deserializationStart;
  private StreamNode<V> deserializationEnd;

  private StreamNode<V> head;
  private StreamNode<V> tail;

  /**
   * Constructs a stream with one element.
   *
   * @param content the element to hold in the {@link com.asml.apa.wta.core.streams.Stream}
   * @param serializationTrigger the amount of additions to the {@link com.asml.apa.wta.core.streams.Stream} after
   *                             which serialization is triggered.
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public Stream(V content, int serializationTrigger) {
    new File(Stream.TEMP_SERIALIZATION_DIRECTORY).mkdirs();
    head = new StreamNode<>(content);
    tail = head;
    diskLocations = new ArrayDeque<>();
    deserializationStart = head;
    deserializationEnd = head;
    id = UUID.randomUUID();
    additionsSinceLastWriteToDisk = 0;
    this.serializationTrigger = serializationTrigger;
  }

  /**
   * Constructs a stream with one element.
   *
   * @param content the element to hold in the {@link com.asml.apa.wta.core.streams.Stream}
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public Stream(V content) {
    this(content, 1800);
  }

  /**
   * Constructs an empty stream.
   *
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public Stream() {
    new File(Stream.TEMP_SERIALIZATION_DIRECTORY).mkdirs();
    deserializationStart = null;
    deserializationEnd = null;
    head = null;
    tail = null;
    diskLocations = new ArrayDeque<>();
    id = UUID.randomUUID();
    additionsSinceLastWriteToDisk = 0;
    serializationTrigger = 1800;
  }

  /**
   * Constructs a {@link Stream} out of a {@link Collection}.
   *
   * @param content the {@link Collection} to construct the {@link Stream} from
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public Stream(@NonNull Collection<V> content) {
    this();
    for (V elem : content) {
      addToStream(elem);
    }
  }

  /**
   * Serializes the internals of the stream.
   *
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  private synchronized void serializeInternals() {
    StreamNode<V> current;
    if (head == deserializationEnd) {
      current = head.getNext();
    } else {
      current = deserializationEnd;
    }
    String filePath = Stream.TEMP_SERIALIZATION_DIRECTORY + id + "-" + System.currentTimeMillis() + "-"
        + Instant.now().getNano() + ".ser";
    List<StreamNode<V>> toSerialize = new ArrayList<>();
    while (current != tail && current != null) {
      toSerialize.add(current);
      current = current.getNext();
    }
    try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(new FileOutputStream(filePath))) {
      objectOutputStream.writeObject(toSerialize);
    } catch (IOException e) {
      log.error("Failed to serialize stream internals to {}", filePath);
      return;
    }
    deserializationEnd.setNext(null);
    deserializationEnd = tail;
    diskLocations.add(filePath);
    additionsSinceLastWriteToDisk = 0;
  }

  /**
   * Deserializes the internals of the stream on demand.
   *
   * @param filePath the chunk of internals to deserialize, to not be {@code null}
   * @return the amount of {@link com.asml.apa.wta.core.streams.Stream.StreamNode} objects deserialized
   * @throws FailedToDeserializeStreamException if an exception occurred when deserializing this batch of the stream
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  private synchronized int deserializeInternals(@NonNull String filePath) {
    int amountOfNodes;
    try (ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(filePath))) {
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
      amountOfNodes = nodes.size();
    } catch (IOException | ClassNotFoundException | ClassCastException e) {
      log.error("Failed to deserialize stream internals from {}", filePath);
      throw new FailedToDeserializeStreamException();
    } finally {
      new File(filePath).delete();
    }
    return amountOfNodes;
  }

  /**
   * Clones the {@link Stream}. Creates a shallow copy, so it points to the same elements.
   *
   * @return a shallow copy of the current {@link Stream}
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public synchronized Stream<V> copy() {
    try {
      Stream<V> clone = (Stream<V>) super.clone();
      clone.id = UUID.randomUUID();
      for (String diskLocation : diskLocations) {
        String newDiskLocation = diskLocation.substring(0, diskLocation.length() - 4) + "_clone.ser";
        Files.copy(Path.of(diskLocation), Path.of(newDiskLocation), StandardCopyOption.REPLACE_EXISTING);
        clone.diskLocations.offer(newDiskLocation);
      }
      return clone;
    } catch (IOException e) {
      log.error("Could not serialize the clone because {}.", e.getMessage());
      throw new FailedToSerializeStreamException();
    } catch (CloneNotSupportedException e) {
      log.error("Could not clone Stream because {}.", e.getMessage());
      throw new FailedToSerializeStreamException();
    }
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
   * Gets an {@link Optional} containing the head of the {@link Stream} if present.
   * If not, it returns an empty {@link Optional}. Consumes the element it returns.
   *
   * @return the head of the {@link Stream} wrapped in an {@link Optional} if present
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
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
   * @return the head of the {@link com.asml.apa.wta.core.streams.Stream}
   * @throws FailedToDeserializeStreamException when some error occurred during routine deserialization of parts of
   *                                            the {@link com.asml.apa.wta.core.streams.Stream}
   * @throws NoSuchElementException when head is called on an empty {@link com.asml.apa.wta.core.streams.Stream}
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public synchronized V head() {
    if (head == null) {
      log.error("Stream#head() was called on an empty stream");
      throw new NoSuchElementException();
    }
    additionsSinceLastWriteToDisk--;
    if (head == deserializationStart) {
      if (diskLocations.isEmpty()) {
        deserializationStart = head.getNext();
      } else {
        additionsSinceLastWriteToDisk += deserializeInternals(diskLocations.poll());
      }
    }
    V ret = head.getContent();
    head = head.getNext();
    if (head == null) {
      tail = null;
    }
    return ret;
  }

  /**
   * Drops the specified amount of elements from the head of the {@link Stream}.
   * If the specified amount of elements is larger than the size of the {@link Stream},
   * the {@link Stream} will be fully emptied. Consumes the {@link Stream}.
   *
   * @param amount the amount of elements to drop from the {@link Stream}
   * @return the {@link Stream} after the elements were dropped
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public synchronized Stream<V> drop(long amount) {
    for (int i = 0; i < amount; i++) {
      if (head == null) {
        tail = null;
        log.error("Stream#drop called for {} elements, but only able to drop {}.", amount, i);
        break;
      }
      additionsSinceLastWriteToDisk--;
      if (head == deserializationStart) {
        if (diskLocations.isEmpty()) {
          deserializationStart = head.getNext();
        } else {
          additionsSinceLastWriteToDisk += deserializeInternals(diskLocations.poll());
        }
      }
      head = head.getNext();
    }
    return this;
  }

  /**
   * Peeks at the head of the stream.
   *
   * @return the head of the {@link com.asml.apa.wta.core.streams.Stream}
   * @throws NoSuchElementException when peek is called on an empty {@link com.asml.apa.wta.core.streams.Stream}
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public synchronized V peek() {
    if (head == null) {
      log.error("Stream#peek() was called on an empty stream");
      throw new NoSuchElementException();
    }
    return head.getContent();
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
          "Serializing stream internals after {} additions since last write to disk",
          additionsSinceLastWriteToDisk);
      serializeInternals();
    }
  }

  /**
   * Returns a stream that maps all elements in this stream using the given function. Consumes the
   * stream.
   *
   * @param op the operation to perform over the {@link com.asml.apa.wta.core.streams.Stream}
   * @param <R> generic return type of the mapping operation
   * @return the mapped stream
   * @throws FailedToDeserializeStreamException when some error occurred during routine deserialization of parts
   *                                            of the {@link com.asml.apa.wta.core.streams.Stream}
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public synchronized <R extends Serializable> Stream<R> map(@NonNull Function<V, R> op) {
    StreamNode<V> next = head;
    Stream<R> ret = new Stream<>();
    while (next != null) {
      if (next == deserializationStart && !diskLocations.isEmpty()) {
        head = next;
        deserializeInternals(diskLocations.poll());
      }
      ret.addToStream(op.apply(next.getContent()));
      next = next.getNext();
    }
    head = null;
    tail = null;
    return ret;
  }

  /**
   * Returns a stream that filters all elements in this stream using the given predicate. Consumes
   * the stream.
   *
   * @param predicate the predicate used for filtering, elements that return false get filtered out
   * @return the filtered {@link com.asml.apa.wta.core.streams.Stream}
   * @throws FailedToDeserializeStreamException when some error occurred during routine deserialization of parts
   *                                            of the {@link com.asml.apa.wta.core.streams.Stream}
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public synchronized Stream<V> filter(@NonNull Predicate<V> predicate) {
    StreamNode<V> next = head;
    Stream<V> ret = new Stream<>();
    while (next != null) {
      if (next == deserializationStart && !diskLocations.isEmpty()) {
        head = next;
        deserializeInternals(diskLocations.poll());
      }
      if (predicate.test(next.getContent())) {
        ret.addToStream(next.getContent());
      }
      next = next.getNext();
    }
    head = null;
    tail = null;
    return ret;
  }

  /**
   * Returns a stream that aggregates all elements in this stream using the given function and initial value
   * using a left fold. Consumes the stream.
   *
   * @param init the initial value
   * @param op the fold operation to perform over the {@link com.asml.apa.wta.core.streams.Stream}
   * @param <R> generic return type of the fold operation
   * @return the resulting accumulator
   * @throws FailedToDeserializeStreamException when some error occurred during routine deserialization of parts of
   *                                            the {@link com.asml.apa.wta.core.streams.Stream}
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public synchronized <R> R foldLeft(R init, @NonNull BiFunction<R, V, R> op) {
    R acc = init;
    StreamNode<V> next = head;
    while (next != null) {
      if (next == deserializationStart && !diskLocations.isEmpty()) {
        head = next;
        deserializeInternals(diskLocations.poll());
      }
      acc = op.apply(acc, next.getContent());
      next = next.getNext();
    }
    head = null;
    tail = null;
    return acc;
  }

  /**
   * Reduces the {@link Stream} with the given accumulator. Consumes the stream.
   *
   * @param accumulator the {@link BinaryOperator} to reduce the stream over
   * @return the result of the reduction, an empty {@link Optional} if the {@link Stream} was empty
   * @author Atour Mousavi Gourabi
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
        additionsSinceLastWriteToDisk += deserializeInternals(diskLocations.poll());
      }
    }
    V ret = head.getContent();
    head = head.getNext();
    if (head == null) {
      tail = null;
    }
    return Optional.of(foldLeft(ret, accumulator));
  }

  /**
   * Converts the {@link Stream} to a {@link List}, and consumes the {@link Stream}.
   *
   * @return a {@link List} with the {@link Stream}s elements
   * @author Atour Mousavi Gourabi
   * @author Henry Page
   * @since 1.0.0
   */
  public synchronized List<V> toList() {
    StreamNode<V> next = head;
    List<V> ret = new ArrayList<>();
    while (next != null) {
      if (next == deserializationStart && !diskLocations.isEmpty()) {
        head = next;
        deserializeInternals(diskLocations.poll());
      }
      ret.add(next.getContent());
      next = next.getNext();
    }
    head = null;
    tail = null;
    return ret;
  }

  /**
   * Performs the action for each element in the {@link Stream}. Consumes the {@link Stream}.
   *
   * @param action the action to perform for all elements of the {@link Stream}
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public synchronized void forEach(Consumer<? super V> action) {
    StreamNode<V> next = head;
    while (next != null) {
      if (next == deserializationStart && !diskLocations.isEmpty()) {
        head = next;
        deserializeInternals(diskLocations.poll());
      }
      action.accept(next.getContent());
      next = next.getNext();
    }
    head = null;
    tail = null;
  }

  /**
   * Counts the number of elements in the {@link Stream}. Consumes the {@link Stream}.
   *
   * @return the size of the {@link Stream}
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public synchronized long count() {
    StreamNode<V> next = head;
    long count = 0;
    while (next != null) {
      if (next == deserializationStart && !diskLocations.isEmpty()) {
        head = next;
        deserializeInternals(diskLocations.poll());
      }
      ++count;
      next = next.getNext();
    }
    head = null;
    tail = null;
    return count;
  }

  /**
   * Converts the {@link Stream} to an array, and consumes the {@link Stream}.
   *
   * @param generator the generator of the array
   * @return an array with the {@link Stream}s elements
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public synchronized V[] toArray(IntFunction<V[]> generator) {
    return toList().toArray(generator);
  }

  /**
   * Deletes all the generated stream files.
   *
   * @author Lohithsai Yadala Chanchu
   * @since 1.0.0
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
