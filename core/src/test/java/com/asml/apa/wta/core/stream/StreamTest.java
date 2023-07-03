package com.asml.apa.wta.core.stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import org.junit.jupiter.api.Test;

/**
 * Fixture for {@link com.asml.apa.wta.core.stream.Stream}.
 */
class StreamTest {

  Stream<Integer> createStreamOfNaturalNumbers(int size) {
    Stream<Integer> stream = new Stream<>();
    for (int i = 1; i <= size; i++) {
      stream.addToStream(i);
    }
    return stream;
  }

  @Test
  void setsUpEmptyStream() {
    Stream<Integer> stream = new Stream<>();
    assertThat(stream.isEmpty()).isTrue();
  }

  @Test
  void setsUpStreamWithOneElement() {
    Stream<Integer> stream = new Stream<>(2);
    assertThat(stream.isEmpty()).isFalse();
    assertThat(stream.head()).isEqualTo(2);
  }

  @Test
  void mapStream() {
    Stream<Integer> stream = createStreamOfNaturalNumbers(10);
    Stream<Integer> mappedStream = stream.map((i) -> {
      if (i < 3) {
        return 0;
      } else {
        return 3;
      }
    });
    assertThat(mappedStream.head()).isEqualTo(0);
    assertThat(mappedStream.head()).isEqualTo(0);
    assertThat(mappedStream.head()).isEqualTo(3);
    assertThat(mappedStream.head()).isEqualTo(3);
  }

  @Test
  void filterStream() {
    Stream<Integer> stream = createStreamOfNaturalNumbers(11);
    Stream<Integer> filteredStream = stream.filter((i) -> i > 9);
    assertThat(filteredStream.head()).isEqualTo(10);
    assertThat(filteredStream.head()).isEqualTo(11);
    assertThat(filteredStream.isEmpty()).isTrue();
  }

  @Test
  void foldStream() {
    Stream<Integer> stream = createStreamOfNaturalNumbers(10);
    int sum = stream.foldLeft(0, Integer::sum);
    assertThat(sum).isEqualTo(55);
  }

  @Test
  void headOfEmptyStream() {
    Stream<Integer> stream = new Stream<>();
    assertThatThrownBy(stream::head).isInstanceOf(NoSuchElementException.class);
  }

  @Test
  void peekAtStream() {
    Stream<Integer> stream = new Stream<>(2);
    assertThat(stream.peek()).isEqualTo(2);
    assertThat(stream.isEmpty()).isFalse();
  }

  @Test
  void peekAtEmptyStream() {
    Stream<Integer> stream = new Stream<>();
    assertThatThrownBy(stream::peek).isInstanceOf(NoSuchElementException.class);
  }

  @Test
  void mapUsingNullOperation() {
    Stream<Integer> stream = createStreamOfNaturalNumbers(1309);
    assertThatThrownBy(() -> stream.map(null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void filterUsingNullOperation() {
    Stream<Integer> stream = createStreamOfNaturalNumbers(102);
    assertThatThrownBy(() -> stream.filter(null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void foldUsingNullOperation() {
    Stream<Integer> stream = createStreamOfNaturalNumbers(457);
    assertThatThrownBy(() -> stream.foldLeft(0, null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void simpleStreamWorkflow() {
    Stream<Integer> stream = createStreamOfNaturalNumbers(10);
    int one = stream.head();
    stream.addToStream(1);
    int two = stream.head();
    stream.addToStream(2);
    int sum = stream.foldLeft(0, Integer::sum);
    assertThat(one).isEqualTo(1);
    assertThat(two).isEqualTo(2);
    assertThat(sum).isEqualTo(55);
  }

  @Test
  void mapConsumesStream() {
    Stream<Integer> stream = createStreamOfNaturalNumbers(10);
    stream.map((i) -> 2);
    assertThat(stream.isEmpty()).isTrue();
  }

  @Test
  void filterConsumesStream() {
    Stream<Integer> stream = createStreamOfNaturalNumbers(10);
    stream.filter((i) -> i % 2 == 0);
    assertThat(stream.isEmpty()).isTrue();
  }

  @Test
  void foldLeftConsumesStream() {
    Stream<Integer> stream = createStreamOfNaturalNumbers(10);
    stream.foldLeft(0, Integer::sum);
    assertThat(stream.isEmpty()).isTrue();
  }

  @Test
  void clonePartiallyConsumedStream() {
    Stream<Integer> stream = createStreamOfNaturalNumbers(10);
    int one = stream.head();
    stream.addToStream(1);
    int two = stream.head();
    stream.addToStream(2);
    int sumClone = stream.copy().foldLeft(0, Integer::sum);
    int sumOriginal = stream.foldLeft(0, Integer::sum);
    assertThat(one).isEqualTo(1);
    assertThat(two).isEqualTo(2);
    assertThat(sumClone).isEqualTo(55);
    assertThat(sumOriginal).isEqualTo(55);
  }

  @Test
  void cloneStreamWithOneElement() {
    Stream<Integer> stream = new Stream<>();
    stream.addToStream(1);
    Stream<Integer> clone = stream.copy();
    assertThat(clone.head()).isEqualTo(1);
    assertThat(stream.head()).isEqualTo(1);
  }

  @Test
  void setUpStreamFromCollection() {
    List<Boolean> list = List.of(true, false, true, false);
    Stream<Boolean> stream = new Stream<>(list);
    assertThat(stream.isEmpty()).isFalse();
    assertThat(stream.head()).isEqualTo(true);
    assertThat(stream.head()).isEqualTo(false);
    assertThat(stream.head()).isEqualTo(true);
    assertThat(stream.head()).isEqualTo(false);
    assertThat(stream.isEmpty()).isTrue();
  }

  @Test
  void setUpStreamFromEmptyCollection() {
    Stream<Long> emptyStream = new Stream<>(List.of());
    assertThat(emptyStream.isEmpty()).isTrue();
  }

  @Test
  void setUpStreamFromNullCollection() {
    Queue<Long> queue = null;
    assertThatThrownBy(() -> new Stream<>(queue)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void findFirstOnEmptyStream() {
    Stream<?> stream = new Stream<>();
    assertThat(stream.isEmpty()).isTrue();
    assertThat(stream.findFirst()).isEmpty();
  }

  @Test
  void findFirstOnStreamWithOneElement() {
    Stream<?> stream = new Stream<>(1);
    assertThat(stream.isEmpty()).isFalse();
    Optional<?> head = stream.findFirst();
    assertThat(head).isPresent();
    assertThat(head.get()).isEqualTo(1);
  }

  @Test
  void findFirstOnStreamWithThreeElements() {
    Stream<?> stream = new Stream<>(List.of(3, 2, 1));
    assertThat(stream.isEmpty()).isFalse();
    Optional<?> head = stream.findFirst();
    assertThat(head).isPresent();
    assertThat(head.get()).isEqualTo(3);
  }

  @Test
  void dropZeroOnEmptyStream() {
    Stream<?> stream = new Stream<>();
    assertThat(stream.isEmpty()).isTrue();
    assertThat(stream.drop(0)).isEqualTo(stream);
    assertThat(stream.isEmpty()).isTrue();
  }

  @Test
  void dropZeroOnStreamWithOneElement() {
    Stream<?> stream = new Stream<>(1);
    assertThat(stream.isEmpty()).isFalse();
    assertThat(stream.drop(0)).isEqualTo(stream);
    assertThat(stream.isEmpty()).isFalse();
    assertThat(stream.head()).isEqualTo(1);
  }

  @Test
  void dropOneOnStreamWithOneElement() {
    Stream<?> stream = new Stream<>(1);
    assertThat(stream.isEmpty()).isFalse();
    assertThat(stream.drop(1)).isEqualTo(stream);
    assertThat(stream.findFirst()).isEmpty();
  }

  @Test
  void dropThreeOnStreamWithOneElement() {
    Stream<?> stream = new Stream<>(1);
    assertThat(stream.isEmpty()).isFalse();
    assertThat(stream.drop(3)).isEqualTo(stream);
    assertThat(stream.findFirst()).isEmpty();
  }

  @Test
  void dropTwoOnStreamWithThreeElements() {
    Stream<?> stream = new Stream<>(List.of(1, 2, 3));
    assertThat(stream.isEmpty()).isFalse();
    assertThat(stream.drop(2)).isEqualTo(stream);
    assertThat(stream.head()).isEqualTo(3);
  }

  @Test
  void reduceEmptyStream() {
    Stream<Integer> stream = new Stream<>();
    assertThat(stream.reduce(Integer::sum)).isEmpty();
    assertThat(stream.isEmpty()).isTrue();
  }

  @Test
  void reduceStreamWithOneElement() {
    Stream<Integer> stream = new Stream<>(3);
    assertThat(stream.isEmpty()).isFalse();
    assertThat(stream.reduce(Integer::sum).orElse(-1)).isEqualTo(3);
    assertThat(stream.isEmpty()).isTrue();
  }

  @Test
  void reduceStreamWithMultipleElements() {
    Stream<Integer> stream = new Stream<>(List.of(1, 2, 3, 4, 5));
    assertThat(stream.isEmpty()).isFalse();
    assertThat(stream.reduce(Integer::sum).orElse(-1)).isEqualTo(15);
    assertThat(stream.isEmpty()).isTrue();
  }

  @Test
  void forEachEmptyStream() {
    Stream<Double> stream = new Stream<>();
    List<Double> doubleList = new ArrayList<>();
    assertThat(stream.isEmpty()).isTrue();
    stream.forEach(doubleList::add);
    assertThat(doubleList).isEmpty();
    assertThat(stream.isEmpty()).isTrue();
  }

  @Test
  void forEachEmptyStreamAddToNonEmptyList() {
    Stream<Double> stream = new Stream<>();
    List<Double> doubleList = new ArrayList<>();
    doubleList.add(1.0);
    assertThat(stream.isEmpty()).isTrue();
    stream.forEach(doubleList::add);
    assertThat(doubleList).isNotEmpty();
    assertThat(doubleList.get(0)).isEqualTo(1.0);
    assertThat(doubleList.size()).isEqualTo(1);
    assertThat(stream.isEmpty()).isTrue();
  }

  @Test
  void forEachStream() {
    Stream<Double> stream = new Stream<>(List.of(1.0, 2.0, 3.0, 4.0));
    List<Double> doubleList = new ArrayList<>();
    assertThat(stream.isEmpty()).isFalse();
    stream.forEach(doubleList::add);
    assertThat(doubleList).isNotEmpty();
    assertThat(doubleList).hasSize(4);
    assertThat(stream.isEmpty()).isTrue();
  }

  @Test
  void countEmptyStream() {
    Stream<Double> stream = new Stream<>();
    assertThat(stream.isEmpty()).isTrue();
    assertThat(stream.count()).isEqualTo(0);
  }

  @Test
  void countStreamWithOneElement() {
    Stream<Integer> stream = new Stream<>(List.of(1));
    assertThat(stream.isEmpty()).isFalse();
    assertThat(stream.count()).isEqualTo(1);
    assertThat(stream.isEmpty()).isTrue();
  }

  @Test
  void countStreamWithFiveElementsWithDuplicates() {
    Stream<Integer> stream = new Stream<>(List.of(1, 2, 2, 3, 1));
    assertThat(stream.isEmpty()).isFalse();
    assertThat(stream.count()).isEqualTo(5);
    assertThat(stream.isEmpty()).isTrue();
  }

  @Test
  void emptyStreamToArray() {
    Stream<Double> stream = new Stream<>();
    assertThat(stream.toArray(Double[]::new)).isEmpty();
  }

  @Test
  void streamToArray() {
    Stream<Character> stream = new Stream<>(List.of('a', 'b'));
    Character[] arr = stream.toArray(Character[]::new);
    assertThat(stream.isEmpty()).isTrue();
    assertThat(arr).isNotEmpty();
    assertThat(arr[0]).isEqualTo('a');
    assertThat(arr[1]).isEqualTo('b');
    assertThat(arr).hasSize(2);
  }

  @Test
  void foldLeftAfterFilter() {
    List<String> list = new ArrayList<>(4);
    list.add(null);
    list.add("Harry ");
    list.add("Pott");
    list.add("er");
    Stream<String> stream = new Stream<>(list);
    String name = stream.filter(Objects::nonNull).foldLeft("", (acc, cur) -> acc + cur);
    assertThat(name).isEqualTo("Harry Potter");
  }

  @Test
  void reduceAfterFilters() {
    Stream<Long> stream = new Stream<>(List.of(-1L, 3L, 8291L, -3189L, 0L));
    long sum = stream.filter(Objects::nonNull)
        .filter(x -> x > 0)
        .reduce(Long::sum)
        .orElse(-1L);
    assertThat(sum).isEqualTo(8294L);
  }

  @Test
  void reduceNullAfterFilters() {
    Stream<Long> stream = new Stream<>(List.of(-1L, 3L, 8291L, -3189L, 0L));
    assertThatThrownBy(() -> stream.filter(Objects::nonNull)
            .filter(x -> x > 0)
            .reduce(null)
            .orElse(-1L))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void mapSandwichedByFilters() {
    List<Long> list = new ArrayList<>(5);
    list.add(null);
    list.add(-13L);
    list.add(4L);
    list.add(81904L);
    list.add(0L);
    Stream<Long> stream = new Stream<>(list);
    Stream<Long> processedStream =
        stream.filter(Objects::nonNull).map(x -> x > 0 ? x : -x).filter(x -> x > 10);
    assertThat(processedStream.count()).isEqualTo(2);
  }

  @Test
  void toListAfterFilter() {
    Stream<Long> stream = new Stream<>(List.of(-1L, 3L, 8291L, -3189L, 0L));
    List<Long> sum = stream.filter(Objects::nonNull).filter(x -> x > 0).toList();
    assertThat(sum).containsExactlyElementsOf(List.of(3L, 8291L));
  }

  @Test
  void addToStreamAfterFilter() {
    Stream<Long> stream = new Stream<>(List.of(-1L, 3L, 8291L, -3189L, 0L));
    stream.addToStream(null);
    stream.addToStream(39L);
    Stream<Long> filteredStream = stream.filter(Objects::nonNull).filter(x -> x > 0);
    filteredStream.addToStream(null);
    long nulls = filteredStream.filter(Objects::isNull).count();
    assertThat(nulls).isEqualTo(1L);
  }

  @Test
  void forEachAfterFilter() {
    Stream<Long> stream = new Stream<>(List.of(-1L, 3L, 8291L, -3189L, 0L));
    stream.addToStream(null);
    stream.addToStream(39L);
    Stream<Long> secondaryStream = new Stream<>();
    stream.filter(Objects::nonNull).filter(x -> x > 0).forEach(secondaryStream::addToStream);
    long nulls = secondaryStream.filter(Objects::isNull).count();
    assertThat(nulls).isEqualTo(0L);
  }

  @Test
  void peekAfterFilter() {
    Stream<Character> stream = new Stream<>(List.of('a', 'b', 'c', 'd', 'e', '.', '.', '.'));
    Stream<Character> filteredStream = stream.filter(x -> x != '.');
    assertThat(filteredStream.peek()).isEqualTo('a');
    assertThat(filteredStream.countFilter(x -> x > 'a')).isEqualTo(4);
  }
}
