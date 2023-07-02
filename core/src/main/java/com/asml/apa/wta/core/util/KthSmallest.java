package com.asml.apa.wta.core.util;

import com.asml.apa.wta.core.stream.Stream;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class to find the kth smallest number in a {@link Stream} of {@code doubles}.
 *
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
public class KthSmallest {

  /**
   * Finds the median of a {@link List} of at most five {@link Double}s.
   *
   * @param fiveDoubles     {@link List} of at most five doubles.
   * @return                median of the {@link List}.
   */
  private double findMedian(List<Double> fiveDoubles) {
    if (fiveDoubles.size() > 5 || fiveDoubles.size() < 1) {
      throw new IllegalArgumentException();
    }
    fiveDoubles.sort(Double::compare);
    if (fiveDoubles.size() % 2 == 0) {
      return (fiveDoubles.get(fiveDoubles.size() / 2) + fiveDoubles.get(fiveDoubles.size() / 2 - 1)) / 2;
    } else {
      return fiveDoubles.get(fiveDoubles.size() / 2);
    }
  }

  /**
   * Gets the median of medians, recursively.
   * Provides a good starting pivot.
   *
   * @param data            {@link Stream} to compute the median of medians over.
   * @return                median of medians.
   */
  private double medianOfMedians(Stream<Double> data) {
    Stream<Double> stream = data;
    while (!stream.isEmpty()) {
      Stream<Double> recursive = new Stream<>();
      while (!stream.isEmpty()) {
        List<Double> listOfFive = new ArrayList<>();
        for (int i = 0; i < 5 && !stream.isEmpty(); i++) {
          listOfFive.add(stream.head());
        }
        recursive.addToStream(findMedian(listOfFive));
      }
      if (recursive.copy().count() == 1) {
        return recursive.head();
      } else {
        stream = recursive;
      }
    }
    return 0.0;
  }

  /**
   * Finds the kth smallest in the {@link Stream}.
   *
   * @param data          {@link Stream} to query.
   * @param kthSmallest   amount of numbers smaller than the one we want to fetch.
   * @return              kth smallest number in the {@link Stream}.
   */
  public double find(Stream<Double> data, long kthSmallest) {
    Stream<Double> stream = data;
    long kth = kthSmallest;
    while (!stream.isEmpty()) {
      double medianOfMedians = medianOfMedians(stream.copy());
      Stream<Double> smaller = stream.copy().filter(x -> x < medianOfMedians);
      Stream<Double> larger = stream.copy().filter(x -> x > medianOfMedians);
      long equalSize = stream.copy().countFilter(x -> x == medianOfMedians);
      long smallerSize = stream.copy().countFilter(x -> x < medianOfMedians);
      if (kth < smallerSize) {
        stream = smaller;
      } else if (kth < equalSize + smallerSize) {
        return medianOfMedians;
      } else {
        stream = larger;
        kth -= smallerSize + equalSize;
      }
    }
    return -1.0;
  }
}
