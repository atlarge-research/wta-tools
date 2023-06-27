package com.asml.apa.wta.core.utils;

import com.asml.apa.wta.core.streams.Stream;
import java.util.ArrayList;
import java.util.List;

public class KthLargest {

  /**
   * Finds the median of a {@link List} of at most five {@link Double}s.
   *
   * @param fiveDoubles the {@link List} of at most five doubles
   * @return the median of the {@link List}
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
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
   * @param data the {@link Stream} to compute the median of medians over
   * @return the median of medians
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  private double medianOfMedians(Stream<Double> data) {
    while (true) {
      Stream<Double> recursive = new Stream<>();
      long amount = 0;
      while (!data.isEmpty()) {
        List<Double> listOfFive = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
          if (!data.isEmpty()) {
            listOfFive.add(data.head());
          }
        }
        amount++;
        recursive.addToStream(findMedian(listOfFive));
      }
      if (amount == 1) {
        return recursive.head();
      } else {
        data = recursive;
      }
    }
  }

  /**
   * Finds the kth smallest in the {@link Stream}.
   *
   * @param data the {@link Stream} to query
   * @param kthSmallest the amount of numbers smaller than the one we want to fetch
   * @return the kth smallest number in the {@link Stream}
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public double findKthSmallest(Stream<Double> data, long kthSmallest) {
    while (true) {
      double medianOfMedians = medianOfMedians(data.copy());
      Stream<Double> smaller = data.copy().filter(x -> x < medianOfMedians);
      Stream<Double> larger = data.copy().filter(x -> x > medianOfMedians);
      long equalSize = data.copy().filter(x -> x == medianOfMedians).count();
      long smallerSize = smaller.copy().count();
      if (kthSmallest < smallerSize) {
        data = smaller;
      } else if (kthSmallest < equalSize + smallerSize) {
        return medianOfMedians;
      } else {
        data = larger;
        kthSmallest -= smallerSize + equalSize;
      }
    }
  }
}
