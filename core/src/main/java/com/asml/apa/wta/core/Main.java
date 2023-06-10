package com.asml.apa.wta.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Main {

  public static void main(String[] args) {
    String input =
        "Device             tps    kB_read/s    kB_wrtn/s    kB_dscd/s    kB_read    kB_wrtn    kB_dscd\n"
            + "sda               0.01         0.54         0.00         0.00      70941          0          0\n"
            + "sdb               0.00         0.02         0.14         0.00       2544      18040          0\n"
            + "sdc               3.20        20.62        65.88        19.98    2721325    8693408    2636916";

    List<List<String>> parsedList = new ArrayList<>();
    String[] lines = input.split("\n");

    // Parse the input and store it in a 2D list
    for (String line : lines) {
      List<String> row = Arrays.asList(line.trim().split("\\s+"));
      parsedList.add(row);
    }

    List<Double> columnSums = new ArrayList<>();
    int numColumns = parsedList.get(0).size();

    // Initialize column sums
    for (int columnIndex = 0; columnIndex < numColumns; columnIndex++) {
      columnSums.add(0.0);
    }

    // Calculate column sums
    for (int rowIndex = 1; rowIndex < parsedList.size(); rowIndex++) {
      List<String> row = parsedList.get(rowIndex);
      for (int columnIndex = 1; columnIndex < numColumns; columnIndex++) {
        double value = Double.parseDouble(row.get(columnIndex));
        columnSums.set(columnIndex, columnSums.get(columnIndex) + value);
      }
    }

    System.out.println(columnSums);
  }
}
