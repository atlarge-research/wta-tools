package com.asml.apa.wta.spark;

import org.apache.log4j.Logger;

public class Main {

  public static void main(String[] args) {
    Logger logger = PluginLogger.getInstance();
    PluginLogger.loadConfig();
    System.out.println(logger.getAllAppenders().hasMoreElements());
  }
}
