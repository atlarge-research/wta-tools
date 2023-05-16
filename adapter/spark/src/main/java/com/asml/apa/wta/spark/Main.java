package com.asml.apa.wta.spark;

import java.io.IOException;
import org.slf4j.Logger;

public class Main {

  public static void main(String[] args) throws IOException {
    Logger logger = PluginLogger.getInstance();
    PluginLogger.loadConfig("adapter/spark/log4j2.properties");
    logger.info("Hello World!");
  }
}
