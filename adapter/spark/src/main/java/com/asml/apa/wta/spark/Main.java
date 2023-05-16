package com.asml.apa.wta.spark;

import org.apache.log4j.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Main {

  public static Logger logger = LogManager.getLogger(Main.class);
  public static void main(String[] args) throws IOException {
    Properties props = new Properties();
    props.load(new FileInputStream("adapter/spark/log4j2.properties"));
    PropertyConfigurator.configure(props);
    logger.info("Hello World");
  }
}
