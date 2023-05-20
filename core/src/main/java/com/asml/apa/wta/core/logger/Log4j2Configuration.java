package com.asml.apa.wta.core.logger;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.utils.WtaUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;

public class Log4j2Configuration {

  public static void setUpLoggingConfig() {
    RuntimeConfig config = WtaUtils.readConfig();
    Level logLevel;
    switch (config.getLogLevel()) {
      case "TRACE":
        logLevel = Level.TRACE;
        break;
      case "OFF":
        logLevel = Level.OFF;
        break;
      case "ALL":
        logLevel = Level.ALL;
        break;
      case "DEBUG":
        logLevel = Level.DEBUG;
        break;
      case "FATAL":
        logLevel = Level.FATAL;
        break;
      case "WARN":
        logLevel = Level.WARN;
        break;
      case "INFO":
        logLevel = Level.INFO;
        break;
      case "ERROR":
      default:
        logLevel = Level.ERROR;
        break;
    }
    Configurator.setRootLevel(logLevel);
  }
}
