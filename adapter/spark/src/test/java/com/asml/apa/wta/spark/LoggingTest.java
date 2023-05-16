package com.asml.apa.wta.spark;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.log4j.*;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class LoggingTest {

    private Logger logger;

    private static void testConfigLoad() throws IOException {
        Properties props = new Properties();
        props.load(new FileInputStream("src/test/resources/log4j2.properties"));
        PropertyConfigurator.configure(props);
    }

    @Test
    public void loggerNotNullAfterInit() {
        assertThat(logger).isNull();
        logger = PluginLogger.getInstance();
        assertThat(logger).isNotNull();
    }

    @Test
    public void loggerSameInstance() {
        assertThat(PluginLogger.getInstance()).isEqualTo(PluginLogger.getInstance());
    }

    @Test
    public void parsedSuccessful() throws IOException {
        logger = PluginLogger.getInstance();
        testConfigLoad();
    }
}
