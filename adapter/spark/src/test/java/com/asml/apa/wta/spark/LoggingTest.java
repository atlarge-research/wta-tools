package com.asml.apa.wta.spark;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;

class LoggingTest {

    private Logger logger;

    @Test
    public void loggerNotNullAfterInit() {
        assertThat(logger).isNull();
        logger = PluginLogger.getInstance();
        assertThat(logger).isNotNull();
    }

    @Test
    public void loggerSameInstance() {
        logger = PluginLogger.getInstance();
        assertThat(logger).isEqualTo(PluginLogger.getInstance());
    }
}
