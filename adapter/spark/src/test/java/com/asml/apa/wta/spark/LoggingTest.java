package com.asml.apa.wta.spark;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class LoggingTest {

    @BeforeEach
    public void setup() {
        PluginLogger.loadConfig();
    }

    @Test
    public void failWithoutLoadingConfig() {
        PluginLogger.log("working");
    }

    @Test
    public void testLog() {
        PluginLogger.log("working");
        PluginLogger.log("is");
    }
}
