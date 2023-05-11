package com.asml.apa.wta.spark.listener;

import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerApplicationStart;

/**
 * This class is an application-level listener for the Spark data source.
 *
 * @author Pil Kyu Cho
 * @since 1.0.0
 */
public class ApplicationLevelListener extends SparkListener {
        public void onApplicationStart(SparkListenerApplicationStart applicationStart) {}

        public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {}
}
