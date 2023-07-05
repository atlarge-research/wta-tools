package com.asml.apa.wta.spark.datasource;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.asml.apa.wta.core.WtaWriter;
import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.model.Domain;
import com.asml.apa.wta.spark.listener.ApplicationLevelListener;
import com.asml.apa.wta.spark.listener.JobLevelListener;
import com.asml.apa.wta.spark.listener.StageLevelListener;
import com.asml.apa.wta.spark.listener.TaskLevelListener;
import com.asml.apa.wta.spark.stream.MetricStreamingEngine;
import org.apache.spark.SparkContext;
import org.junit.jupiter.api.Test;

class SparkDataSourceTest {

  @Test
  void removeListeners() {
    SparkContext ctx = mock(SparkContext.class);
    RuntimeConfig config = new RuntimeConfig();
    config.setAuthors(new String[] {"Harry Porter"});
    config.setDomain(Domain.SCIENTIFIC);
    SparkDataSource dataSource =
        new SparkDataSource(ctx, config, mock(MetricStreamingEngine.class), mock(WtaWriter.class));

    dataSource.registerTaskListener();
    dataSource.registerStageListener();
    dataSource.registerJobListener();
    dataSource.registerApplicationListener();

    dataSource.removeListeners();

    verify(ctx).addSparkListener(any(TaskLevelListener.class));
    verify(ctx).addSparkListener(any(StageLevelListener.class));
    verify(ctx).addSparkListener(any(JobLevelListener.class));
    verify(ctx).addSparkListener(any(ApplicationLevelListener.class));

    verify(ctx).removeSparkListener(any(TaskLevelListener.class));
    verify(ctx).removeSparkListener(any(StageLevelListener.class));
    verify(ctx).removeSparkListener(any(JobLevelListener.class));
    verify(ctx).removeSparkListener(any(ApplicationLevelListener.class));
  }
}
