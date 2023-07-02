package com.asml.apa.wta.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.model.Domain;
import org.junit.jupiter.api.Test;

class ConfigReaderIntegrationTest {

  @Test
  void readsConfigNullArg() {
    assertThatThrownBy(() -> RuntimeConfig.readConfig(null)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void readsConfigNoFileInFilepath() {
    assertThatThrownBy(() -> RuntimeConfig.readConfig("nonExistentFile.json"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void readsConfigFileCorrectly() {
    RuntimeConfig cr = RuntimeConfig.readConfig("src/test/resources/testConfig.json");
    assertThat(cr.getAuthors()).isEqualTo(new String[] {"Test Name"});
    assertThat(cr.getDomain()).isEqualTo(Domain.SCIENTIFIC);
    assertThat(cr.getDescription()).isEqualTo("Test Description");
    assertThat(cr.getResourcePingInterval()).isEqualTo(2000);
    assertThat(cr.getExecutorSynchronizationInterval()).isEqualTo(4000);
    assertThat(cr.isStageLevel()).isEqualTo(true);
    assertThat(cr.getOutputPath()).isEqualTo("/home/user/WTA");
    assertThat(cr.isAggregateMetrics()).isTrue();
  }

  @Test
  void readsConfigFileWhereTheDescriptionIsNotThere() {
    RuntimeConfig cr = RuntimeConfig.readConfig("src/test/resources/testConfigNoDesc.json");
    assertThat(cr.getAuthors()).isEqualTo(new String[] {"Test Name"});
    assertThat(cr.getDomain()).isEqualTo(Domain.ENGINEERING);
    assertThat(cr.getDescription()).isEqualTo("");
    assertThat(cr.getResourcePingInterval()).isEqualTo(500);
    assertThat(cr.getExecutorSynchronizationInterval()).isEqualTo(-1);
    assertThat(cr.isStageLevel()).isEqualTo(true);
    assertThat(cr.getOutputPath()).isEqualTo("/home/user/WTA");
  }

  @Test
  void readsConfigFileWhereTheEventsAreNotThere() {
    RuntimeConfig cr = RuntimeConfig.readConfig("src/test/resources/testConfigNoEvents.json");
    assertThat(cr.getAuthors()).isEqualTo(new String[] {"Test Name"});
    assertThat(cr.getDomain()).isEqualTo(Domain.INDUSTRIAL);
    assertThat(cr.getDescription()).isEqualTo("Test Description");
    assertThat(cr.isStageLevel()).isEqualTo(true);
    assertThat(cr.getOutputPath()).isEqualTo("/home/user/WTA");
  }

  @Test
  void readsConfigFileWhereNoAuthorsAreGiven() {
    assertThatThrownBy(() -> RuntimeConfig.readConfig("src/test/resources/testConfigNoAuthor.json"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void readsConfigFileWhereAuthorFieldNotThere() {
    assertThatThrownBy(() -> RuntimeConfig.readConfig("src/test/resources/testConfigInvalidAuthor.json"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void readsConfigFileWhereLogSettingIsNotThere() {
    RuntimeConfig cr = RuntimeConfig.readConfig("src/test/resources/testConfigNoLogSettings.json");
    assertThat(cr.getAuthors()).isEqualTo(new String[] {"Test Name"});
    assertThat(cr.getDomain()).isEqualTo(Domain.INDUSTRIAL);
    assertThat(cr.getDescription()).isEqualTo("Test Description");
    assertThat(cr.isStageLevel()).isEqualTo(true);
    assertThat(cr.getOutputPath()).isEqualTo("/home/user/WTA");
  }

  @Test
  void readsConfigFileWithInvalidDomain() {
    assertThatThrownBy(() -> RuntimeConfig.readConfig("src/test/resources/testConfigInvalidDomain.json"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void readsConfigFileWhereOutputPathIsNotThere() {
    assertThatThrownBy(() -> RuntimeConfig.readConfig("src/test/resources/testConfigNoOutputPath.json"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void readsConfigFileWhereIsStageLevelIsNotThere() {
    RuntimeConfig cr = RuntimeConfig.readConfig("src/test/resources/testConfigNoIsStageLevel.json");
    assertThat(cr.getAuthors()).isEqualTo(new String[] {"Test Name"});
    assertThat(cr.getDomain()).isEqualTo(Domain.SCIENTIFIC);
    assertThat(cr.getDescription()).isEqualTo("Test Description");
    assertThat(cr.isStageLevel()).isEqualTo(false);
  }

  @Test
  void readsConfigFileWhereAggregateMetricsIsNotThereDefaultsToFalse() {
    RuntimeConfig cr = RuntimeConfig.readConfig("src/test/resources/testConfigNoIsStageLevel.json");
    assertThat(cr.isAggregateMetrics()).isFalse();
  }

  @Test
  void stringNumbersAutoParsedToIntAndViceVersa() {
    RuntimeConfig cr = RuntimeConfig.readConfig("src/test/resources/testConfigInvalidValues.json");
    assertThat(cr.getResourcePingInterval()).isEqualTo(2000);
    assertThat(cr.getExecutorSynchronizationInterval()).isEqualTo(2000);
    assertThat(cr.getDescription()).isEqualTo("13");
  }

  @Test
  void unparseableStringBooleansSetsToDefaultValues() {
    RuntimeConfig cr = RuntimeConfig.readConfig("src/test/resources/testConfigInvalidValues.json");
    assertThat(cr.isStageLevel()).isFalse();
    assertThat(cr.isAggregateMetrics()).isFalse();
  }
}
