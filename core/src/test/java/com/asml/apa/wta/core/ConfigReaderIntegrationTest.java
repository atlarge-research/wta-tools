package com.asml.apa.wta.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.model.enums.Domain;
import com.asml.apa.wta.core.utils.WtaUtils;
import org.junit.jupiter.api.Test;

class ConfigReaderIntegrationTest {

  @Test
  void readsConfigNullArg() {
    assertThatThrownBy(() -> WtaUtils.readConfig(null)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void readsConfigNoFileInFilepath() {
    assertThatThrownBy(() -> WtaUtils.readConfig("nonExistentFile.json"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void readsConfigFileCorrectly() {
    RuntimeConfig cr = WtaUtils.readConfig("src/test/resources/testConfig.json");
    assertThat(cr.getAuthors()).isEqualTo(new String[] {"Test Name"});
    assertThat(cr.getDomain()).isEqualTo(Domain.SCIENTIFIC);
    assertThat(cr.getDescription()).isEqualTo("Test Description");
    assertThat(cr.getResourcePingInterval()).isEqualTo(2000);
    assertThat(cr.getExecutorSynchronizationInterval()).isEqualTo(4000);
    assertThat(cr.isStageLevel()).isEqualTo(true);
    assertThat(cr.getLogLevel()).isEqualTo("INFO");
    assertThat(cr.getOutputPath()).isEqualTo("/home/user/WTA");
  }

  @Test
  void readsConfigFileWhereTheDescriptionIsNotThere() {
    RuntimeConfig cr = WtaUtils.readConfig("src/test/resources/testConfigNoDesc.json");
    assertThat(cr.getAuthors()).isEqualTo(new String[] {"Test Name"});
    assertThat(cr.getDomain()).isEqualTo(Domain.ENGINEERING);
    assertThat(cr.getDescription()).isEqualTo("");
    assertThat(cr.getResourcePingInterval()).isEqualTo(1000);
    assertThat(cr.getExecutorSynchronizationInterval()).isEqualTo(2000);
    assertThat(cr.isStageLevel()).isEqualTo(true);
    assertThat(cr.getLogLevel()).isEqualTo("INFO");
    assertThat(cr.getOutputPath()).isEqualTo("/home/user/WTA");
  }

  @Test
  void readsConfigFileWhereTheEventsAreNotThere() {
    RuntimeConfig cr = WtaUtils.readConfig("src/test/resources/testConfigNoEvents.json");
    assertThat(cr.getAuthors()).isEqualTo(new String[] {"Test Name"});
    assertThat(cr.getDomain()).isEqualTo(Domain.INDUSTRIAL);
    assertThat(cr.getDescription()).isEqualTo("Test Description");
    assertThat(cr.isStageLevel()).isEqualTo(true);
    assertThat(cr.getOutputPath()).isEqualTo("/home/user/WTA");
  }

  @Test
  void readsConfigFileWhereNoAuthorsAreGiven() {
    assertThatThrownBy(() -> WtaUtils.readConfig("src/test/resources/testConfigNoAuthor.json"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void readsConfigFileWhereAuthorFieldNotThere() {
    assertThatThrownBy(() -> WtaUtils.readConfig("src/test/resources/testConfigInvalidAuthor.json"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void readsConfigFileWhereLogSettingIsNotThere() {
    RuntimeConfig cr = WtaUtils.readConfig("src/test/resources/testConfigNoLogSettings.json");
    assertThat(cr.getAuthors()).isEqualTo(new String[] {"Test Name"});
    assertThat(cr.getDomain()).isEqualTo(Domain.INDUSTRIAL);
    assertThat(cr.getDescription()).isEqualTo("Test Description");
    assertThat(cr.isStageLevel()).isEqualTo(true);
    assertThat(cr.getLogLevel()).isEqualTo("ERROR");
    assertThat(cr.getOutputPath()).isEqualTo("/home/user/WTA");
  }

  @Test
  void readsConfigFileWithInvalidDomain() {
    assertThatThrownBy(() -> WtaUtils.readConfig("src/test/resources/testConfigInvalidDomain.json"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void readsConfigFileWhereOutputPathIsNotThere() {
    assertThatThrownBy(() -> WtaUtils.readConfig("src/test/resources/testConfigNoOutputPath.json"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void readsConfigFileWhereIsStageLevelIsNotThere() {
    RuntimeConfig cr = WtaUtils.readConfig("src/test/resources/testConfigNoIsStageLevel.json");
    assertThat(cr.getAuthors()).isEqualTo(new String[] {"Test Name"});
    assertThat(cr.getDomain()).isEqualTo(Domain.SCIENTIFIC);
    assertThat(cr.getDescription()).isEqualTo("Test Description");
    assertThat(cr.isStageLevel()).isEqualTo(false);
    assertThat(cr.getLogLevel()).isEqualTo("INFO");
  }
}
