package org.learn.flink.connector.hive;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class FlinkStart {
  public static void main(String[] args) {
    EnvironmentSettings settings =
        EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();

    TableEnvironment env = TableEnvironment.create(settings);
    }
}
