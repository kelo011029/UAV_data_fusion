package com.telecom.dataProcess

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.catalog.hive.HiveCatalog

object kafka2hive {
  def main(args:Array[String]):Unit={
    val settings = EnvironmentSettings.inStreamingMode
    val tableEnv = TableEnvironment.create(settings)

    val name = "my_hive"
    val defaultDatabase = "radio_data"
    val hiveConfDir = "/opt/hive/conf"

    val hive = new HiveCatalog(name, defaultDatabase, hiveConfDir)
    tableEnv.registerCatalog("my_hive", hive)

    // set the HiveCatalog as the current catalog of the session
    tableEnv.useCatalog("my_hive")
  }

}
