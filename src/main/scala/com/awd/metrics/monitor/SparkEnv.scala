package com.awd.metrics.monitor

import org.apache.spark.sql.SparkSession

trait SparkEnv {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("Metrics Monitor")
      .config("spark.eventLog.enabled", "true")
      .config("spark.logConf", "true")
      .config("spark.extraListeners", "com.awd.metrics.monitor.JobMetricsRecorder")
      .getOrCreate()
  }

}