# metrics-monitor

#### Overview
This module provides a custom Spark Listener that generates a report at the end of a job. The report contains statistics such as: Number of Stages, Longest Stage, Smallest Task, Largest Task and Average Task.

This information can be leveraged to understand the distribution of our data. The main purpose of this is to highlight skewed partitions from either the way data is stored or shuffled.

#### How to use
After including the jar within your project the custom Spark Listener can be include by either:

```scala
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
```

or

```scala
 import spark.implicits._
 spark.sparkContext.addListener("com.awd.metrics.monitor.JobMetricsRecorder")
```

#### Output
```
20/02/08 19:26:38 WARN JobMetricsRecorder: 
   Application ID: local-1581189980716
   Application Duration (ms): 10599
   Number of Stages: 3
   Longest Stage: Stage Id: 2 Duration (ms): 7647
   Number of Tasks: 208
   Longest Task (ms): 1554
   Shortest Task (ms): 39
   Average Task (ms) : 534
```
