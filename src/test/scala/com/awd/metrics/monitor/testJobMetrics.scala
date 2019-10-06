package com.awd.metrics.monitor

import org.scalatest.FunSuite

class testJobMetrics extends FunSuite with SparkEnv {

  import spark.implicits._

  val dummy = Seq(("Test1", "Test2", 324), ("Dummy1", "Dummy2", 123)).toDF("A", "B", "C")
  val dummy2 = Seq(("Test1", "Alfie", "Davidson")).toDF("A", "Forename", "Surname")

  dummy.join(dummy2, Seq("A"), "left_outer").show(10, false)

}
