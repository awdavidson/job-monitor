package com.awd.metrics.monitor

import org.scalatest.FunSuite

class testJobMetrics extends FunSuite
  with SparkEnv {

  import spark.implicits._


  def randomInt1to100 = scala.util.Random.nextInt(100)+1

  val df = spark.sparkContext.parallelize(
    Seq.fill(100){(randomInt1to100,randomInt1to100,randomInt1to100)}
  ).toDF("A", "B", "C")


  df.show()


  val dummy2 = Seq((10, "Alfie", "Davidson")).toDF("A", "Forename", "Surname")

  df.join(dummy2, Seq("A"), "left_outer").show(10, false)

}
