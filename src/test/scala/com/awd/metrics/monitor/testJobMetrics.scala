package com.awd.metrics.monitor

import org.scalatest.FunSuite

class testJobMetrics extends FunSuite
  with SparkEnv {

  import spark.implicits._


  def randomInt1to100 = scala.util.Random.nextInt(100) + 1

  test("Process for our listener") {

    val df = spark.sparkContext.parallelize(
      Seq.fill(1000) {
        (randomInt1to100, randomInt1to100, randomInt1to100)
      }
    ).toDF("A", "B", "C")

    val dummy2 = Seq.fill(20) {
      (randomInt1to100, "Alfie", "Davidson")
    }.toDF("A", "Forename", "Surname")

    val joinedDF = df.join(dummy2, Seq("A"), "left_outer")

    joinedDF.write.mode("Overwrite").parquet("output/test.parq")


  }


}
