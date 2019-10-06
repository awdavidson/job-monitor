package com.awd.metrics.monitor

import org.apache.spark.scheduler._

import scala.collection.mutable.{ArrayBuffer}
import org.slf4j.LoggerFactory

case class StageVals(appId: String,
                     stageId: Int,
                     stageDuration: Long,
                     numOfTasks: Int
                    )

case class TaskVals(stageId: Int,
                    taskDuration: Long)

case class AppMetrics(appId: String,
                      numOfStages: Int,
                      appDuration: Long,
                      numOfTasks: Int,
                      maxTaskDuration: Long,
                      minTaskDuration: Long,
                      avgTaskDuration: Long
                     )


class JobMetricsRecorder extends SparkListener {

  val stageMetricsData: ArrayBuffer[StageVals] = ArrayBuffer.empty[StageVals]
  var appId: String = _
  val taskDuration: ArrayBuffer[(Int, Long)] = ArrayBuffer.empty[(Int, Long)]

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    appId = applicationStart.appId.getOrElse("Not Defined")
  }


  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {

    val stageInfo = stageCompleted.stageInfo

    val eachStageMetrics = StageVals(appId,
      stageInfo.stageId,
      stageInfo.completionTime.getOrElse(0L) - stageInfo.submissionTime.getOrElse(0L),
      stageInfo.numTasks
    )

    stageMetricsData += eachStageMetrics

    println(s"Check the following: " + stageMetricsData)
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val eachTaskDuration = Tuple2(taskEnd.stageId,
      taskEnd.taskInfo.finishTime - taskEnd.taskInfo.launchTime
    )

    taskDuration += eachTaskDuration

  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {

    val aggStageMetrics: Array[(String, (Int, Long, Int))] = stageMetricsData.groupBy(_.appId).mapValues(x =>
      (
        x.map(_.stageId).size,
        x.map(_.stageDuration).sum,
        x.map(_.numOfTasks).sum
      )
    ).toArray

    val metrics = AppMetrics(appId,
      aggStageMetrics.head._2._1,
      aggStageMetrics.head._2._2,
      aggStageMetrics.head._2._3,
      stageMax(taskDuration).maxBy(_._2)._2,
      stageMin(taskDuration).minBy(_._2)._2,
      avg(stageAvg(taskDuration))
    )

    val logger = LoggerFactory.getLogger(this.getClass.getName)

    logger.warn(s"\n   Application ID: " + metrics.appId +
      s"\n   Number of Stages: " + metrics.numOfStages +
      s"\n   Application Duration (ms): " + metrics.appDuration +
      s"\n   Number of Tasks: " + metrics.numOfTasks +
      s"\n   Longest Task (ms): " + metrics.maxTaskDuration +
      s"\n   Shortest Task (ms): " + metrics.minTaskDuration +
      s"\n   Average Task (ms) : " + metrics.avgTaskDuration
    )

  }

  def stageMax(taskDuration: ArrayBuffer[(Int, Long)]): List[(Int, Long)] = {
    taskDuration
      .groupBy(_._1)
      .mapValues(_.map(_._2).max)
      .toList
  }

  def stageMin(taskDuration: ArrayBuffer[(Int, Long)]): List[(Int, Long)] = {
    taskDuration
      .groupBy(_._1)
      .mapValues(_.map(_._2).min)
      .toList
  }

  def stageAvg(taskDuration: ArrayBuffer[(Int, Long)]): List[(Int, Long)] = {
    taskDuration
      .groupBy(_._1)
      .map {
        case (stageId, taskDuration) =>
          val times = taskDuration.map(_._2)
          (stageId, times.sum / times.size)
      }
      .toList
  }

  def avg(stageAvg: List[(Int, Long)]): Long = {
    val times = stageAvg.map(_._2)
    times.sum / times.size
  }
}