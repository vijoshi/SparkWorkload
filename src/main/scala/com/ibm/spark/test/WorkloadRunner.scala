package com.ibm.spark.test

import java.util.concurrent.atomic.AtomicBoolean

import scala.io.StdIn

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by vinayak on 24/11/16.
 */

abstract class WorkloadRunner(sparkWorkload: SparkWorkload, sparkContext:
SparkContext) extends
  Runnable {
  val requestStop = new AtomicBoolean(false)

  def stopRunning(): Unit = {
    requestStop.set(true)
  }

  def stepExecuted(): Unit = {

  }

  def beforeStart(): Unit = {

  }

  def afterEnd(): Unit = {
    if (sparkWorkload.workLoadConf().waitAfterComplete) {
      StdIn.readLine()
    }
  }

  def shouldStop(): Boolean

  override def run(): Unit = {
    beforeStart()
    while (!requestStop.get() && !Thread.interrupted() && !shouldStop()) {
      sparkWorkload.workLoadTasks().foreach(task => task.execute(sparkContext))
      stepExecuted()
    }
    afterEnd()
  }
}

class IterationWorkloadRunner(sparkWorkload: SparkWorkload, sparkContext:
SparkContext,
    iterations: Int) extends
  WorkloadRunner(sparkWorkload, sparkContext) {

  private var execCount = 0

  override def stepExecuted(): Unit = {
    execCount += 1
  }

  override def shouldStop(): Boolean = {
    execCount >= iterations
  }
}

class DurationWorkloadRunner(sparkWorkload: SparkWorkload, sparkContext:
SparkContext,
    timeoutMillis: Long) extends
  WorkloadRunner(sparkWorkload, sparkContext) {

  var startTime = 0L

  override def beforeStart(): Unit = {
    startTime = System.currentTimeMillis()
  }

  override def shouldStop(): Boolean = {
    System.currentTimeMillis() - startTime >= timeoutMillis
  }
}
