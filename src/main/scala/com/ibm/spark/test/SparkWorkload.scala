package com.ibm.spark.test

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, ThreadFactory}

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by vinayak on 24/11/16.
 */
object SparkWorkload {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val sparkWorkloadConf = parseArgs(args)

    /*   val execService = Executors.newFixedThreadPool(sparkWorkloadConf.numApps, new
           WorkloadThreadFactory())

       //execService.invokeAll()

      val r = for (i <- 1 to sparkWorkloadConf.numApps) yield {
         val sparkWorkload = new SparkWorkload(sparkWorkloadConf, List(new SparkSQLWorkloadTask()))
         getWorkloadRunner(sparkWorkloadConf, sparkWorkload, new SparkContext(conf))
       }

       println(s"$r - ${r.getClass}")
   */
    val sc = new SparkContext(conf)
    val sparkWorkload = new SparkWorkload(sparkWorkloadConf, List(new SparkStreamingWorkloadTask(),
      new SparkSQLWorkloadTask(), new SparkStorageWorkloadTask()))

    getWorkloadRunner(sparkWorkload, conf, sc).run()

    sc.stop()
  }

  /*def getWorkloadRunner1(conf: SparkWorkloadConf, sparkWorkload: SparkWorkload, sparkContext:
  SparkContext): WorkloadRunner = {
    conf match {
      case (n, i, 0) => new IterationWorkloadRunner(sparkWorkload, sparkContext, i)
      case (n, 0, d) => new DurationWorkloadRunner(sparkWorkload, sparkContext, d)
      case _ => new IterationWorkloadRunner(sparkWorkload, sparkContext, 1)
    }
  } */

  def getWorkloadRunner(sparkWorkload: SparkWorkload, sparkConf: SparkConf, sparkContext:
  SparkContext): WorkloadRunner = {

    val conf = sparkWorkload.workLoadConf()

    if (conf.iterations > 0) {
      new IterationWorkloadRunner(sparkWorkload, sparkContext, conf.iterations)
    } else if (sparkWorkload.workLoadConf().duration > 0) {
      new DurationWorkloadRunner(sparkWorkload, sparkContext, conf.duration)
    } else {
      new IterationWorkloadRunner(sparkWorkload, sparkContext, 1)
    }

  }

  def parseArgs(args: Array[String]): SparkWorkloadConf = {
    var iterations = 0
    var duration = 0L
    var wait = false

    var i = 0

    while (i < args.length) {
      args(i) match {
        case "--iterations" if (i + 1 < args.length) => {
          iterations = args(i + 1).toInt
          i += 1
        }
        case "--duration" if (i + 1 < args.length) => {
          duration = args(i + 1).toInt
          i += 1
        }
        case "--wait" => wait = true
        case _ =>
      }
      i += 1
    }
    new SparkWorkloadConf(1, iterations, duration, wait)
  }
}

class SparkWorkload(conf: SparkWorkloadConf, tasks: List[SparkWorkloadTask]) {
  def workLoadTasks() = tasks

  def workLoadConf() = conf
}

case class SparkWorkloadConf(numApps: Int, iterations: Int, duration: Long, waitAfterComplete: Boolean = false)

class WorkloadThreadFactory extends ThreadFactory {

  val count = new AtomicInteger(0)

  override def newThread(r: Runnable): Thread = {
    val th = new Thread()
    th.setDaemon(true)
    th.setName("workload-runner-" + count.incrementAndGet())
    th
  }

}