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
    val sparkWorkloadConf = new SparkWorkloadConf(1, 0, 2)

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
    val sparkWorkload = new SparkWorkload(sparkWorkloadConf, List(new SparkSQLWorkloadTask()))
    getWorkloadRunner(sparkWorkloadConf, sparkWorkload, sc).run()

    println("End: ")
    scala.io.StdIn.readLine()
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

  def getWorkloadRunner(conf: SparkWorkloadConf, sparkWorkload: SparkWorkload, sparkContext:
  SparkContext): WorkloadRunner = {

    if (conf.iterations > 0) {
      new IterationWorkloadRunner(sparkWorkload, sparkContext, conf.iterations)
    } else if (conf.duration > 0) {
      new DurationWorkloadRunner(sparkWorkload, sparkContext, conf.duration)
    } else {
      new IterationWorkloadRunner(sparkWorkload, sparkContext, 1)
    }

  }
}

class SparkWorkload(conf: SparkWorkloadConf, tasks: List[SparkWorkloadTask]) {
  def workLoadTasks() = tasks
}

case class SparkWorkloadConf(numApps: Int, iterations: Int, duration: Long)

class WorkloadThreadFactory extends ThreadFactory {

  val count = new AtomicInteger(0)

  override def newThread(r: Runnable): Thread = {
    val th = new Thread()
    th.setDaemon(true)
    th.setName("workload-runner-" + count.incrementAndGet())
    th
  }

}