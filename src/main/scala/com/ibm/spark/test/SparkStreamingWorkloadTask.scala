package com.ibm.spark.test
import scala.collection.mutable
import scala.io.StdIn
import scala.util.Random

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by vinayak on 27/11/16.
 */
class SparkStreamingWorkloadTask (name: String = "Streaming Work") extends SparkWorkloadTask(name) {
  override def execute(sparkContext: SparkContext): Unit = {

    val ssc = new StreamingContext(sparkContext, Seconds(1))

    //ssc.awaitTerminationOrTimeout()
    val txtstream = new TextRDDStream(sparkContext)
    val words = ssc.queueStream(txtstream.Queue)

    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    words.print()

    // Print the first ten elements of each RDD generated in this DStream to the console
    //wordCounts.print()

    val enqueuer = new Thread(new Runnable {
      override def run(): Unit = {
        try {
          while (true) {
            txtstream.next()
            Thread.sleep(800)
          }
        } catch {
          case _: InterruptedException =>
        }
      }
    }, "Enqueuer")

    enqueuer.setDaemon(true)

    enqueuer.start()

    ssc.start()             // Start the computation
    println ("stream wait start")
    ssc.awaitTerminationOrTimeout(5000)  // Wait for the computation to terminate
    println ("stream wait end")
    //StdIn.readLine()

    enqueuer.interrupt()
    ssc.stop(false)
    //StdIn.readLine()
  }

  class TextRDDStream (sc: SparkContext) {
    val queue = new mutable.Queue[RDD[String]] ()

    val random = new Random()

    val data = ("Apache Spark provides programmers with. an application programming interface " +
      "centered on a data structure called the resilient distributed dataset (RDD), a read-only " +
      "multiset of data items distributed over a. cluster of machines, that is maintained in a " +
      "fault-tolerant way.[2] It was developed in response to limitations in the MapReduce " +
      "cluster computing paradigm, which forces. a particular linear dataflow. structure on " +
      "distributed programs: MapReduce programs read input data from disk, map a function across " +
      "the data, reduce the results. of the map, and store reduction results on disk. Spark's RDDs" +
      " function as a working set for distributed programs that offers. a (deliberately) " +
      "restricted form of distributed shared memory.").split("\\.")

    val max = data.length

    def next(): Unit = {
      queue.synchronized {
        //println ("**** enqueuing ****")
        queue.enqueue(sc.makeRDD(data(random.nextInt(max)).split(" ")))
      }
    }

    def Queue: mutable.Queue[RDD[String]] = queue

  }
}
