
import java.text.DecimalFormat

import _root_.LossyCountingSpark.Item.Item
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{InputDStream, DStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.{rdd, HashPartitioner, SparkContext, SparkConf}
import scala.collection.immutable.IndexedSeq
import scala.collection.{immutable, mutable}
import scala.util.Random


object LossyCountingSpark {

  /**
   * 15% is the frequency above which we want to print out frequent items
   */
  val frequency = 15.0

  /**
   * output = f*N - e*N, where N is the total number of windows
   */
  val error = 0.1 * frequency

  val rand = new Random()

  object Item extends Enumeration{
    type Item = Value
    val Red = Value("Red")
    val Green = Value("Green")
    val Blue = Value ("Blue")
    val Yellow = Value ("Yellow")
    val Brown = Value ("Brown")
  }

  /**
   * Simulation of the window input data
   */
  val itemBatches = List[List[String]](
    List.concat(create(70, Item.Red), create(10, Item.Blue), create(15, Item.Yellow), create(2, Item.Brown), create(3, Item.Green)),
    List.concat(create(60, Item.Red), create(20, Item.Blue), create(20, Item.Yellow)),
    List.concat(create(70, Item.Red), create(15, Item.Blue), create(0, Item.Yellow), create(2, Item.Brown), create(13, Item.Green)),
    List.concat(create(80, Item.Red), create(20, Item.Blue)),
    List.concat(create(80, Item.Red), create(20, Item.Blue))
  )

  def main(args: Array[String]): Unit = {

    val sc = initSparkContext()
    val frequencyVal = sc.broadcast(frequency)
    val errorVal = sc.broadcast(error)
    val streamingContext = new StreamingContext(sc, Seconds(1))
    streamingContext.checkpoint("./checkpoint/")
    var totalWindows = 0

    val rddQueue = new mutable.SynchronizedQueue[RDD[String]]()
    val inputDStream = streamingContext.queueStream(rddQueue)

    //update window on driver
    inputDStream.foreachRDD(rdd => totalWindows += 1)

    val items: DStream[String] = inputDStream.flatMap(line => line.split(" "))
    val itemOneValuePairs: DStream[(String, Int)] = items.map(item => (item, 1))
    val countPerItem: DStream[(String, Int)] = itemOneValuePairs.reduceByKey((count1, count2) => count1 + count2)

    val updatedState = countPerItem.updateStateByKey(updateFrequencyByKeyAndDecr _,
      new HashPartitioner(streamingContext.sparkContext.defaultParallelism))

    //keep only the items that exceed the threshold given by the Lossy Counting algorithm
    val output = updatedState.filter(itemWithCounts => itemWithCounts._2.toDouble > (frequencyVal.value * totalWindows - errorVal.value * totalWindows))
    output.print()

    streamingContext.start() // Start the computation

    // Create and push some RDDs into the 
    for (i <- itemBatches.indices) {
      rddQueue += streamingContext.sparkContext.makeRDD(itemBatches(i))
      Thread.sleep(1000)
    }
    val labelWithCounts = calcTrueCounts(itemBatches)
    printTrueCounts(labelWithCounts)

    streamingContext.stop()
  }


  def updateFrequencyByKeyAndDecr(newValues: Seq[Int], currentCount: Option[Int]): Option[Int] = {
    val sum: Int = newValues.sum
    val aggregate: Int = currentCount match {
      case Some(count) =>
        count + sum
      case None =>
        sum
    }
    val decrementedValue = aggregate - 1
    if (itemShouldBeEliminated(decrementedValue)) {
      None //Spark will discard this key
    } else {
      Some(decrementedValue)
    }
  }


  def printTrueCounts(labelWithCounts: mutable.HashMap[String, Int]) = {
    val totalElements = labelWithCounts.foldLeft(0)(_ + _._2)
    println("True Counts")
    println(s"$totalElements total items")
    val df = new DecimalFormat("#.##")
    for (labelWithCount <- labelWithCounts) {
      val trueCount = labelWithCount._2
      val trueFreq = calcFreq(trueCount, totalElements)
      println(s"${labelWithCount._1} - ${df.format(trueFreq)} % with true count $trueCount")
    }
  }

  def calcFreq(count: Int, totalElements: Int) = {
    (count.toDouble / totalElements.toDouble) * 100.0
  }

  def calcTrueCounts(itemsList: List[List[String]]): mutable.HashMap[String, Int] = {
    val map = new mutable.HashMap[String, Int]
    itemsList.foreach { coloursList =>
      coloursList.foreach { colour =>
        val count = map.getOrElse(colour, 0)
        map.put(colour, count + 1)
      }
    }
    map
  }


  def create(elements: Int, item: Item): List[String] = {
    val seq: IndexedSeq[String] = for (i <- 1 to elements) yield {
      item.toString
    }
    seq.toList
  }



  private def itemShouldBeEliminated(decrementedValue: Int): Boolean = {
    decrementedValue <= 0
  }

  def initSparkContext(): SparkContext = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Lossy_Counting")
    new SparkContext(conf)
  }


}
