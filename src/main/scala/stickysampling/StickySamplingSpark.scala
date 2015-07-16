package stickysampling

import java.util.concurrent.ThreadLocalRandom

import utils.Utils._
import model.{ItemWithId, Item}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import storage.MemCacheDB

import scala.collection.mutable

object StickySamplingSpark {

  //f = 0.2, e=0.02, d = 0.002
  // t = (1/e) * log(1/(s*d))
  // e = 50 * log (1/(0.2*0.002))


  /**
   * Frequency above which we want to print out frequent items
   */
  val frequency = 0.5

  /**
   * output = f*N - e*N, where N is the total number of elements
   */
  val error = 0.2 * frequency

  /**
   * t = (1/error) * log(1/(frequency * probabilityOfFailure))
   * probabilityOfFailure affects the number of elements before changing the sampling rate
   */
  val probabilityOfFailure = 0.1 * error

  /**
   * The first t elements are sampled at rate r=1, the next 2t are sampled at rate r=2, the next 4t at r=4 and so on
   */
  val t = (1.0 / error) * Math.log(1.0 / (frequency * probabilityOfFailure))

  val memCacheDB = new MemCacheDB[String, Int]

  val RATE = "rate"
  val INITIAL_SAMPLING_RATE = 1

  /**
   * Simulation of the input data assigned with a numeric id
   */
  val itemBatches: List[List[ItemWithId]] = {
    val itemset = List[List[String]](
      List.concat(create(19, Item.Red), create(11, Item.Blue), create(10, Item.Yellow), create(10, Item.Brown), create(0, Item.Green)),
      List.concat(create(30, Item.Red), create(10, Item.Blue), create(10, Item.Yellow)),
      List.concat(create(30, Item.Red), create(10, Item.Blue), create(0, Item.Yellow), create(5, Item.Brown), create(5, Item.Green)),
      List.concat(create(40, Item.Red), create(10, Item.Blue)),
      List.concat(create(40, Item.Red), create(10, Item.Blue))
    )
    var orderId = 0L
    for (currList <- itemset) yield {
      for (str <- currList) yield {
        orderId += 1L
        ItemWithId(str, orderId)
      }
    }
  }


  def main(args: Array[String]): Unit = {

    println(s"Frequency: $frequency, Error: $error, Probability of failure: $probabilityOfFailure, t=$t")
    val sc = initSparkContext()
    val frequencyVal = sc.broadcast(frequency)
    val errorVal = sc.broadcast(error)

    var totalElements = 0L

    val streamingContext = new StreamingContext(sc, Seconds(1))
    streamingContext.checkpoint("./checkpoint/")
    val partitioner = new HashPartitioner(streamingContext.sparkContext.defaultParallelism)

    val rddQueue = new mutable.SynchronizedQueue[RDD[ItemWithId]]()
    val inputDStream = streamingContext.queueStream(rddQueue)

    inputDStream.foreachRDD(rdd => totalElements += rdd.count())


    val itemsWithCount = inputDStream.map(itemWithId => (itemWithId.item, ItemCountRate(itemWithId.id, memCacheDB.get(RATE).getOrElse(INITIAL_SAMPLING_RATE), 0)))

    val updatedState: DStream[(String, ItemCountRate)] = itemsWithCount.updateStateByKey(updateStateBySampling _, partitioner)

    val output = updatedState.filter{
      itemWithCounts =>
        println(s"Total elements $totalElements, predicate: ${frequencyVal.value * totalElements - errorVal.value * totalElements}")
        itemWithCounts._2.currentCount.toDouble > (frequencyVal.value * totalElements - errorVal.value * totalElements)
    }
    output.print()

    streamingContext.start() // Start the computation

    // push some RDDs
    for (i <- itemBatches.indices) {
      rddQueue += streamingContext.sparkContext.makeRDD(itemBatches(i))
      Thread.sleep(1000)
    }

    streamingContext.stop()

  }


  def samplingRateHasChanged(currentSamplingRate: Int, existingStateOpt: Option[ItemCountRate]): Boolean = {
    existingStateOpt match {
      case Some(existingState) =>
        currentSamplingRate > existingState.rate
      case None =>
        //no prev state exists, so sampling rate has not changed
        false
    }
  }

  def lowerIdFirstComparator(item1: ItemCountRate, item2: ItemCountRate): Boolean ={
    item1.itemId < item2.itemId
  }

  def updateStateBySampling(newItems: Seq[ItemCountRate], currentCountOpt: Option[ItemCountRate]): Option[ItemCountRate] = {

    //sort new values to preserve any order
    val valuesSorted = newItems.sortWith(lowerIdFirstComparator)

    newItems.foldLeft(currentCountOpt){ (B, listItem) =>
      val currSamplingRate = SamplingRateRetriever.deriveSamplingRate(listItem.itemId, t)
      val newCount = B match {
        case Some(currentCount) =>
          currentCount.currentCount + 1

        case None => //item does not exist
          getNewCountWithSampling(B, listItem, currSamplingRate)
      }

      if (newCount > 0){
        if (samplingRateHasChanged(currSamplingRate, B)){
          println(s"Sampling rate has changed: $currSamplingRate")
          memCacheDB.put(RATE, currSamplingRate)
          //toss a coin and for each unsuccessful shot, drop new count by one until successful or until 0
          dropCountByTossingCoins(newCount) match {
            case Some(reducedCount) =>
              Some(ItemCountRate(listItem.itemId, currSamplingRate, reducedCount))
            case None =>
              None
          }
        }else{
          Some(ItemCountRate(listItem.itemId, currSamplingRate, newCount))
        }
      } else{
        None
      }

    }

  }

  def getCurrentCount(currentCountOpt: Option[ItemCountRate]): Long = {
    currentCountOpt match {
      case Some(count) =>
        count.currentCount
      case None =>
        0L
    }
  }

  def getNewCountWithSampling(B: Option[ItemCountRate], listItem: ItemCountRate, samplingRate: Int): Long = {
    val countToAdd = if (canSelectItWithSamplingRate(samplingRate)) 1L else 0
      B match {
        case Some(existingItem) =>
          existingItem.currentCount + countToAdd
        case None =>
          countToAdd
      }
  }

  def canSelectItWithSamplingRate(samplingRate: Int): Boolean = {
    ThreadLocalRandom.current().nextDouble() < (1.0 / samplingRate)
  }



  def dropCountByTossingCoins(newCount: Long): Option[Long] = {
    var countAfterSampling = newCount
    while(countAfterSampling > 0 && unsuccessfulCoinToss()){
      countAfterSampling -= 1
    }
    if (countAfterSampling>0) Some(countAfterSampling) else None
  }

  def unsuccessfulCoinToss(): Boolean = {
    ThreadLocalRandom.current().nextDouble() < 0.5
  }

  def initSparkContext(): SparkContext = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Sticky Sampling")
    new SparkContext(conf)
  }



}
