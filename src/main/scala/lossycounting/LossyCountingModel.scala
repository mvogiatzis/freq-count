package lossycounting

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import storage.MemCacheDB

import scala.collection.mutable


class LossyCountingModel[T](
                             val frequency: Double,
                             val error: Double
                             ) extends Serializable {

  private var totalProcessedElements = 0L

  private val map = mutable.HashMap.empty[T, Int]
  val itemCountDB = new MemCacheDB[String, Int]


  def process(dataWindow: List[T]): LossyCountingModel[T] = {
    dataWindow.foreach { item =>
      totalProcessedElements += 1
      incrCount(item)
    }
    decreaseAllFrequencies()
    this
  }

  def computeOutput(): Array[(T, Int)] = {
    map.filter { itemWithFreq =>
      itemWithFreq._2.toDouble >= (frequency * totalProcessedElements - error * totalProcessedElements)
    }.toArray.sortWith((pair1, pair2) => pair1._2 > pair2._2)
  }

  def incrCount(item: T): Unit = {
    map.get(item) match {
      case Some(value) =>
        map.put(item, value + 1)
      case None =>
        map.put(item, 1)
    }
  }


  def decreaseAllFrequencies(): Unit = {
    map.foreach { itemFrequency =>
      if (itemShouldBeRemoved(itemFrequency)) {
        map.remove(itemFrequency._1)
      } else {
        map.put(itemFrequency._1, itemFrequency._2 - 1)
      }
    }
  }

  def itemShouldBeRemoved(itemFrequency: (T, Int)): Boolean = {
    itemFrequency._2 == 1
  }


  ////////////////////////////// SPARK //////////////////////////////////////

  private def itemShouldBeEliminated(decrementedValue: Int): Boolean = {
    decrementedValue <= 0
  }

  def update(data: RDD[String]): LossyCountingModel[T] = {
    //    inputDStream.foreachRDD { rdd =>
    //      totalProcessedElements += rdd.count() }
    totalProcessedElements += data.count()

    println("IN process")
    val items = data.flatMap(line => line.split(" "))
    val itemOneValuePairs = items.map(item => (item, 1))

    val countPerItem: RDD[(String, Int)] = itemOneValuePairs.reduceByKey((count1, count2) => count1 + count2)

    val lossyCounts = countPerItem.map(keyCounts => (keyCounts._1, lossyCount(keyCounts)))
    val itemsAboveThreshold = lossyCounts.filter { itemWithCounts => itemWithCounts._2.toDouble >= (frequency * totalProcessedElements - error * totalProcessedElements) }

    this
  }

  def lossyCount(newValue: (String, Int)): Int = {
    val itemLabel: String = newValue._1
    val windowCount: Int = newValue._2
    val aggregate = itemCountDB.get(newValue._1) match {
      case Some(count) =>
        count + windowCount
      case None =>
        windowCount
    }
    val decrementedValue = aggregate - 1
    if (itemShouldBeEliminated(decrementedValue)) {
      itemCountDB.remove(itemLabel)
      0
    } else {
      itemCountDB.put(itemLabel, decrementedValue)
      decrementedValue
    }
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

  def getTotalProcessedElements: Long = {
    totalProcessedElements
  }


}
