package frequencycount.stickysampling

import frequencycount.{Item, FrequencyCount}
import utils.RandomNumberGenerator
import utils.Utils._

import scala.collection.immutable.HashMap
import scala.collection.mutable

/**
 *
 * @param frequency Frequency above which we want to print out frequent items
 * @param error output = f*N - e*N, where N is the total number of elements
 * @tparam T The type of item
 */
class StickySamplingModel[T](val frequency: Double,
                             val error: Double,
                              val probabilityOfFailure: Double) extends FrequencyCount[T] {

  private var totalProcessedElements = 0L
  private val map = mutable.HashMap.empty[T, Int]

  var rng = new RandomNumberGenerator()

  def withRng(randomNumberGenerator: RandomNumberGenerator): StickySamplingModel[T] = {
    this.rng = randomNumberGenerator
    this
  }

  /**
   * The first t elements are sampled at rate r=1, the next 2t are sampled at rate r=2, the next 4t at r=4 and so on
   */
  val t = (1.0 / error) * Math.log(1.0 / (frequency * probabilityOfFailure))

  val INITIAL_SAMPLING_RATE = 1
  var samplingRate = INITIAL_SAMPLING_RATE

  def process(dataStream: List[T]): StickySamplingModel[T] = {

    dataStream.foreach { item =>
      totalProcessedElements += 1
      val currSamplingRate = SamplingRateRetriever.deriveSamplingRate(totalProcessedElements, t)

      updateItemWithSampling(item, currSamplingRate)

      if (samplingRateHasChanged(samplingRate, currSamplingRate)) {
        decreaseAllEntriesByCoinToss(currSamplingRate)
      } else {
        //do nothing
      }
      samplingRate = currSamplingRate
    }
    this
  }

  private def updateItemWithSampling(item: T, samplingRate: Int): Unit = {
    map.get(item) match {
      case Some(existingItem) =>
        map.put(item, existingItem + 1)
      case None =>
        if (canSelectItWithSamplingRate(samplingRate)) {
          map.put(item, 1)
        } else {
          ()
        }
    }
  }

  private def canSelectItWithSamplingRate(samplingRate: Int): Boolean = {
    rng.getNextDouble() < (1.0 / samplingRate)
  }

  private def decreaseAllEntriesByCoinToss(samplingRate: Int): Unit = {
    map.foreach { item =>
      var currCount = item._2
      while (currCount > 0 && unsuccessfulCoinToss()) {
        currCount -= 1
      }
      if (currCount > 0)
        map.put(item._1, currCount)
      else
        map.remove(item._1)
    }
  }

  private def samplingRateHasChanged(prevRate: Int, currRate: Int): Boolean = {
    currRate > prevRate
  }

  private def unsuccessfulCoinToss(): Boolean = {
    rng.getNextDouble() > 0.5
  }

  def computeOutput(): Array[(T, Int)] = {
    map.filter { itemWithFreq =>
      itemWithFreq._2.toDouble >= (frequency * totalProcessedElements - error * totalProcessedElements)
    }.toArray.sortWith((pair1, pair2) => pair1._2 > pair2._2)
  }

  //used for testing
  def setModel(predefinedMap: HashMap[T, Int]): StickySamplingModel[T] = {
    map.clear()
    predefinedMap.foreach(pair => this.map += pair)
    this
  }

  //used for testing
  def getMap(): mutable.HashMap[T, Int] ={
    map
  }

}

object StickySamplingModel{

  def main(args: Array[String]): Unit = {
      val frequency = 0.2
      val error = 0.1 * frequency
    val probabilityOfFailure = 0.1 * error

      val itemBatches = List(
        List.concat(create(19, Item.Red), create(11, Item.Blue), create(10, Item.Yellow), create(10, Item.Brown), create(0, Item.Green)),
        List.concat(create(30, Item.Red), create(10, Item.Blue), create(10, Item.Yellow)),
        List.concat(create(30, Item.Red), create(10, Item.Blue), create(0, Item.Yellow), create(5, Item.Brown), create(5, Item.Green)),
        List.concat(create(40, Item.Red), create(10, Item.Blue)),
        List.concat(create(40, Item.Red), create(10, Item.Blue))
      )

      val model = new StickySamplingModel[String](frequency, error, probabilityOfFailure)
      println(s"Frequency: $frequency, Error: $error Probability of failure: $probabilityOfFailure")
      for (i <- itemBatches.indices) {
        model.process(itemBatches(i))
        model.computeOutput().foreach(pair => println(pair))
        println("=============")
        Thread.sleep(1000L)
      }
  }

}
