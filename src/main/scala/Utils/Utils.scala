package utils

import frequencycount.Item

import scala.collection.immutable.IndexedSeq

object Utils {

  def create(elements: Int, item: Item.Item): List[String] = {
    val seq: IndexedSeq[String] = for (i <- 1 to elements) yield {
      item.toString
    }
    seq.toList
  }

}
