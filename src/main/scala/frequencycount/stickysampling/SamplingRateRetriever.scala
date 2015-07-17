package frequencycount.stickysampling

object SamplingRateRetriever {


  /**
   * Returns the rate that corresponds to this item based on the number of items that have been processed so far
   * @param itemSeqId A sequential id (past items + 1)
   * @param t
   * @return
   */
  def deriveSamplingRate(itemSeqId: Long, t: Double): Int = {

    var currRate = 1
    var sum = 0.0
    while((currRate*t + sum) < itemSeqId){
      sum += (currRate*t)
      currRate *= 2
    }
    currRate
  }

}
