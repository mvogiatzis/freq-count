package stickysampling

case class ItemCountRate(itemId: Long, rate: Int, currentCount: Long) {

  override def toString(): String ={
    "Count= " + currentCount + " rate=" + rate + " itemId= "+itemId
  }

}
