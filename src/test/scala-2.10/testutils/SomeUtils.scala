package testutils

import unitspec.UnitSpec

object SomeUtils extends UnitSpec{

  def assertColourAndCount(output: (String, Int), colour: String, count: Int): Unit = {
    assert(output._1 === colour)
    assert(output._2 === count)
  }

}
