package lossycounting

import model.Item
import unitspec.UnitSpec
import utils.Utils._
import testutils.SomeUtils._

class LossyCountingModelSpec extends UnitSpec{
  val frequency = 0.2
  val error = 0.1 * frequency

  "Lossy Counting" should "count correct values per batch" in {
    val lossyCounting = new LossyCountingModel[String](frequency, error)

    println("All items with true frequency > 20% will be output")
    println("All items between 18%-20% will be output and false positives")
    println("No element will be print with frequency less than 18%")

    val window0 = List.concat(create(19, Item.Red), create(11, Item.Blue), create(10, Item.Yellow), create(10, Item.Brown), create(0, Item.Green))

    val step0 = lossyCounting.process(window0)
    val step0Output = step0.computeOutput()

    // red freq = 18/50 = 0.36
    // blue freq = 10/50 = 0.2,
    // yellow freq = 9/50 = 0.18
    // brown freq = 9/50 = 0.18
    assert(step0Output.length == 4)

    assertColourAndCount(step0Output(0), Item.Red.toString, 18)
    assertColourAndCount(step0Output(1), Item.Blue.toString, 10)
    assertColourAndCount(step0Output(2), Item.Yellow.toString, 9)
    assertColourAndCount(step0Output(3), Item.Brown.toString, 9)

    val window1 = List.concat(create(30, Item.Red), create(10, Item.Blue), create(10, Item.Yellow))
    val step1 = lossyCounting.process(window1)
    val step1Output = step1.computeOutput()

    //red freq = 47 / 100
    //blue freq = 19 / 100
    //yellow freq = 18 / 100
    //brown freq = 8/100
    assert(step1Output.length === 3)
    assertColourAndCount(step1Output(0), Item.Red.toString, 47)
    assertColourAndCount(step1Output(1), Item.Blue.toString, 19)
    assertColourAndCount(step1Output(2), Item.Yellow.toString, 18)

    val window2 = List.concat(create(30, Item.Red), create(10, Item.Blue), create(0, Item.Yellow), create(5, Item.Brown), create(5, Item.Green))
    val step2 = lossyCounting.process(window2)
    val step2Output = step2.computeOutput()

    //red freq = 76 / 150 = 50%
    // blue freq = 28 / 150 = 18.6%
    // yellow freq = 17/150 = 11.3%
    // brown freq = 12 / 150 = 8%

    assert(step2Output.length === 2)
    assertColourAndCount(step2Output(0), Item.Red.toString, 76)
    assertColourAndCount(step2Output(1), Item.Blue.toString, 28)

    val window3 = List.concat(create(40, Item.Red), create(10, Item.Blue))
    val step3 = lossyCounting.process(window3)
    val step3Output = step3.computeOutput()

    //red freq = 115/200 = 57.5%
    //blue freq = 37/200 = 18.5%
    // yellow freq = 18 / 200 = 9.5%
    //brown freq = 11/200 = 5.5%

    assert(step3Output.length === 2)
    assertColourAndCount(step3Output(0), Item.Red.toString, 115)
    assertColourAndCount(step3Output(1), Item.Blue.toString, 37)

    val window4 = List.concat(create(40, Item.Red), create(10, Item.Blue))

    val step4 = lossyCounting.process(window4)
    val step4Output = step4.computeOutput()

    //red freq = 154 / 250 = 61.6%
    //blue freq = 46/250 = 18.4%
    //yellow freq = 17/250 = 6.8%
    //brown freq = 10 / 250 = 4%

    assert(step4Output.length === 2)
    assertColourAndCount(step4Output(0), Item.Red.toString, 154)
    assertColourAndCount(step4Output(1), Item.Blue.toString, 46)
  }

}
