package stickysampling

import org.scalatest.mock.MockitoSugar
import unitspec.UnitSpec

class SamplingRateRetrieverSpec extends UnitSpec with MockitoSugar {

  "Sampling Rate Retriever" should "pick the correct rate" in {
    assert(SamplingRateRetriever.deriveSamplingRate(1, 20) === 1)
    assert(SamplingRateRetriever.deriveSamplingRate(5, 20) === 1)
    assert(SamplingRateRetriever.deriveSamplingRate(20, 20) === 1)

    assert(SamplingRateRetriever.deriveSamplingRate(21, 20) === 2)
    assert(SamplingRateRetriever.deriveSamplingRate(50, 20) === 2)

    assert(SamplingRateRetriever.deriveSamplingRate(61, 20) === 4)
    assert(SamplingRateRetriever.deriveSamplingRate(139, 20) === 4)
    assert(SamplingRateRetriever.deriveSamplingRate(140, 20) === 4)

    assert(SamplingRateRetriever.deriveSamplingRate(141, 20) === 8)
    assert(SamplingRateRetriever.deriveSamplingRate(299, 20) === 8)
  }

}
