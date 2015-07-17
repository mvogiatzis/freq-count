package frequencycount


trait FrequencyCount[T] {

  def process(dataWindow: List[T]): FrequencyCount[T]

  def computeOutput(): Array[(T, Int)]

}
