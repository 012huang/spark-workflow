package datapreprocessing.datatrainsformation

import org.apache.spark.mllib.linalg.{Vector,Vectors}

class ZTrainsformer(
      means:Array[Double],
      variance:Array[Double]
      ) extends VectorTransformer {
  
  private val std:Array[Double] = variance.map(math.sqrt(_))
  

  
  def transformer(value:Vector):Vector = {
    val newValue = for(i <- 0 until value.size) yield {
      (value(i) - means(i)) / std(i)
    } 
    Vectors.dense(newValue.toArray)
  }

}
