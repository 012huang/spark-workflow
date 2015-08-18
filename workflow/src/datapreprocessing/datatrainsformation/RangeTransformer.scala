package datapreprocessing.datatrainsformation

import org.apache.spark.mllib.linalg.{Vector,Vectors}

class RangeTransformer(
      max:Array[Double],
      min:Array[Double],
      newMax:Double,
      newMin:Double
      ) extends VectorTransformer{
  
  
  lazy val diff:Vector = {
    val di = for(i<- 0 until max.size) yield {
      val temp = max(i) - min(i)
      if(temp == 0.0) 1 else temp
    }
    Vectors.dense(di.toArray)
  }
  
  lazy val newDiff:Double = newMax - newMin
  
  
  def transformer(value:Vector):Vector = {
    val newValue = for(i<- 0 until value.size) yield {
      (value(i) - min(i)) / diff(i) * (newDiff) + newMin
    }
    Vectors.dense(newValue.toArray)
  }
  

}
