package datapreprocessing.datatrainsformation

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vector,Vectors}


trait VectorTransformer extends Serializable{
  
  def transformer(value:Vector):Vector
  
  /**
   * 变换接口
   **/
  
  def transformer(value:RDD[Vector]):RDD[Vector] = {
    value.map(transformer(_))
  }
  

}
