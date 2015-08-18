package datapreprocessing.datatrainsformation

import scala.collection.mutable.HashMap
import org.apache.spark.rdd.RDD
import datapreprocessing.preAnalysis
import datapreprocessing.util

class TransformerManage(params:HashMap[String,String]) {
  
  
  def createTransformer(statistics:preAnalysis):VectorTransformer = {
    
    val method = params.get("transform.value.method").get
    method match {
      case "range" => 
        val newMax = util.numFromParams(params, "transform.value.params.1", 1)
        val newMin = util.numFromParams(params, "transform.value.params.2", 0)
        new RangeTransformer(statistics.max.toArray,statistics.min.toArray,newMax,newMin)
      case "z" => 
        new ZTrainsformer(statistics.means.toArray,statistics.variance.toArray)
      case "fractional" => 
        new FractionalTrainsformer(statistics.max.toArray,statistics.min.toArray)
      case _ => throw new Exception("变换方法输入错误，没有 " + method +" 方法")
    }
  }
  
  
  def runTransform(dataSet:RDD[Array[String]],statistics:preAnalysis):RDD[Array[String]] = {
    val vData = util.preData(dataSet)
    println("开始数据变换，变换方法为" + params.get("transform.value.method").get)
    val result = createTransformer(statistics).transformer(vData)
    util.vectorToString(result)
  }
  
  
 

}
