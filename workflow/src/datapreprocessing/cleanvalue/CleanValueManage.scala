package datapreprocessing.cleanvalue

import scala.collection.mutable.HashMap
import org.apache.spark.rdd.RDD
import datapreprocessing.preAnalysis
import datapreprocessing.util

class CleanValueManage(params:HashMap[String,String]) {
  
  private def createVoidValueReplace(): VoidValueReplace = {
    new VoidValueReplace(params)
  }
  
  private def createOutlierFilter(mean:Array[Double],variance:Array[Double]):NormalDistribution = {
    val vot = util.numFromParams(params, "clean.value.outlier.threshold", 3)
    val vom = params.getOrElse("clean.value.outlier.method", "edge_value_replace")
    val voaa = params.getOrElse("clean.value.outlier.addanalysis", "false").toBoolean
    val voat = params.getOrElse("clean.value.outlier.addtarget", "false").toBoolean
    val tarcol = params.get("target.col").get.toInt
    val colNum = params.get("column.num").get.toInt
    val dc = (0 until mean.length).toArray 
    val replaceColumn = (voaa ,voat) match {
      case (true,true) => dc
      case (true,false) => Array(tarcol)
      case (false, true) => dc.filter(_ != tarcol)
      case (false, false) => Array[Int]()
    }
    
    new NormalDistribution(vot,vom,dc,mean,variance)
  }
  
  
  
  def runCleanValue(dataSet:RDD[Array[String]], statistics:preAnalysis):RDD[Array[String]] = {
    var data = dataSet
    var colNum = params.get("column.num").get.toInt
    var col = (0 until colNum).toArray 
    val vv = params.getOrElse("clean.value.void", "false").toBoolean
    val vo = params.getOrElse("clean.value.outlier", "false").toBoolean    
    if(vv) {
      println("开始值替换功能")
      data = createVoidValueReplace.runCleanValue(data, statistics)
    }
    if(vo) {
      println("开始异常值过滤")
      data = createOutlierFilter(statistics.means.toArray,statistics.variance.toArray).runOption(data)
    }
    data
  }

}
