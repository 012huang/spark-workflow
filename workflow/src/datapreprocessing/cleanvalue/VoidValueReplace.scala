package datapreprocessing.cleanvalue

import scala.collection.mutable.HashMap
import datapreprocessing.util
import org.apache.spark.rdd.RDD
import datapreprocessing.preAnalysis

class VoidValueReplace(params:HashMap[String,String]) {
  
  var replaceColumn:Array[Int] = Array()
  var colNum:Int = params.get("column.num").get.toInt
  
  val addTarget:Boolean = params.getOrElse("clean.value.void.addtarget", "false").toBoolean
  val addAnalysis:Boolean = params.getOrElse("clean.value.void.addanalysis", "false").toBoolean
  
  private def createInputCol() {
    val initialCol = (0 until colNum).toArray
    val targetCol = params.get("target.col").get.toInt
    replaceColumn = (addTarget ,addAnalysis) match {
      case (true,true) => initialCol
      case (true,false) => Array(targetCol)
      case (false, true) => initialCol.filter(_ != targetCol)
      case (false, false) => Array[Int]()
    }
  }
  
  
  private def createCleaner:ReplaceOption = {
    val cleaner = params.get("clean.value.void.method").get
    val vd = params.getOrElse("clean.value.void.string", "N/A")
    val classId = util.arrFromParamsInt(params, "catecategorical.col").map(_.toInt)
    cleaner match{
      case "method1" => 
        val method = new VoidValueReplaceMethod1
        method.setVoidValue(Array.fill(colNum)(vd))
        method.setReplaceColumn(replaceColumn)
        method.setCategoricalColId(classId)
        method
      case _ => 
        throw new Exception("值清洗方法选择错误，没有 "+cleaner+" 方法" )
    }
  }
  
  def runCleanValue(dataSet:RDD[Array[String]],statistics:preAnalysis):RDD[Array[String]] = {
	createInputCol()  
	if(replaceColumn.isEmpty) {
	  createCleaner.runReplace(dataSet, statistics)
	} else dataSet
  }

}
