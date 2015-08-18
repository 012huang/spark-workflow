package datapreprocessing.columnselection

import scala.collection.mutable.HashMap
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vectors,Vector}
import datapreprocessing.util

class ColumnSelectionManage(params:HashMap[String,String]) {
  
  
  def createSelection:ColumnSelection = {
    val targetCol = params.get("target.col").get.toInt
    val columnNum = params.get("column.num").get.toInt
    val selectionMethodStr = params.getOrElse("column.selection.method", "forward") 
    val selectionMethod:ColumnSelection = selectionMethodStr match {
      case "forward" | "backward" =>
        val corr = util.numFromParams(params,"column.selection.correletion.threshold",0)
        val colLimit = util.numFromParams(params, "column.selection.num.limit", Int.MaxValue)
        val method = new RegressionSelection(targetCol,columnNum,selectionMethodStr,corr,colLimit.toInt)
        method.setEvaluateParam(params)
        method
              
      case "decisiontree" => 
        val depth = util.numFromParams(params, "column.selection.tree.deep", 6)
        val maxbins = util.numFromParams(params,"column.selection.tree.maxbins",100)
        val classCol = util.arrFromParamsInt(params, "catecategorical.col").map(_.toInt)
        val method = new DecisionTreeSelection(targetCol,columnNum,depth.toInt,maxbins.toInt,classCol)
        method
      case _ =>
        throw new Exception("字段选择方法输入错误:输入为"+selectionMethodStr+" 应为forward,backward,decisiontree中的一个")        
    }
    println("字段选择判定-选择的方法是:" + selectionMethodStr)
    selectionMethod
  }
  
  
  def runSelection(dataSet:RDD[Array[String]]):Array[Int] = {
    val selectionMethod = createSelection
    val result = if(selectionMethod != null) selectionMethod.selection(dataSet)
                 else Array[Int]()
    result
  }

}
