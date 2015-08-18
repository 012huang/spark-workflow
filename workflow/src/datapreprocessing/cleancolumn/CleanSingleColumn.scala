package datapreprocessing.cleancolumn

import datapreprocessing.preAnalysis
import org.apache.spark.rdd.RDD
import scala.collection.mutable.HashMap
import datapreprocessing.util
/**
 * 单值字段清洗
 * 字段中单值个数超过记录总数的99%
 * */

class CleanSingleColumn(
      judgeCondition:Double = 0.99,
      categoricalColId:Array[Int] = Array()
      ) extends CleanColumn{
  
  require(setJudgeCondition  ,"参数输入有误: "+
      "clean.column.void.condition:"+ judgeCondition+" 应属于[0.2,0.7]之间"+
      "不符合输入规范!")
  
  def setJudgeCondition():Boolean = {
    if(judgeCondition > 0.2 && judgeCondition < 1){
    	true
    } else false
  }
    
  /**
   * 运行清洗接口
   * */
  def runClean(statistics:preAnalysis,analysisData:RDD[Array[String]]):Array[Int] = {
    if(!categoricalColId.isEmpty){
      val valueCount = util.classValueCount(analysisData,categoricalColId)
      excludeOneValueCol(statistics.count,statistics.uniqueNumCount,valueCount)
    } else Array.empty[Int]
  }
  
  def excludeOneValueCol(totalCnt: Long, uniqueNum: Array[Long], valueCnt: Array[(Int, String, Long)]): Array[Int] = {
	val precent = valueCnt.map { case (colNum, key, value) => (colNum, value.toDouble / totalCnt) }
	var col = { for (i <- 0 until uniqueNum.size) yield i }.toArray
	for (elem <- precent) {
		if (elem._2 > judgeCondition) col(elem._1) = -1
	}
	col
  }
}
