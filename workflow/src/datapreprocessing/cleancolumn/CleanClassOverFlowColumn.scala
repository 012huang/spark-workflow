package datapreprocessing.cleancolumn

import datapreprocessing.preAnalysis
import org.apache.spark.rdd.RDD

class CleanClassOverFlowColumn(
      judgeCondition:Double = 0.7,
      judgeDataType:Int = 50
      ) extends CleanColumn{
  
  require(setJudgeCondition && setJudgeDataType ,"参数输入有误: "+
      "clean.column.void.condition:"+ judgeCondition+" 应属于[0.2,0.7]之间"+
      "categorical.judge:"+judgeDataType+" 应属于[2,1000]之间" +
      "不符合输入规范!")

  
  def setJudgeCondition():Boolean = {
    if(judgeCondition > 0.2 && judgeCondition < 1){  
    	true
    } else false
  }
  
  def setJudgeDataType():Boolean = {
    if(judgeDataType > 2 && judgeDataType < 1000){
        true      
    } else false
  }
  
  def runClean(statistics:preAnalysis, analysisData:RDD[Array[String]]):Array[Int] = {
    excludeMultiClass(statistics)
  }

  def excludeMultiClass(statistics:preAnalysis): Array[Int] = {
      val totalCnt = statistics.count
      val uniqueNum = statistics.uniqueNumCount
	  require(totalCnt > 0 && uniqueNum.size > 0, "数据记录数与字段数必须大于 0条,")
	  val n = uniqueNum.size
	  val col = for (i <- 0 until n) yield {
		  if (totalCnt > 500) {
			  if (uniqueNum(i) > judgeDataType && uniqueNum(i) < totalCnt * judgeCondition) -1 else i
		  } else i
	  }
	  col.toArray
	}
  
}
