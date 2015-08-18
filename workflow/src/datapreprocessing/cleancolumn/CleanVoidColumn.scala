package datapreprocessing.cleancolumn

import datapreprocessing.preAnalysis
import org.apache.spark.rdd.RDD

class CleanVoidColumn(
      judgeCondition:Double = 0.7 ,
      judgeDataType:Int = 50
      ) extends CleanColumn {  
  
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

  
  def runClean(statistics:preAnalysis,analysisData:RDD[Array[String]]):Array[Int] = {
      excludeVoidCol(statistics)
  }
  

	/**
	 * 排除空值过多的字段，返回保留字段id,排除字段为 -1
	 */
  def excludeVoidCol(statistics:preAnalysis): Array[Int] = {
      val totalCnt = statistics.count
      val notNum = statistics.notNumCount
      val uniqueNum = statistics.uniqueNumCount

	  require(totalCnt > 0 && uniqueNum.size > 0, "数据记录数与字段数必须大于 0条,")
		val n = notNum.size
		require(n == uniqueNum.size, "传入字段数必须相同")
		val precent = notNum.toArray.map(_ / totalCnt.toDouble)
		val col = for (i <- 0 until n) yield {
			if (totalCnt > 500) {
				if (precent(i) > judgeCondition && uniqueNum(i) > judgeDataType) -1 else i
			} else {
				if (precent(i) > judgeCondition && uniqueNum(i) > totalCnt * judgeCondition) -1 else i
			}
		}
		col.toArray
	}
  

}
