package datapreprocessing.cleancolumn

import org.apache.spark.rdd.RDD
import datapreprocessing.util
import org.apache.spark.mllib.stat.Statistics
import datapreprocessing.preAnalysis

class CorrelationAnalysis(
      corrThreshold:Double,
      targetCol:Int
      ) extends CleanColumn {
  
  private def corrMatrixDeal(corrArr:Array[Double], row:Int):Array[Int] = {
    val base = row * targetCol
    val tempArr = for(i<- 0 until row) yield {
      if(math.abs(corrArr(base + i)) > corrThreshold) i
      else -1
    }
    tempArr.toArray   
  }
  
  def runClean(statistics:preAnalysis,analysisData:RDD[Array[String]]):Array[Int] = {
    val vData = util.preData(analysisData)
    val m = Statistics.corr(vData)
    corrMatrixDeal(m.toArray,m.numRows)
  }

}
