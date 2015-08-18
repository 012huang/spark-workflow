package datapreprocessing.cleancolumn

import datapreprocessing.preAnalysis
import org.apache.spark.rdd.RDD

trait CleanColumn {
  
  
  /**
   * 执行字段清洗接口，返回保留字段ID
   * */
  def runClean(statistics:preAnalysis,analysisData:RDD[Array[String]]):Array[Int]

}
