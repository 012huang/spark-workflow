package datapreprocessing.columnselection

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vectors,Vector}



trait ColumnSelection {
  
  /**
   * 字段列分析接口，并返回所选择的的列id
   * */
  def selection(dataSet:RDD[Array[String]]):Array[Int]

}



trait CheckSelectionMethod {
  
  def runCheck(data:RDD[Vector]):Double
  
  
}


/**
 * 字段变换接口
 * */
trait ColumnChange {
  
  def hashNext:Boolean
  
  def changeMethod():Array[Int]
  
}

