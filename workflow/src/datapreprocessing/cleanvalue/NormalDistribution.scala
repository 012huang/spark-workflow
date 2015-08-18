package datapreprocessing.cleanvalue

import org.apache.spark.mllib.linalg.{Vectors,Vector}
import org.apache.spark.rdd.RDD
import datapreprocessing.util

class NormalDistribution(
      threshold:Double,
      method:String,
      dealCol:Array[Int],
      mean:Array[Double],
      variance:Array[Double]
      ) {
  
  require(!dealCol.isEmpty && !mean.isEmpty && !variance.isEmpty,"处理字段，平均值，方差集合都不能为空")
  
  private def computerEdgeValue:Array[Double] = {
    if(!dealCol.isEmpty){
      for(id <- dealCol) yield
      (threshold * variance(id)) + mean(id)      
    } else Array.empty[Double]
  }
  
  def runOption(dataSet:RDD[Array[String]]):RDD[Array[String]] = {
    println("选择的异常值处理方法是:" + method)
    method match {
      case "edge_value_replace" => edgeReplace(dataSet)
      case "mean_value_replace" => meanReplace(dataSet)
      case "delete_value" => deleteValue(dataSet)
      case _ => 
        throw new Exception("异常值过滤方法选择错误,没有: " +method+ " 方法")
    }
  }
  
  
  private def replacer(dataSet:RDD[Array[String]], edgeValue:Array[Double], replaceValue:Array[String]):RDD[Array[String]] = {
      val vr = new ValueReplace{
           def valueReplace(value:Array[String],edgeValue:Array[Double]):Array[String] = {
              for(i<- replaceCol) {
                val currValue = util.dealNumber(value(i))
            	if(math.abs(currValue) > edgeValue(i)) value(i) = replaceValue(i)    
              }
              value
           }
           
           def valueReplace(dataSet:RDD[Array[String]],edgeValue:Array[Double]):RDD[Array[String]] = {
             dataSet.map(valueReplace(_,edgeValue))
           }           
      }
      vr.setReplaceColumn(dealCol)
      vr.setReplaceValue(replaceValue)
      vr.valueReplace(dataSet, edgeValue)
  }
    
  private def edgeReplace(dataSet:RDD[Array[String]]):RDD[Array[String]] = {
    if(mean.size > 0 && variance.size > 0){
      val thresholdValue = computerEdgeValue
      replacer(dataSet,thresholdValue,thresholdValue.map(_.toString))
    } else {
      throw new Exception("平均值与方差集合为空，无法进行异常值过滤")
    }
  }
  
  private def meanReplace(dataSet:RDD[Array[String]]):RDD[Array[String]] = {
    if(mean.size > 0) {
      val thresholdValue = computerEdgeValue
      replacer(dataSet,thresholdValue,mean.map(_.toString))
      
    } else {
      throw new Exception("平均值与方差集合为空，无法进行异常值过滤")     
    }
  }
  
  private def deleteValue(dataSet:RDD[Array[String]]):RDD[Array[String]] = {
    val thresholdValue = computerEdgeValue
    val deleter = new ValueDelete{
      def deleteValue(value:Array[String], edgeValue:Array[Double]):Boolean = {
        var flag = true
        for(i<- deleteCol) {
           val currValue = util.dealNumber(value(i))
           if(math.abs(currValue) > edgeValue(i)) flag = false    
        }
        flag
      }
      
      def deleteValue(value:RDD[Array[String]], edgeValue:Array[Double]):RDD[Array[String]] = {
        value.filter(deleteValue(_,edgeValue))
      }
    }
    deleter.setDeleteCol(dealCol)
    deleter.deleteValue(dataSet, thresholdValue)
  }

}
