package datapreprocessing.cleanvalue

import org.apache.spark.rdd.RDD

trait ValueDelete extends Serializable{
  
  var deteleValue:Array[String] = Array()
  var deleteCol:Array[Int] = Array()
  
  def setDeteleValue(dv:Array[String]) = deteleValue = dv
  def setDeleteCol(dc:Array[Int]) = deleteCol = dc
  
  /**
   * 删除确定值
   * */
  def deleteValue(dataSet:Array[String]):Boolean = {
    var flag = true
    for(i<- deleteCol) {
      if(dataSet(i) == deteleValue(i)) flag = false
    }
    flag
  }
  
  def deleteValue(dataSet:RDD[Array[String]]):RDD[Array[String]] = {
    if(!deteleValue.isEmpty){
      dataSet.filter(deleteValue)      
    } else dataSet
  }
  
  /**
   * 根据条件删除值
   * */
  def deleteCondititon(dataSet:Array[String], f:String => Boolean):Boolean = {
    var flag = true
    for(i<- deleteCol) {
      if(f(dataSet(i))) flag = false
    }
    flag     
  }

  def deleteValueWithCondititon(dataSet:RDD[Array[String]], f:String => Boolean):RDD[Array[String]] = {
    dataSet.filter(deleteCondititon(_, f))
  }

}
