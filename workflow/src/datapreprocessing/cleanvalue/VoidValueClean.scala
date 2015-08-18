package datapreprocessing.cleanvalue

import org.apache.spark.rdd.RDD
import datapreprocessing.util
import org.apache.spark.mllib.linalg.{Vectors,Vector}
import datapreprocessing.preAnalysis


/**
 * 值替换基类
 * @params oldValue 被替换值
 * @params replaceValue 替换新值
 * @params replaceCol 操作的列
 * */
class ValueReplace extends Serializable{
  
  protected var oldValue:Array[String] = _
  protected var replaceValue:Array[String] = _ 
  protected var replaceCol:Array[Int] = _
  
  def setVoidValue(vd :Array[String]) = oldValue = vd
  def setReplaceValue(rv:Array[String]) = replaceValue = rv
  def setReplaceColumn(rc:Array[Int]) = replaceCol = rc
  
  def valueReplace(value:Array[String]):Array[String] = {
    for(i<- replaceCol) 
      if(value(i) == oldValue(i)) value(i) = replaceValue(i)
    value
  }
  
  /**
   * 值替换接口
   **/
  
  def valueReplaceCondition(value:Array[String],f:String => Boolean):Array[String] = {
    for(i<- replaceCol) 
      if(f(value(i))) value(i) = replaceValue(i)
    value
    
  }
  
  
  def valueReplace(value:RDD[Array[String]]):RDD[Array[String]] = {
    value.map(valueReplace(_))
  }
  
  def valueReplaceCondition(value:RDD[Array[String]],f:String => Boolean):RDD[Array[String]] = {
    value.map(valueReplaceCondition(_,f))
  }
  

}


trait ReplaceOption extends ValueReplace{
  
   def runReplace(value:RDD[Array[String]],statistics:preAnalysis):RDD[Array[String]] 
 
}


class VoidValueReplaceMethod1 extends ReplaceOption {
  
  private var categoricalColId:Array[Int] = Array()
  
  
  def setCategoricalColId(classId:Array[Int]) =  categoricalColId = classId

  
  private def initial(){
    if(!categoricalColId.isEmpty && !replaceCol.isEmpty){
    	categoricalColId = for(elem <- categoricalColId if replaceCol.contains(elem)) yield elem      
    } else throw new Exception("替换字段或分类字段为空，请先输入替换字段")
  }
  
  private def computerMode(value:RDD[Array[String]]){
    if(!categoricalColId.isEmpty){
      val mode = util.classValueCount(value, categoricalColId)
      for((id,value,num) <- mode) {
        if(!value.equals("") && num != 1) replaceValue(id) = value 
      }
    }
  }
  
  private def computerMeans(mn:Vector){
    if(!categoricalColId.isEmpty){
      replaceValue = mn.toArray.map(_.toString)
    } else {
      for(i <- 0 until mn.size if !categoricalColId.contains(i)) {
        replaceValue(i) = mn(i).toString
      }
    }
  }
  
 def runReplace(value:RDD[Array[String]],statistics:preAnalysis):RDD[Array[String]] = {
    val n = value.first.length
    replaceValue = Array.fill(n)("0")
    initial()
    computerMode(value)
    computerMeans(statistics.means)
    valueReplace(value)
  }
  
}
