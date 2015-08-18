package datapreprocessing.cleancolumn

import scala.reflect.ClassTag
import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD
import datapreprocessing.preAnalysis
import datapreprocessing.util

class CleanColumnManage(params:HashMap[String,String]) {
  
  /**
   * 字段清洗数据执行数组
   * */
  private var cleanStep:Array[CleanColumn] = Array[CleanColumn]()
  private val cj = util.numFromParams(params, "categorical.judge", 50)
  private val columnNum = params.get("column.num").get.toInt
  
  
  /**
   * 执行步奏长度
   * */
  def stepLimit = cleanStep.size
  
  private def createVoidColumn():CleanColumn = {
    val ccvc = util.numFromParams(params,"clean.column.void.condition",0.70)   
    println("创建CleanVoidColumn "+"clean.column.void.condition: "+ ccvc)    
    new CleanVoidColumn(ccvc,cj.toInt)    
  }
  
  private def createSingleColumn():CleanColumn = {
    val ccsc = util.numFromParams(params, "clean.column.single.condition", 0.99)
    val classId = util.arrFromParamsInt(params, "catecategorical.col").map(_.toInt)
    println("创建CleanSingleColumn "+"clean.column.void.condition: "+ccsc)
    new CleanSingleColumn(ccsc,classId)
  }
  
  private def createClassOverFlowColumn():CleanColumn = {
    val ccsc = util.numFromParams(params, "clean.column.classoverflow.condition", 0.5)
    println("创建CleanClassOverFlowColumn: "+"clean.column.void.condition: "+ccsc) 
    new CleanClassOverFlowColumn(ccsc,cj.toInt)
  }
  
  private def createCorrelationFilter():CorrelationAnalysis = {
    val vt = util.numFromParams(params, "clean.column.correlation.threshold", 0.3)
    println("创建CleanCorrelationFilter: "+"clean.column.correlation.threshold: "+ vt) 
    val tar = params.get("target.col").get.toInt
    new CorrelationAnalysis(vt,tar)
  }

  
  private def createStep(){
    val step = ArrayBuffer[CleanColumn]()
    if(isUse(params.getOrElse("clean.column.void", "false"))) step += createVoidColumn
    if(isUse(params.getOrElse("clean.column.single", "false"))) step += createSingleColumn
    if(isUse(params.getOrElse("clean.column.classoverflow", "false"))) step += createClassOverFlowColumn
    if(isUse(params.getOrElse("clean.column.correlation", "false"))) step += createCorrelationFilter
    cleanStep = step.toArray
  }
  
  def runClean(statistics:preAnalysis,analysisData:RDD[Array[String]]):Array[Int] = {
    createStep
    val result:Array[Int] = Array.fill(columnNum)(0)
    if(!cleanStep.isEmpty) {
      println("开始字段清洗第一步操作")
      for(step <- cleanStep) {
        val stepResult = step.runClean(statistics,analysisData)
        var i= 0 
        while(i < columnNum && !stepResult.isEmpty) {
          if(stepResult(i) != -1 && result(i) != -1) result(i) = i
          else result(i) = -1
          i += 1
        }
        println("字段过滤: " + stepResult.mkString("[",",","]"))
      } 
    }else throw new Exception("字段清洗步奏为空，不执行字段清洗操作！")   
    result.filter(_ != -1)
  }
  
  
  private lazy val isUse = (key:String) => key match {
    case "true" => true
    case "false" => false
    case _ => 
      println("字段清洗Boolean类型参数:"+ key +"输入错误，输入默认类型False")
      false
  }

}
