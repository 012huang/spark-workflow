package datapreprocessing.columnselection

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vectors,Vector}
import org.apache.spark.mllib.stat.Statistics
import scala.collection.mutable.HashMap
import datapreprocessing.util

class RegressionSelection(
      targetCol:Int,
      columnNum:Int = 0,
      changeMethod:String,
      var corrThreshold:Double = 0,
      var colnumNumLimit:Int = Int.MaxValue
      ) extends ColumnSelection {
  
  /**
   * @corrThreshold 相关系数阈值
   * @initialSet 初始符合阈值的字段集合
   * @increaseSet 动态增长的字段集合
   * @bestSet 最优字段集合
   * 
   * */
  private var initialSet:Array[(Int,Double)] = Array()
  private var increaseSet:Array[Int] = Array()
  private var bestSet:Array[Int] = Array()
  private var evaluateParams = new HashMap[String,String]()
  private var colNumUnderThreshold:Int = 0
  
  def setEvaluateParam(params:HashMap[String,String]){
    evaluateParams = params.filter{
      case(key,value) =>
        key.split(".").contains("evaluate")
      } 
  }
  
  /**
   * 初始化参数， 并获得初始集合
   * @param dataSet 数据集
   * */
  private def initial(dataSet:RDD[Vector]) {
    colnumNumLimit = math.min(colnumNumLimit,2)
    require(columnNum > targetCol,"输入的目标列在数据中不存在,"+"数据列宽:" + columnNum +"< 目标列:" + targetCol)
    val corrMatrix = Statistics.corr(dataSet)
    val row = corrMatrix.numRows
    val corrArr = corrMatrix.toArray
    var count = 0
    while(!createInitialSet(corrArr,row)){
      count += 1
      if(count > 10) corrThreshold = 0
    }
    initialSet = initialSet.sortBy(_._2)
  } 
  
  private def createInitialSet(corrArr:Array[Double],row:Int):Boolean = {
    val base = row * targetCol
    var flag = true
    colNumUnderThreshold = 0
    val tempArr = for(i<- 0 until row if math.abs(corrArr(base + i)).toString != "NaN") yield {
      if (math.abs(corrArr(base + i)) >= corrThreshold) colNumUnderThreshold += 1
      (i, math.abs(corrArr(base + i)))
    }
    if(colNumUnderThreshold < colnumNumLimit) {
      val oldThreshold = computerCorrThreshold
      println("与目标列相关性高与" + oldThreshold + "的输入列个数小于:" + colnumNumLimit + " ,现调整为:" + corrThreshold +"并重新计算！")
      flag = false
    }   
    else {
      println("初始化列数为:"+ tempArr.size)
      initialSet = tempArr.toArray
    }    
    flag
  }
  
  /**
   * 创建字段变更方法
   * */
  private def createChangeMethod():ColumnChange = {
    require(!initialSet.isEmpty)
    changeMethod match {
      case "forward" => new ForwardSelection(initialSet.map(_._1),colnumNumLimit,colNumUnderThreshold)
      case "backward" => new BackwardSelection(initialSet.map(_._1),colnumNumLimit,colNumUnderThreshold)
      case _ => throw new Exception("字段变更方法输入错误，不存在" + changeMethod + "方法！！")
    }
    
  }
  
  /**重新计算阈值*/
  private def computerCorrThreshold():Double = {
    val oldThreshold = corrThreshold
    if(corrThreshold > 0.1) corrThreshold -= 0.1
    else corrThreshold /= 10
    oldThreshold
  }
  
  
  private def createColumnCheck():CheckSelectionMethod = {
    val splitPoint = util.numFromParams(evaluateParams, "column.selection.evaluate.datasplit", 0.8)
    new CheckMethodByModel(targetCol,increaseSet,splitPoint)
  }
  
  private def columnCheck(dataSet:RDD[Vector],checkMethod:CheckSelectionMethod):Double = {
    checkMethod.runCheck(dataSet)    
  }
  
  
  
  def selection(dataSet:RDD[Array[String]]):Array[Int] = {
    val vdata = util.preData(dataSet)
    initial(vdata)
    val method = createChangeMethod
    var variance = Double.MaxValue
    while(method.hashNext) {
      increaseSet = method.changeMethod
      println("选择集合为: " + increaseSet.mkString(","))
      val currVariance = columnCheck(vdata,createColumnCheck)
      println("方差为:" + currVariance)
      if(currVariance < variance) {
        variance = currVariance
        bestSet = increaseSet
      }
    }  
    println("最优集合为: " + bestSet.mkString(","))
    bestSet
  }
}


/**
 * 字段向前选择算法
 * */
class ForwardSelection(
      columnSet:Array[Int],
      lowLimitim:Int,
      highLimitim:Int
      ) extends ColumnChange {
  
  require(lowLimitim < columnSet.length)
  
  var increaseSet:Array[Int] = Array()
  var count = 0
  
  def hashNext:Boolean = {
    !columnSet.isEmpty && increaseSet.length + 1 < columnSet.length && increaseSet.length < highLimitim
  }  
  
  def changeMethod:Array[Int] = {
    if(count == 0) {
      increaseSet = columnSet.drop(columnSet.length - lowLimitim)
      count += 1
    } else {
      increaseSet = columnSet.drop(columnSet.length - (increaseSet.length + 1))
      count += 1
    }
    increaseSet
  } 
  
}

/**
 * 字段向后删除算法
 * */

class BackwardSelection(
      columnSet:Array[Int],
      lowLimitim:Int,
      highLimitim:Int
      ) extends ColumnChange {
  
  require(lowLimitim < columnSet.length)
  
  var count = 0
  
  var increaseSet:Array[Int] = columnSet
  
  def hashNext:Boolean = {
    !columnSet.isEmpty && increaseSet.length > lowLimitim 
  }  
  
  def changeMethod:Array[Int] = {
    if(count == 0) {
      increaseSet = columnSet.drop(columnSet.length - highLimitim)
      count += 1
    } else {
      increaseSet = columnSet.drop(columnSet.length - increaseSet.length + 1)
      count += 1
    }
    increaseSet
  } 
  
}
