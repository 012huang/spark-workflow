package workflow.dataset

import scala.reflect._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkException
import workflow.mathanalysis.OnlineStatistica
import workflow.mathanalysis.util
import org.apache.spark.mllib.regression.LabeledPoint
import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import workflow.dataset.DataType._

class DataSet extends DataInfo {
  
     var data :RDD[Array[String]] = _   
     var columnTop:ColumnSet = _
   
     def this(data:RDD[Array[String]],columnTop:ColumnSet){
       this()
       this.data = data
       this.columnTop = columnTop
       this.dataRow = columnTop.length
     }
     
     def this(data:RDD[Array[String]]){
       this()
       this.data = data
       preAnalysis
     }

    def cache{
	  data.cache
	}
	
	def unpersist(blocking:Boolean){
	  data.unpersist(blocking)
	}
	
	def setData(newData:RDD[Array[String]]) = data = newData
	def getData = data
	
	def setColumnSet(newCol:ColumnSet) = columnTop = newCol
	def getColumnSet = columnTop
	
	/**
	 * 设置字段为输入角色
	 * */
	def setInputRole(inputName:Array[String])  {
	  if(!inputName.isEmpty) {
	    columnTop.setInputRole(inputName)
//	    println("配置输入列: "+inputName.mkString(",")+" 成功")
	  }
	  else println("输入字段集合为空值，设置输入角色失败！")
	}
	
	/**
	 * 设置字段为目标角色
	 * */
	def setTargetRole(targetName:String) {
	  if(targetName != null) {
	    columnTop.setTargetRole(targetName)
//	    println("配置目标列: "+targetName+" 成功")
	  }
	  else println("目标字段集合为空值，设置目标角色失败！")
	}
	
	/**
	 * 设置字段属性为分类类型
	 * */
	def setCategoricalColumn(columnName:Array[String]) = {
	  if(!columnName.isEmpty){
	    columnTop.setCategoricalColumn(columnName)
	  } else println("设置分类字段的集合为空！")	  
	}
	
	/**
	 * 设置字段属性为连续类型
	 * */
	def setContinuousColumn(columnName:Array[String]) = {
	  if(!columnName.isEmpty){
	    columnTop.setContinuousColumn(columnName)
	  } else println("设置连续字段的集合为空！")	  
	  
	}
	
	/**
	 * 设置字段属性为ID类型
	 * */
	def setIDColumn(columnName:Array[String]) = {
	  if(!columnName.isEmpty){
	    columnTop.setIDColumn(columnName)
	  } else println("设置ID字段的集合为空！")	  
	  
	}
	
	def setAllColumnToContinuous = columnTop.setAllColumnToContinuous
	
	
	def setAllColumnToInput = columnTop.setAllColumnToInput
	
    def setCategoricalFeaturesInfo(idWithMap:Array[(Int,HashMap[String,Int])]){
	  columnTop.setCategoricalFeaturesInfo(idWithMap)
	}	
	
	
	def getStatis:OnlineStatistica = {
	    data.aggregate(new OnlineStatistica)(
	        (agg, value) => agg.add(value),
	        (agg1, agg2) => agg1.merge(agg2))
	}
	
    def toVectorData:VectorData = {
	  val rddVect = util.preData(data)
	  val newCom = new VectorData(rddVect ,columnTop.clone)
	  newCom.dataColumn = dataColumn
	  newCom.dataRow = dataRow
	  newCom.dataName = dataName
      newCom.splitStr = splitStr
      newCom.logger = logger
	  newCom
	}
    
    def toLabeledData:LabeledData = {
      val len = columnTop.size
      val tempColumnTop = columnTop.clone
      var columnSet = tempColumnTop.getColumn
      var j = 0
      var inputColumns:ArrayBuffer[Column] = ArrayBuffer[Column]()
      var targetColumn:Column = null
      for(column<- columnSet if !column.isVoid) yield{
        if(column.isInput) {
          column.id = j
          inputColumns += column
          j +=1
        } 
        if(column.isTarget) {
          targetColumn = column
        }
      }
      targetColumn.id  = inputColumns.length
      val newColumn    = inputColumns += targetColumn
      tempColumnTop.changeAllColumn(newColumn.toArray)
	  val inputCol     = columnTop.getAllIdOfInput
	  val targetCol    = columnTop.getIdOfTarget
	  val newLabel     = if(!inputCol.isEmpty){
	    val rddLabel   = data.map(value 
	        =>{ 
	           val temp  = util.preData(value)
	           val label = util.vectorToLabel(temp, targetCol, inputCol)
	           label
	        } 
	      )
	      new LabeledData(rddLabel,tempColumnTop)	  
	  } else {
	    throw new Exception("没有目标列 ， 或没有输入列无法转换成标签数据！")
	  }
	  newLabel.dataColumn = dataColumn
	  newLabel.dataRow    = dataRow
	  newLabel.dataName   = dataName
      newLabel.splitStr   = splitStr	 
      newLabel.logger     = logger

	  newLabel
    }

    
/**
 * 合并列相同的记录的字段名
 * */	
	private def unionColumn(other:ColumnSet):Boolean = {
	  if(columnTop.size == other.size) {
	    var columnThis  = columnTop.getColumn
	    var columnOther = other.getColumn
	    var success = true
	    for(i<- 0 until columnTop.size) {
	     success = success && columnThis(i).merge(columnOther(i))
	    }
	    if(success) columnTop.changeAllColumn(columnThis)
	    success
	  }
	  else false
	}
/**
 * 合并两列的数据
 * */	
	private def unionRDD(other:RDD[Array[String]]):Boolean = {
	  var success = true
	  var result:RDD[Array[String]] = null
	  try{
	   result = data.union(other)
	   data = result
	  }catch{
	    case ex:SparkException => success = false
	  }
	  success
	}
/**
 * 合并两个数据集合
 **/	
	def union(other:DataSet) = {
	  if(unionColumn(other.columnTop)){
	     if(unionRDD(other.data)) println("数据集合并成功！")
	     else println("spark 出现合并数据集合失败！")
	  } else {
	    println("无法合并字段名，请检查要合并的两列字段名，存储类型是否相同！")
	  }
	}
	
	
	def filterValue(fun1:String => Boolean,
	                fun2:ColumnSet => Int){
	  val flag = fun2(columnTop)
	  data = data.filter(value => fun1(value(flag)))
	}
	
	/**
	 * 过滤字段
	 * */
	def filterColumn(stayColumn:Array[Int]):this.type = {
	  data = data.map(
	      value =>
	        for(elem<- stayColumn) yield value(elem)
	  )
	  columnTop = columnTop.filter(
	      col => 
	        stayColumn.contains(col.id)
	  ).refreshColumnId	  
	  this
	}
	
	/**
	 * 按条件过滤字段
	 * */
	def filterColumnWithCondition(f:Column => Boolean):this.type = {
	  columnTop.filter(f)
	  val stayColumn = this.columnTop.getAllId
	  data = data.map(
	      value =>
	        for(elem<- stayColumn) yield value(elem)
	  )
	  this.columnTop.refreshColumnId
	  this
	}
	
	

	private def getStoreType():HashMap[Int,StoreType.StoreType] = {
 
    if(data != null) {
      val tempData  = data.take(1000).tail      
      val columnNum = tempData(0).length      
      
      val flag = Array.fill(columnNum)(0)
      tempData.foreach{value =>
        for(i <- 0 until columnNum){
          try{
             if(value(i) != "" && value(i) != " ") value(i).toDouble
          }catch{
            case ex:NumberFormatException =>
            flag(i) = 1           
          }
        }
      }
      
      var columnStoreType = HashMap[Int,StoreType.StoreType]()
      for(i <- 0 until columnNum) {
        if(flag(i) == 0) columnStoreType(i) = StoreType.DoubleType 
        else columnStoreType(i) = StoreType.StringType
      }
      return columnStoreType
    } else throw new Exception("数据集为空，请检查数据源是否正确！")    

  } 
  
	
  def dataSplit(splitPoint :Array[Double]):Array[DataSet] = {
    val dataTemp = data.randomSplit(splitPoint, math.random.toLong)
    val afterSplit = for(i <- 0 until splitPoint.length) 
      yield new DataSet(dataTemp(i), columnTop)   
    afterSplit.toArray
  }	
  
  //目前只支持0 ，1 按比例分割
  def dataSplitWithLabel(splitPoint :Double):(DataSet,DataSet) = {
    val target = columnTop.getIdOfTarget
    val rightData = data.filter(row => row(target) == 0)
    val badData = data.filter(row => row(target) == 1)
    val rightTemp  = rightData.randomSplit(Array(splitPoint,1 - splitPoint), math.random.toLong)
    val rightTraining = rightTemp(0)
    val rightTest = rightTemp(1)
    val badTemp = badData.randomSplit(Array(splitPoint,1 - splitPoint), math.random.toLong)
    val badTraining = badTemp(0)
    val badTest = badTemp(1)
    def trainingData = (rightTraining ++ badTraining).sample(false, 1, math.random.toLong)
    def testData = (rightTest ++ badTest).sample(false, 1, math.random.toLong)
    (new DataSet(trainingData,columnTop),new DataSet(testData,columnTop))
    
  }
  
  /**
   * 对数据初步分析，构建数据集合
   * */
  def preAnalysis():this.type = {
    
    
    val status = getStatis
    val count = status.count
    
    if(count == 0) {
      throw new Exception("数据记录数为零，建议更换数据源在进行操作")
    }
    
    //获取初始统计量信息
    val columnNum = status.columnNum
    val columnName = data.first
    val max = status.max
    val min = status.min
    val uniqueValue = status.uniqueNumCount
    val notNum = status.notNumCount
    val mean = status.means
    val variance = status.variance
    val colStoreType = getStoreType()
    val avlidNum = for(i<- 0 until columnNum) yield{count - notNum(i)}
    
    //创建列信息
    val columnInfo = for(i<- 0 until columnNum) yield{
        val colTemp = new Column(i,columnName(i),Continuous,colStoreType(i))
        
        if(colTemp.storeType == StoreType.DoubleType) colTemp.isAvlidStatis = true    
        
        colTemp.setAvlidNum(avlidNum(i))
        colTemp.setMax(max(i))
        colTemp.setMin(min(i))
        colTemp.setMean(mean(i))
        colTemp.setVariance(variance(i))
        colTemp.setUniqueValueCnt(uniqueValue(i))
        colTemp
    }
    filterBank
    columnInfo(columnNum - 1).setRoleTarget
    //得到数据集合
    this.columnTop = new ColumnSet(columnInfo.toArray)
    this.dataRow = count
    this.dataColumn = columnNum
    this 
  }
  
  
  private def filterBank(){
    data = data.filter(f=>f.size == dataColumn)
  }

  override def clone():DataSet= {
    new DataSet(data,columnTop.clone)
  }	
  
}

object DataSet extends Serializable{
  
  
  
}
