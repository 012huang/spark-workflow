package workflow.dataset

import DataType._
import StoreType._
import RoleType._
import scala.collection.mutable.HashMap

class Column(
      var id        :Int,
      var columnName:String,
      var dataType  :DataType = Continuous, 
      var storeType :StoreType = StringType,
      var Role      :Role = Input) extends Serializable {
  
  var isAvlidStatis = false
  
  private var AvlidNum               :Double = 0
  private var Mean                   :Double = 0
  private var Variance               :Double = 0
  private var Max                    :Double = 0
  private var Min                    :Double = 0
  private var UniqueValueCnt         :Long = 0
  private var CategoricalFeaturesInfo:HashMap[String,Int] = HashMap[String,Int]()
  private var CategoricalNum         :Int = 0
  
  def setAvlidNum(value:Double)      = AvlidNum = value
  def getAvlidNum                    = AvlidNum
  
  def setMean(value:Double)          = Mean = value
  def getMean = Mean

  def setVariance(value:Double)      = Variance = value
  def getVariance = Variance

  def setMax(value:Double)           = Max = value
  def getMax = Max

  def setMin(value:Double)           = Min = value
  def getMin = Min 
  
  def setUniqueValueCnt(value:Long)  = UniqueValueCnt = value
  def getUniqueValueCnt              = UniqueValueCnt

  def setCategoricalFeaturesInfo(value:HashMap[String,Int]) = CategoricalFeaturesInfo = value
  def getCategoricalFeaturesInfo     = CategoricalFeaturesInfo
  
  def setCategoricalNum(value:Int)   = CategoricalNum = value
  def getCategoricalNum              = CategoricalNum
  
  def isContinuous:Boolean           = if(dataType == Continuous) true else false
  def isCategorical:Boolean          = if(dataType == Categorical) true else false
  def isID:Boolean                   = if(dataType == ID) true else false
  
  def isIntType:Boolean              = if(storeType == IntType) true else false
  def isDoubleType:Boolean           = if(storeType == DoubleType) true else false
  def isStringType:Boolean           = if(storeType == StringType) true else false
  
  def isTarget:Boolean               = if(Role == Target) true else false
  def isInput:Boolean                = if(Role == Input) true else false
  def isVoid:Boolean                 = if(Role == Void) true else false
  
  def setDataTypeContinuous          = dataType = Continuous
  def setDataTypeCategorical         = dataType = Categorical
  def setDataTypeID                  = dataType = ID
  
  def setStoreTypeIntType            = storeType = IntType
  def setStoreTypeDoubleType         = storeType = DoubleType
  def setStoreTypeStringType         = storeType = StringType
  
  def setRoleTarget                  = Role = Target
  def setRoleInput                   = Role = Input
  def setRoleVoid                    = Role = Void

  


  
  
  


/**
 * 合并两列信息 , 包含统计量
 * 
 * @developing
 **/
  def mergeWithStatist(other:Column):Boolean = {
    if(columnName == other.columnName && 
        dataType == other.dataType && 
        storeType == other.storeType &&
        Role == other.Role){
      val deltaMean = Mean - other.Mean
      AvlidNum += other.AvlidNum
      Mean = (AvlidNum * Mean + other.AvlidNum * other.AvlidNum) / (AvlidNum + other.AvlidNum)
      //@developing
      Variance += other.Variance + deltaMean * deltaMean * AvlidNum * other.AvlidNum /
            (AvlidNum + other.AvlidNum)
      UniqueValueCnt += other.UniqueValueCnt
      true
    } else {
      false
    }    
  }
 
/**
 *合并两列信息 ， 不包含统计量  
 */
  def mergeOnly(other:Column):Boolean = {
    if(columnName == other.columnName){
      true
    }else {
       false
    }
  }
  
  def merge(other:Column):Boolean = {
    if(isAvlidStatis && other.isAvlidStatis) 
      mergeWithStatist(other)
    else 
      mergeOnly(other)
  }
  
  override def clone:Column = {
    val newColumn = new Column(id,columnName,dataType,storeType,Role)
    newColumn.AvlidNum = AvlidNum
    newColumn.isAvlidStatis = isAvlidStatis
    newColumn.Max = Max
    newColumn.Min = Min
    newColumn.Mean = Mean
    newColumn.Variance = Variance
    newColumn.UniqueValueCnt = UniqueValueCnt
    newColumn.CategoricalFeaturesInfo = CategoricalFeaturesInfo
    newColumn.CategoricalNum = CategoricalNum
    newColumn
  }
  


}
