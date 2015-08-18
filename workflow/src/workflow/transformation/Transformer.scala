package workflow.transformation


import scala.collection.mutable.HashMap
import datapreprocessing.DataDeal
import org.apache.spark.rdd.RDD
import datapreprocessing.DataPreprocessingModel
import scala.collection.mutable.ListBuffer
import workflow.records.Property
import workflow.dataset.DataSet
import workflow.dataset.ColumnSet
import workflow.model.ModelPredict
import scala.io.Source
import java.io.PrintWriter
import java.io.OutputStreamWriter
import java.io.FileOutputStream
import java.io.IOException
import workflow.model.SaveLoadModel

/**
 * 数据变换接口
 **/
trait Transformer {
  
  def transform(dataSet:DataSet):DataSet

}

/**
 * 自助数据准备
 * */

class AutoDataProcessing(properties:ListBuffer[Property[_]]) extends Transformer with ModelPredict with SaveLoadModel{
  
  val params  :HashMap[String,String] = new HashMap[String,String]()
  
  val paramsKeys = Array(
      ("clean.column"                          , "true"), 
      ("clean.column.void"                     , "true"),
      ("clean.column.void.condition"           , "0.7"), 
      ("clean.column.classoverflow"            , "false"),
      ("clean.cloumn.classoverflow.condition"  , "0.5"), 
      ("clean.column.single"                   , "true"),
      ("clean.column.single.condition"         , "0.99"), 
      ("clean.value"                           , "true"),
      ("clean.value.void"                      , "true"), 
      ("clean.value.void.method"               , "method1"),
      ("clean.value.void.string"               , "N/A"), 
      ("clean.value.void.addanalysis"          , "true"),
      ("clean.value.void.addtarget"            , "true"), 
      ("clean.column.correlation"              , "true"),
      ("clean.column.correlation.threshold"    , "0.2"), 
      ("clean.value.outlier"                   , "true"),
      ("clean.value.outlier.addanalysis"       , "true"), 
      ("clean.value.outlier.addtarget"         , "true"),
      ("clean.value.outlier.threshold"         , "3") , 
      ("clean.value.outlier.method"            , "edge_value_replace"),
      ("transform.value"                       , "false"), 
      ("transform.value.method"                , "range"),
      ("transform.value.params.1"              , "1"), 
      ("transform.value.params.2"              , "0"),
      ("column.selection"                      , "true"), 
      ("column.selection.evaluate.datasplit"   , "0.8"),
      ("column.selection.method"               , "forward"), 
      ("column.selection.correletion.threshold", "0.2"), 
      ("column.selection.num.limit"            , "10"), 
      ("column.selection.evaluate.treedeep"    , "10"),
      ("column.selection.evaluate.treemaxbins" , "100")
      )
  
  
  var dataModel :DataPreprocessingModel = _  
  var hasModel  = false
  
  /**
   * 执行数据自助准备
   * @params DataSet 数据集合 
   * 
   * */
  def transform(dataSet:DataSet):DataSet = {
    
    val paramsTr = propertyToHashMap(dataSet.getColumnSet)
    val dataDeal = new DataDeal(paramsTr)
    
    val newData = try {
         dataDeal.cleanData(dataSet.data)
    }catch{
      case ex:Exception =>
        throw new Exception("自助数据准备异常: " + ex.getMessage())
    }
    dataSet.setData(newData)
    dataModel = dataDeal.createModel
    val cols = dataModel.columnSelection
    val tmp = dataSet.getColumnSet.filterById(cols).refreshColumnId
    dataSet.setColumnSet(tmp)
    saveModel(paramsTr)
    hasModel = true
    dataSet
  }
  
  
  private def propertyToHashMap(columnInfo:ColumnSet):HashMap[String,String] =  {
    val targetId = columnInfo.getIdOfTarget
    val cateId = columnInfo.getAllIdOfCategorical    
    
    val paramsTr = new HashMap[String,String]()
    paramsTr("target.col")           = targetId.toString
    paramsTr("catecategorical.col")  = cateId.mkString(",")
    
    for((key, default) <- paramsKeys) {
      var hasContains = false
      for(propertie <- properties){
        if(key == propertie.getName) {
          paramsTr(key) = propertie.getValue.toString
          hasContains = true
        }
      }
      if(!hasContains) {
    	paramsTr(key) = default
      }
    }
    paramsTr    
  }
  
  def predict(dataSet:DataSet):DataSet = {
    val result = dataModel.predict(dataSet.data)
    dataSet.setData(result)
    
    val cols = dataModel.columnSelection
    val tmp = dataSet.getColumnSet.filterById(cols).refreshColumnId
    dataSet.setColumnSet(tmp)
    hasModel = true
    dataSet
    
  }
  
  def saveModel(paramTr:HashMap[String,String]){
    val autoModel = DataPreprocessingModel.modeToString(dataModel, paramTr)
    params("automodelparams") = autoModel
  }
  
  def rebulitModel(modelPath:String):this.type = {
    readParams(modelPath)
    if(!params.isEmpty){
      dataModel = createModel()
      return this
    } else {
      throw new Exception("模型重建失败，原因是该模型参数为空！")
    }
  }
  
  def createModel():DataPreprocessingModel = {
    val autoModelStr = params.get("automodelparams").get
    DataPreprocessingModel.createModel(autoModelStr)
  }
  



}
