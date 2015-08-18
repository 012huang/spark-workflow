package workflow.actors

import workflow.WorkActor
import workflow.Workflow
import workflow.model.DataLearning
import workflow.model.ModelFactory
import workflow.dataset.DataSet
import workflow.records.Record
import workflow.records.Property
import workflow.model.ModelPredict
import java.io.File


class ModelActor(workflow: Workflow) extends WorkActor(workflow){
  
  private var modelStr           :String       =_
  private var mathModel          :DataLearning =_
  private var dataSet            :DataSet      =_
  private var receiveProperties  :Record       =_
  
  def createModel():this.type = {
    modelStr = getFirstPropertyValue("method").toString
    println("选择的模型是:" + modelStr)
    mathModel =  new ModelFactory(modelStr).getModel(properties)
    this
  }
  
  override def run(){    
    if(!hasStarted()) {
      super.run
      val newdata = createModel
      println("开始运行模型:")
      val (trainingData,testData) = splitData
      println("trainingData记录数为：" + trainingData.data.count)
      println("testData记录数为：" + testData.data.count)
      mathModel.runModel(trainingData)      
      println("模型运行结束")
      val result = mathModel.predict(testData)
      val passRecord = new Record
      passRecord.addProperty(new Property("data" ,result))
      super.handleRecord(passRecord)
    }

  }
  
  override def handleRecord(record: Record){
    this.receiveProperties = record
    this.dataSet = try{
      record.getFirstProperty("data").getValue.asInstanceOf[DataSet].clone
      
    } catch {
      case ex:Exception =>
        throw new Exception("上游节点数据获取错误！")
    } 
    println("得到上游数据节点" + this.dataSet.toString())   
  }
  
  override def finalizes(){
    val modelname = this.getName
    val modeltype = this.getType
    println("保存" + modelname +" "+ modeltype)
    val savedir = new File(workflow.modelSavePath)
    if(!savedir.exists()) savedir.mkdirs()
    val modeSavePath = savedir+"/"+ modelStr + modelname
    println("保存组件路径为" + modeSavePath)
    mathModel.saveParams(modeSavePath)
    workflow.registerModel(modeltype, modelname, modelStr,modeSavePath)
  }
  
  
  private def splitData():(DataSet,DataSet) = {
    if(dataSet != null) {
      val splitPoint = getDoublePropertyValueOrElse("split.point", 0.8)
//      if(modelStr != "kmeans"){
//    	dataSet.dataSplitWithLabel(splitPoint)        
//      } else {
        val arrDataSet = dataSet.dataSplit(Array(splitPoint, 1 - splitPoint))
        (arrDataSet(0),arrDataSet(1))
//      }
      
    } else {
      throw new Exception("数据集合为空")
    }
    
  }

  def getPredictor():ModelPredict = {
    if(mathModel != null){
      return mathModel
    } else null
  }


}
