package workflow.actors

import workflow.Workflow
import workflow.WorkActor
import workflow.transformation.Transformer
import workflow.transformation.AutoDataProcessing
import workflow.records.Record
import workflow.dataset.DataSet
import workflow.records.Property
import workflow.model.ModelPredict
import workflow.model.SaveLoadModel
import java.io.File

class TransformationActor(workflow:Workflow) extends WorkActor(workflow){
  
  private var receiveProperties :Record       =_
  private var dataSet           :DataSet      =_
  private var columnNum         :Int          =_ 
  private var transformer       :Transformer  =_
  private var predicter         :ModelPredict =_
  private var method            :String       =_
  
  def createTransformater():Transformer = {
    method = getFirstPropertyValue("method").toString
    transformer = method match {
      case "AutoDataPrepare"  =>
        val autoData = new AutoDataProcessing(properties)
        predicter = autoData
        autoData
      case _ => 
        throw new Exception("没有方法: "+ method)
    }
    transformer
  }
  
  override def run(){
    if(!hasStarted()) {
      super.run
      val newdata = createTransformater.transform(dataSet)
      val passRecord = new Record
      passRecord.addProperty(new Property("data" ,newdata))
      super.handleRecord(passRecord)
    }
    
  }
  
  override def handleRecord(record: Record){    
    this.receiveProperties = record
    this.dataSet = try{
      record.getFirstProperty("data").getValue.asInstanceOf[DataSet].clone
    } catch {
      case ex:Exception =>
        throw new Exception("获取上游节点数据产生错误: " + ex.getMessage())
    }
    this.columnNum = dataSet.dataColumn 
  }
  
  override def finalizes(){
    if(predicter != null){
      val modelname = this.getName
      val modeltype = this.getType
      val savedir = new File(workflow.modelSavePath)
      if(!savedir.exists()) savedir.mkdirs()
      val modeSavePath = savedir+"/"+ method + modelname
      predicter.asInstanceOf[SaveLoadModel].saveParams(modeSavePath)
      workflow.registerModel(modeltype, modelname, method, modeSavePath)      
    }
  }
  
  def getPredictor():ModelPredict = {
    if(predicter != null){
      return predicter
    } else null
  }
  
  
  
}
