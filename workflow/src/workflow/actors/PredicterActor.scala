package workflow.actors

import workflow.WorkActor
import workflow.Workflow
import scala.collection.mutable.ListBuffer
import workflow.model.ModelPredict
import workflow.records.Record
import workflow.dataset.DataSet
import workflow.records.Property


class PredicterActor(workflow:Workflow) extends WorkActor(workflow){
   
  def this(workflow:Workflow,predicter:ModelPredict){
    this(workflow)
    this.predicter = predicter
  }
  
  private var predicter     :ModelPredict  =_
  private var dataSet       :DataSet       =_    
    
    
    
  override def run(){
	  if(!hasStarted && predicter != null){
	    super.run
	    val result = predicter.predict(dataSet)
        val passRecord = new Record
        passRecord.addProperty(new Property("data" ,result))
        super.handleRecord(passRecord)
	  }
	}
    
    
    
  override def handleRecord(record: Record){
    this.dataSet = try{
      record.getFirstProperty("data").getValue.asInstanceOf[DataSet].clone
    } catch {
      case ex:Exception =>
        throw new Exception("上游节点数据获取错误！")
    } 
  }    
}
