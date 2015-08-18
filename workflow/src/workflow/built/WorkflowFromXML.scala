package workflow.built

import workflow.Workflow
import java.io.File
import scala.xml.Elem
import scala.xml.Document
import scala.xml.Node
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashMap
import workflow.WorkActor
import java.io.FileSystem
import scala.xml.XML
import workflow.actors.ImportDataActor
import workflow.actors.TransformationActor
import workflow.actors.EvaluationActor
import workflow.actors.ModelActor
import workflow.records.Property
import workflow.actors.StartActor
import scala.io.Source
import workflow.model.SaveLoadModel
import workflow.actors.PredicterActor

class WorkflowFromXML(xmlPath :String) extends WorkflowBuilter{
     
/**
 * 
 *    从XML文档创建一个工作流
 *
 */
 
  def fromXML:Workflow = {
		
	val doc       :Elem     = getXMLElem
	val workflow  :Workflow = new Workflow
    val parants   = new HashMap[String,Array[String]]()
    val actors    = new HashMap[String,WorkActor]()
    for(compNode <- (doc \ "component")) {
        //获取组件id ,和type
		val componentId = compNode.attribute("id").get.toString		
		val componentType = compNode.attribute("type").get.toString
		
		//
		parants(componentId) = for(elem <- (compNode \\ "parent").toArray) yield {
		  (elem \\ "@id").toString
		}
		
		parants.foreach{
		  case(id, parents) =>
		    println(id + " parents:" + parents.mkString(","))
		}
		
		val actor = componentType match {
		  case "inputData"       => new ImportDataActor(workflow)
		  case "transformation"  => new TransformationActor(workflow)
		  case "model"           => new ModelActor(workflow)
		  case "evaluation"      => new EvaluationActor(workflow)
		  case "start"           => new StartActor(workflow)
		  case "predict"         => createrPredictor(workflow, componentId, compNode)
		  case _                 => 
		    throw new Exception ("无法识别的组件类型: " + componentType)
		}
		actor.setType(componentType)
		actor.setName(componentId)
		println("actorType: " + componentType)
		
		for(elem<- (compNode \\ "parameter")) {
		  val pro = new Property()
		   val id = (elem \\ "@name").toString
		   val value = (elem \\ "@defaultValue").toString
		   actor.addProperty(new Property(id,value))
		   println(componentId + " parameter: " + id + ":" +value)
		}
		
		actors(componentId) = actor
    }
	
	
	
	for((name, actor) <- actors) {
	  val parentsName = getParentActor(name, parants)
	  for(parentName <- parentsName) {
	    actor.addProvider(actors(parentName))
	  }
	  val childsName  = getChildActor(name, parants)
	  for(childName <- childsName) {
	    actor.addConsumer((actors(childName)))
	  }	
	  workflow.addActor(actor)
	}
	workflow
  }
	
  /**
   * 得到该节点的父亲节点
   * */
  private def getParentActor(name: String, actors:HashMap[String,Array[String]]): Array[String] = {
    actors(name)
    
  }
  /**
   * 得到该节点的孩子节点
   * */
  private def getChildActor(name: String, actors:HashMap[String,Array[String]]): Array[String] = {
    for((compName, parents) <- actors.toArray if parents.contains(name)) yield {
      compName
    }
  }
  
  def createrPredictor(workflow:Workflow, modelName:String, compNode:Node) : PredicterActor = {
    val registerPath  :String         = workflow.modelregisterPath
    var modelPath     :String         = null
    var modelType     :String         = null
    var predictor     :PredicterActor = null
    var modelMethod   :String         = null
    for(elem<- (compNode \\ "parameter")) {
	   val id = (elem \\ "@name").toString
	   val value = (elem \\ "@defaultValue").toString
       if(id.equals("method")) modelMethod = value
    }

    getModelPath(registerPath, 
                 modelName, 
                 tmp => modelType   = tmp, 
                 tmp => modelMethod = tmp,
                 tmp => modelPath   = tmp)
    if(modelPath == null || modelType == null || modelMethod == null) throw new Exception("在模型目录中没有保存找到相应的模型")
    println("modelType" +" "+ modelType)
    println("modelMethod" + "=" + modelMethod)
    println("modelPath" + "=" + modelPath)
    modelType match {
      case "transformation"  => val trans = new TransformationActor(workflow)
                                trans.addProperty(new Property("method", modelMethod))
                                trans.createTransformater
                                val pre = trans.getPredictor
                                if(pre != null) {
                                  try {
                                	  pre.rebulitModel(modelPath)                                    
                                  } catch {
                                    case ex:Exception => throw new Exception("数据模型加载错误，原因是:" + ex.getMessage())
                                  }
                                  predictor = new PredicterActor(workflow,pre)
                                }
      case "model"           => val model = new ModelActor(workflow)
                                model.addProperty(new Property("method", modelMethod))
                                model.createModel
                                val pre = model.getPredictor
                                if(pre != null) {
                                  try {
                                	  pre.rebulitModel(modelPath)                                    
                                  } catch {
                                    case ex:Exception => throw new Exception("数据模型加载错误，原因是:" + ex.getMessage())
                                  }
                                  predictor = new PredicterActor(workflow,pre)
                                }
      case _                 => throw new Exception("模型类型输入错误,没有" +modelType+ "类型！！")
    }
    predictor
  }
  
  
  def getModelPath(registerPath:String, modelName:String, modelType:String => Unit, modelMethod:String => Unit, modelPath:String => Unit)  {
    val dataIn = Source.fromFile(registerPath,"UTF-8")
                       .getLines.map(data=>{val tmp = data.split("=");(tmp(0),tmp(1))})
    while(dataIn.hasNext) {
      val (key,value) = dataIn.next
      println(key + "=" + value)
      if(key.equals("Name") && value.equals(modelName)){
        val (_, mty)   = dataIn.next
        modelType(mty)
        val (_, mmd) = dataIn.next
        modelMethod(mmd)
        val (_, mp)   = dataIn.next 
        modelPath(mp)
        return
      }
    }
    
  }
  
  private def checkWorkflow(actors:HashMap[String,WorkActor]):Unit = {
    var startActorNum:Int = 0
    for((_, actor) <- actors){
      if(actor.getType == "start") startActorNum += 1
    }
    if(startActorNum == 0) throw new Exception("开始节点个数为0,请在工作流中加入开始节点。")
    if(startActorNum > 1) throw new Exception("开始节点个数为"+ startActorNum +",请保证每个工作流中只有一个开始节点。")
  }
  
  def builter():Workflow = {
    fromXML
  }
	
	
  private def getXMLElem():Elem = {
	  try {
	    XML.loadFile(xmlPath)
	  } catch {
	  case ex :Exception => 
	    throw new Exception("读取XML文件时出现错误: " + ex.getMessage())
	  }
  }


}
