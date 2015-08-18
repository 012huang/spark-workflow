package workflow

import java.io.File
import java.io.IOException
import java.lang.reflect.Constructor
import java.lang.reflect.InvocationTargetException
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashMap
import java.util.logging.Logger
import workflow.built.WorkflowBuilter
import workflow.built.WorkflowFromXML
import org.apache.spark.SparkContext
import java.io.PrintWriter
import java.io.OutputStreamWriter
import java.io.FileOutputStream

 class Workflow {
	private var actors    : ListBuffer[WorkActor] = new ListBuffer[WorkActor]()
	private var logger    : Logger                = null
	
	var sc                :SparkContext           = _
	var userName          :String                 = _
	var Id                :String                 = "1101"
	
	var workDirectory     :String                 = "/home/workspace/" + Id
	val modelSavePath     :String                 = workDirectory + "/" + "model"
	val modelregisterPath :String                 = workDirectory + "/" + "ModelRegisterTabel"
	
	def this(actors:ListBuffer[WorkActor]){
	  this()
	  this.actors = actors
	}
	
	
	def sizeOfActors:Int = actors.size
	
	
	/**
	 * 增加一个Actor到Worflow
	 * @param 加入actor
	 */
	def addActor(actor: WorkActor){
		this.actors += actor
	}
	
//	def addPredicter(actor: WorkActor) {
//	    this.predicter += actor
//	}
	
	/**
	 * 
	 * 开始运行一个工作流
	 */
	def run(){
	    
		for(actor <- actors){
			if(! actor.hasConsumers()){
				actor.run()
			}
		}
		

		for(actor <- actors){
		    println("开始finalizes" + actor.getName)
			actor.finalizes()
		}
	}
	
	
	def log(message:String){
		logger.info(message);
	}
	
	def logError(message:String){
		logger.severe(message);
	}
	
	def die(message:String){
		logError(message);
		System.exit(1);
	}
	
	def registerModel(modelType:String, modleName:String, mothed:String ,path:String){
	  val wd = new File(workDirectory)
      if(!wd.exists()) wd.mkdirs()

      val out = new PrintWriter(new OutputStreamWriter(new FileOutputStream(modelregisterPath,true),"UTF-8"))
      out.println(s"Name=${modleName}")
      out.println(s"Type=${modelType}")
      out.println(s"Model=${mothed}")
      out.println(s"path=${path}")
      out.close()
	}
}
