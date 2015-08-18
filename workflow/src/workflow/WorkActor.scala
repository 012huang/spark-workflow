package workflow


import scala.collection.mutable.ListBuffer
import workflow.records.Property
import workflow.records.Record


class WorkActor(workflow: Workflow) extends Provider with Consumer {
	private var providers :ListBuffer[Provider]    = new ListBuffer[Provider]()  
	private var consumers :ListBuffer[Consumer]    = new ListBuffer[Consumer]()         
	private var started   :Boolean                 = false              
	protected var properties:ListBuffer[Property[_]] = new ListBuffer[Property[_]]()
	private var actorType :String                  = _
	private var actorName :String                  = _
	
	
	def setType(actortype:String) = actorType = actortype
	def getType = actorType

	def setName(actorname:String) = actorName = actorname
	def getName = actorName
	
	
	/**
	 * 工作流执行入口
	 * */
	def run(){
		if(! started){
			started = true;
			
			for(provider <- providers){
				provider.run();
			}
		}
	}
	
	def finalizes(){
		
	}
	
	/** 
	 * 数据流传递接口
	 */
	def handleRecord(record: Record){
		
		for(consumer <- consumers){
			consumer.handleRecord(record);
		}
	}
	
	/**
	 * 判断是否有Consumer被注册在actor上
	 * 
	 * @return true 说明有后继节点，否则为最终节点
	 */
	def hasConsumers(): Boolean = {
		!consumers.isEmpty
	}
	
	/**
	 * 注册一个上游节点
	 */
	def addProvider(provider:Provider){
		providers += provider
	}
	
	/**
	 * 
	 * 注册一个下游节点
	 */
	def addConsumer(consumer: Consumer){
		consumers += consumer 
	}
	
	/**
	 * 加入一个属性
	 * @param property 需要 key=value 的键值对
	 */
	def addProperty(property: Property[_] ){
		this.properties += property
	}
	
	/**
	 * 得到第一个给定属性名的参数
	 * @param name 属性名
	 * @return 第一个有该属性名的参数
	 */
	def  getFirstProperty(name: String): Property[_] = {
		for(property <- properties){
			if((property != null) && (property.getName() != null) && property.getName().equals(name)){
				return property;
			}
		}
		return null;
	}
	
	/**
	 * 
	 * 返回该属性名对应的值
	 * @param name 属性名
	 * @return 返回被发现的属性名
	 */
	def getFirstPropertyValue(name: String): Any = {
		val prop = getFirstProperty(name);
		if(prop == null){
			return null;
		}
		if(prop.getValue() == null){
			return null;
		}
		prop.getValue()
	}
	
	def getIntPropertyValueOrElse(name: String, default: Int): Int = {
	    val value = getFirstPropertyValue(name: String)
	    if(value == null) default else value.toString.toDouble.toInt
	}
	
	def getDoublePropertyValueOrElse(name: String, default: Double): Double = {
	    val value = getFirstPropertyValue(name: String)
	    if(value == null) default else value.toString.toDouble
	}

	
	def getStringPropertyValueOrElse(name: String, default: String): String = {
	    val value = getFirstPropertyValue(name: String)
	    if(value == null) default else value.toString
	}
	
	
	def getBooleanPropertyValueOrElse(name: String, default: Boolean): Boolean = {
	    val value = getFirstPropertyValue(name: String)
	    if(value == null) default else value.toString.toBoolean
	}
	/**
	 * 判断工作流是否已完成执行操作
	 * @return 如果为true说明工作流以完成操作
	 */
	def hasStarted(): Boolean = started
	
	/**
	 * 
	 * 返回一个日志消息到workflow节点
	 */
	def log(message:String){
		workflow.log(message)
	}
	
	/**
	 * 返回一个错误信息到workflow
	 * @param message Error message to log
	 */
	def logError(message:String){
		workflow.logError(message)
	}
	
	/**
	 * 返回一个错误信息到workflow并且结束进程
	 * @param message Message to log
	 */
	def die(message:String){
		workflow.die(message)
	}
}
