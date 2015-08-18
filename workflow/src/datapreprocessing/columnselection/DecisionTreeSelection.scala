package datapreprocessing.columnselection

import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vectors,Vector}
import scala.collection.mutable.HashMap
import org.apache.spark.mllib.regression.LabeledPoint
import datapreprocessing.util
import org.apache.spark.mllib.tree.DecisionTree
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.mllib.tree.model.Node

class DecisionTreeSelection(
      targetCol:Int,
      columnNum:Int,
      depth:Int,
      maxbins:Int,
      categoricalColId:Array[Int] = Array()
      ) extends ColumnSelection with Serializable{

  private var inputCol:Array[Int] = Array()
  
  private var catecategoricalColAndMap:Array[(Int,HashMap[String,Int])] = Array()
  
  
  /**
   * 检验数据是回归还是分类
   * */
  def validCheckRegOrClass(labelData:RDD[LabeledPoint]):Int = {
    val result = labelData.map(_.label).countByValue
    result.size
  }
  
  def selection(dataSet:RDD[Array[String]]):Array[Int] = {
    var featureInfo = Map[Int,Int]()
    inputCol = for(i <- (0 until columnNum).toArray if i != targetCol) yield i
    if(!categoricalColId.isEmpty){
      createCategoricalMap(dataSet)
      featureInfo = getClassMap()
    }
    val labelData = dataPreparation(dataSet)
    val labelNum = validCheckRegOrClass(labelData)
    val model = if(labelNum < 5){
    	 DecisionTree.trainClassifier(labelData, labelNum, featureInfo, "gini", depth, maxbins)
        } else {
         DecisionTree.trainRegressor(labelData, featureInfo, "gini", depth, maxbins)
        }
	val (nodes,_,_) = nodesWithParentsId(model.topNode,-1)
    nodes.filter(f=>{
      f.split != None
    }).map(_.split.get.feature).sorted.toArray
  }    
  
  private def createCategoricalMap(dataSet:RDD[Array[String]]) = {
    catecategoricalColAndMap = CreateCategoricalValue(dataSet)
  }
  
  private def getClassMap():Map[Int,Int] = {
    catecategoricalColAndMap.map(f=>(f._1,f._2.size)).toMap
  }
  
  private def dataPreparation(dataSet:RDD[Array[String]]):RDD[LabeledPoint] = {   
    dataSet.map(f=>{
      val dataStr = CategoricalValueToClassTranform(f)
      val dataVector = util.preData(dataStr)
      util.vectorToLabel(dataVector, targetCol, inputCol)
    })
  }

/**
 * 分类类型 值到类别的转换
 * */ 
  private def CategoricalValueToClassTranform(value:Array[String]):Array[String] = {
    val newValue = value
    for((elem,classMap) <- catecategoricalColAndMap){
         newValue(elem) = classMap(value(elem)).toString      
    }
    newValue
  }
  
  /**
  * 创建分类类型的映射关系Map函数
  * */ 
  private def CreateCategoricalValueMap(iter: Iterator[Array[String]]):Iterator[Array[HashMap[String,Int]]] = {
      val len = categoricalColId.length
	  val mapArr = new Array[HashMap[String, Int]](len)
	  var j = Array.fill(len)(0)
	  iter.foreach(
	    t => {
	      for(i <- 0 until len) 
	        if(mapArr(i) == null) {
	          mapArr(i) = new HashMap[String, Int]
	          mapArr(i)(t(categoricalColId(i))) = j(i)
	          j(i) += 1
	        } else {
	          if(!mapArr(i).contains(t(categoricalColId(i)))) {
	             mapArr(i)(t(categoricalColId(i))) = j(i)
	             j(i) += 1
	           }
	        }
	    }
	  )
	  Iterator(mapArr)	
  }
  
/**
 * 创建分类类型的映射关系reduce函数
 * */ 
  private def CreateCategoricalValueReduce(m1:Array[HashMap[String, Int]], m2:Array[HashMap[String, Int]]):Array[HashMap[String, Int]] = {
    val len = categoricalColId.length
    var j = 0
      if(!m1.isEmpty && !m2.isEmpty && m2 != null){
	    m2.foreach{
	      t =>
	        if(!t.isEmpty && !m1(j).isEmpty){
	          t.foreach{case(key,value) =>
	          if(!m1(j).contains(key)) {
	            val num = m1(j).size
	            m1(j)(key) = num
	          }  
	        }
	      } else if(!t.isEmpty && m1(j).isEmpty) {
	        m1(j) = t
	      }
	        
	     j += 1
	    }
      } else if(m1.isEmpty && !m2.isEmpty) {
        for(i<- 0 until len) m1(i) = m2(i)
      }
      m1
   }
  
  private def CreateCategoricalValue(input:RDD[Array[String]]):Array[(Int,HashMap[String,Int])] = {
    val myresult = input.mapPartitions(CreateCategoricalValueMap).reduce(CreateCategoricalValueReduce)    
    categoricalColId.zip(myresult).toArray
  }

  private def nodesWithParentsId(topNode:Node,id:Int):(ArrayBuffer[Node],ArrayBuffer[Int],Int)={
    val Nodes = new ArrayBuffer[Node]
    val nodesId = new ArrayBuffer[Int]
    var k = 0
    if(topNode!=null){      
      Nodes.insert(k, topNode)
      nodesId.insert(k,id)
      k=k+1
      if(topNode.leftNode!= None) {
        val (addNew,addId,t)= nodesWithParentsId(topNode.leftNode.get,topNode.id)
        Nodes.insertAll(k,addNew)
        nodesId.insertAll(k,addId)
        k=k+t
      }
      if(topNode.rightNode!= None) {
        val (addNew,addId,t) = nodesWithParentsId(topNode.rightNode.get,topNode.id)
        Nodes.insertAll(k,addNew)
        nodesId.insertAll(k,addId)
        k=k+t
      }
    }
    (Nodes,nodesId,k)
  }


}
