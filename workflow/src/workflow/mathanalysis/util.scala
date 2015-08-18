package workflow.mathanalysis

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vectors,Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import scala.collection.mutable.HashMap

private [workflow] object util {  
  
  def dealNumber(str:String):Double = {
    var value = 0.0
    try{
      value = str.toDouble
    }catch{
      case ex:NumberFormatException => value = 0.0
    }
    value
    
  }
  /**
   * 字符串到Vector类型转换
   * */
  def preData(input:Array[String]):Vector = {
    val temp = input.map(dealNumber)
    Vectors.dense(temp)
  }
  
  def preData(input:RDD[Array[String]]):RDD[Vector] = {
    input.map(preData)
  }
/**
 * Vector 转 labeledPoint
 * */  
  def vectorToLabel(data:Vector,targetCol:Int,inputCol:Array[Int]):LabeledPoint = {
    val label = data(targetCol)
    if(label != 1 || label != 0) println("label应为 0和1，但是得到却是:" + label)
    val features = for(i <- inputCol if i != targetCol) yield data(i)
    new LabeledPoint(label,Vectors.dense(features))
    
  }
  
  def vectorToLabelRdd(data:RDD[Vector],targetCol:Int,inputCol:Array[Int]):RDD[LabeledPoint] = {
    data.map(vectorToLabel(_,targetCol,inputCol))
  }
  
  def vectorToString(vData:RDD[Vector]):RDD[Array[String]] = {
    vData.map(_.toArray.map(_.toString()))
  }


/**
 * 实现训练集与验证集的分割
 * @param labelData 数据集
 * @param dataSplitPoint 分割点
 * */  
  def simpleLabelClassificationData(labelData:RDD[LabeledPoint],dataSplitPoint:Double):(RDD[LabeledPoint],RDD[LabeledPoint]) = {
    val rightData = labelData.filter(_.label == 0)
    val badData = labelData.filter(_.label == 1)
    val rightTemp  = rightData.randomSplit(Array(dataSplitPoint,1 - dataSplitPoint), math.random.toLong)
    val rightTraining = rightTemp(0)
    val rightTest = rightTemp(1)
    val badTemp = badData.randomSplit(Array(dataSplitPoint,1 - dataSplitPoint), math.random.toLong)
    val badTraining = badTemp(0)
    val badTest = badTemp(1)
    def trainingData = (rightTraining ++ badTraining).sample(false, 1, math.random.toLong)
    def testData = (rightTest ++ badTest).sample(false, 1, math.random.toLong)
    (trainingData,testData)
  }
  
  def simpleLabelData(labelData:RDD[LabeledPoint],dataSplitPoint:Double):(RDD[LabeledPoint],RDD[LabeledPoint]) = {
    val simpleData = labelData.randomSplit(Array(dataSplitPoint, 1 - dataSplitPoint), math.random.toLong)
    (simpleData(0),simpleData(1))
  }

  def simpleVectorData(input:RDD[Vector],dataSplitPoint:Double):(RDD[Vector],RDD[Vector]) = {
    val dataTemp = input.randomSplit(Array(dataSplitPoint,1- dataSplitPoint), math.random.toLong)
    val trainingData = dataTemp(0)
    val testData = dataTemp(1)
    (trainingData, testData)
  }

  
 /**
  * 得到分类字段的众数
  * @param input 输入数据
  * @param categoricalColId 分类字段ID
  * @return 对应分类ID的类别众数和众数个数
  * */ 
 def classValueCount(input: RDD[Array[String]], categoricalColId: Array[Int]): Array[(Int, String, Long)] = {
    val n = input.first.length
	val col = categoricalColId
	if (col.isEmpty) {
		return Array()
	}
	val colCnt = col.length
	val addValue = (valueCnt: Array[HashMap[String, Long]], values: Array[String]) => {
		val colLen = valueCnt.length
		for (i <- 0 until colLen) {
			if (valueCnt(i) == null) valueCnt(i) = HashMap[String, Long]()
			val data = values(col(i))
			if(valueCnt(i).contains(data)) {
			  val temp = valueCnt(i)(data)
			  valueCnt(i)(data) = temp + 1
			}
			else valueCnt(i)(data) = 1L
		}
		valueCnt
	}

	val merge = (values1: Array[HashMap[String, Long]], values2: Array[HashMap[String, Long]]) => {
		if (!values1.isEmpty && !values2.isEmpty) {			  
		    val valueSize = values1.length
		    require(values1.length == values2.length)
			for (i <- 0 until valueSize) {
				if (values1(i) != null && values2(i) != null && !values1(i).isEmpty && !values2(i).isEmpty) {
				  values2(i).foreach{
				    case(key, value) =>
				      if(values1(i).contains(key)) {
				        val temp = values1(i)(key)
				        values1(i)(key) = temp
				      } else values1(i)(key) = value
				  }
				} else if ((values1(i) == null || values1(i).isEmpty) && (values2(i) != null && !values2(i).isEmpty)) values1(i) = values2(i)
			}
		} else if (values1.isEmpty && !values2.isEmpty) {
			for (i <- 0 until values1.length) values1(i) = values2(i)
		}
		values1
	}

	val valueCnt = input.aggregate(new Array[HashMap[String, Long]](colCnt))(addValue, merge)
	var precent = new Array[(Int, String, Long)](n)
	for (i <- 0 until n) {
		if (col.contains(i)) {
			var maxValue = 0L
			var cn = col.indexOf(i)
			var maxKey: String = ""
			val len = valueCnt(cn).size
			valueCnt(cn).foreach{
			  case(key,value) => 
			    if(value > maxValue){
			      maxValue = value
			      maxKey = key
			    }
			}
			precent(i) = (i, maxKey, maxValue)
		} else precent(i) = (i, "", 1)
	}
	precent
  }
 
 /**
  * 获取参数为数组参数的转换例如String 1,2,3 => Array[String](1,2,3)
  * 没有key值则为空数组
  * @param paramMap 参数map
  * @param key 参数键值
  * return 参数分割后的数组，若无则为空数组
  * */
 def arrFromParams(params:HashMap[String,String],key:String,split:String = ","):Array[String] = {
   val value = params.get(key)
   if(value == None) Array[String]() else value.get.split(split)
 }
 
 def numFromParams(params:HashMap[String,String],key:String,default:Double):Double = {
   val temp = params.get(key)
   if(temp == None) default else temp.get.toDouble
 }
 
 
  def columnFilterVector(dataSet:RDD[Vector], Col:Array[Int]):RDD[Vector] = {
     dataSet.map(line => Vectors.dense(for(elem <- Col) yield line(elem)))
  }

  def columnFilterArray(dataSet:RDD[Array[String]], Col:Array[Int]):RDD[Array[String]] = {
     dataSet.map(line => for(elem <- Col) yield line(elem))
  }


}
