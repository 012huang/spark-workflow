package workflow.mathanalysis

import workflow.dataset.DataSet
import scala.reflect.ClassTag
import workflow.dataset.ColumnSet
import scala.collection.mutable.HashMap
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import scala.collection.mutable.HashSet
import breeze.linalg.{DenseVector => BDV}
 

class DataManage[T:ClassTag](ColInfo:ColumnSet) extends Serializable{
  
  lazy val colSet = ColInfo.getColumn
  lazy val categoricalColId = ColInfo.getAllIdOfCategorical
  lazy val continuousColId = ColInfo.getAllIdOfContinuous
  lazy val inputColId = ColInfo.getAllIdOfInput
  lazy val targetColId = ColInfo.getIdOfTarget
  
  
  /**
   * 分类类型 值到类别的转换
   * */
  def CategoricalValueToClassTranform(iter:Iterator[Array[String]]):Iterator[Array[String]] = {
    iter.map{
      value =>{ 
        val newValue = value
        for(elem <- categoricalColId) {
         newValue(elem) = colSet(elem).getCategoricalFeaturesInfo(value(elem)).toString
        }
        newValue
       }
      }
  }

  
/**
 * 分类类型 值到类别的转换
 * */ 
  def CategoricalValueToClassTranform(value:Array[String]):Array[String] = {
    val newValue = value
    for(elem <- categoricalColId){
         newValue(elem) = colSet(elem).getCategoricalFeaturesInfo(value(elem)).toString      
    }
    newValue
  }
  
/**
 * 分类类型 类别到值的转换
 * */ 
  
  def CategoricalClassToValueTranform(iter:Iterator[Array[String]]):Iterator[Array[String]] = {
    val mapInfo = ColInfo.getAllCategoricalInfo
    val reMap = mapInfo.map(_.map{case(key,value) => (value,key)})
    iter.map{
      value =>{ 
        val newValue = value
        for(elem <- categoricalColId) {
          newValue(elem) = reMap(elem)(value(elem).toInt)
        }
        newValue
       }
      }
  }
 /**
  * 创建分类类型的映射关系Map函数
  * */ 
  def CreateCategoricalValueMap(iter: Iterator[Array[String]]):Iterator[Array[HashMap[String,Int]]] = {
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
  def CreateCategoricalValueReduce(m1:Array[HashMap[String, Int]], m2:Array[HashMap[String, Int]]):Array[HashMap[String, Int]] = {
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
  
  def CreateCategoricalValue(input:RDD[Array[String]]):Array[(Int,HashMap[String,Int])] = {
    val myresult = input.mapPartitions(CreateCategoricalValueMap).reduce(CreateCategoricalValueReduce)    
    categoricalColId.zip(myresult).toArray
  }
  
  /**
   * 用新选的feature替代以前的feature
   * @params NewFeature:新选择的分类字段Id
   * */
  def LabeledChangeFeature(Input:RDD[LabeledPoint], NewFeature:Array[Int]):RDD[LabeledPoint] = {
    Input.map{
      value =>
      val (label, feature) = (value.label, value.features)
      val featureTemp = for(elem <- NewFeature) yield feature(elem)
      new LabeledPoint(label,Vectors.dense(featureTemp))
    }
  }
  
  /**
   * 在labeledPoint 中过滤掉所选的字段
   * @params FilterFeature:将被过滤掉的字段id
   * */
  def LabeledFilterFeature(Input:RDD[LabeledPoint], FilterFeature:Array[Int]):RDD[LabeledPoint] = {
    Input.map{
      value =>
      val (label, feature) = (value.label, value.features)
      val featureTemp = for(i <- (0 until feature.size).toArray if !FilterFeature.contains(i)) yield feature(i)
      new LabeledPoint(label,Vectors.dense(featureTemp))
    }    
  }
  
  /**
   * 找到异常分类字段的分类类别 ， 并保存在一个HashMap中
   * @params classNum: 作为标准的类别值
   * */
  def FindOutlierValueInCategorical(Input:RDD[LabeledPoint], classNum :Int = 1): Array[(Int,HashMap[Double,(Int,Int)])] = {
    val colId = categoricalColId.filter(_ != targetColId)
    val myresult = Input.mapPartitions(FindOutlierMap(_, classNum)).reduce(FindOutlierReduce)    
    colId.zip(myresult).toArray
  }
  /**
   * 找到异常分类字段的map阶段
   * */
  def FindOutlierMap(iter:Iterator[LabeledPoint], classNum: Double):Iterator[Array[HashMap[Double, (Int, Int)]]] = {
    val colId = categoricalColId.filter(_ != targetColId)
    val len = colId.length
    val valueMap = new Array[HashMap[Double,(Int,Int)]](len)
    iter.foreach{
      labeled => {
        val label = labeled.label
        val features = labeled.features
        for(i <- 0 until len) {
          if(valueMap(i) == null) valueMap(i) = new HashMap[Double,(Int,Int)]
          val value = features(colId(i))
          if(valueMap(i).contains(value)) {
        	 val (rightCnt,totalCnt) = valueMap(i)(value)
             if(label == classNum) {
              valueMap(i).update(value, (rightCnt + 1, totalCnt + 1))
            } else 
              valueMap(i).update(value, (rightCnt, totalCnt + 1))           
          }  else if (label == classNum) valueMap(i)(value) = (1, 1)
            else valueMap(i)(value) = (0, 1)
        }
      }
    }
    Iterator(valueMap)
  }
  
  /**
   * 找到异常分类类别的reduce阶段
   * */
  def FindOutlierReduce(m1:Array[HashMap[Double, (Int, Int)]], m2:Array[HashMap[Double, (Int, Int)]]):Array[HashMap[Double, (Int, Int)]] = {

      if(m2 != null && !m1.isEmpty && !m2.isEmpty  ){
        var j = 0
	    m2.foreach{
	      t =>
	        if(m1(j) != null && !t.isEmpty && !m1(j).isEmpty){
	          t.foreach{case(key,value) =>
	          if(!m1(j).contains(key)) {
	            m1(j)(key) = value
	          } else {
	            val (rightCnt2,totalCnt2) = m2(j)(key)
	            val (rightCnt1,totalCnt1) = m1(j)(key)
	            m1(j).update(key, (rightCnt1 + rightCnt2,totalCnt1 + totalCnt2))
	          }
	        }
	      } else if(!t.isEmpty && m1(j).isEmpty) {
	        m1(j) = t
	      }
	        
	     j += 1
	    }
      } else if(m1.isEmpty && !m2.isEmpty) {
        for(i<- 0 until m2.length) m1(i) = m2(i)
      }
      m1
  }
  /**
   * 在数据集中过滤掉valueMap中包含的元素
   * */
  def FilterValueInCategorical(iter:Iterator[LabeledPoint] ,valueMap:Array[(Int,HashSet[Double])]) = {
    iter.filter{
      t =>{
        val features = t.features
        val result = for((index,map) <- valueMap) yield !map.contains(features(index))
        result.reduce(_ && _)
      }
    }
  }
  
  def FilterValueInCategorical(Labeled:RDD[LabeledPoint], valueMap:Array[(Int,HashSet[Double])]):RDD[LabeledPoint] = {
    Labeled.mapPartitions(FilterValueInCategorical(_,valueMap))
  }
  
 
//----------------------------------------------
//以下为临时代码
/**
 * 求平均值 
 * */  
  def getMean(input:RDD[LabeledPoint],Col:Array[Int]):BDV[Double] = {
    val sc = input.context
    val n = Col.length
    val plus = (value: Iterator[LabeledPoint]) => {
      var sum: BDV[Double] = BDV.zeros(n) 
      var count :BDV[Double] = BDV.zeros(n)
      while(value.hasNext){
        val line = value.next.features
        for(i <- 0 until n) {
           val data = line(Col(i))
           val currSum = sum(i)
           sum(i) = data.toDouble + currSum
           val currCount = count(i)
           count(i) = currCount+1
          
        }
      }
      (sum,count) 
    }    

    val (sum,count) = sc.runJob(input, plus).reduce((sum1,sum2)=>(sum1._1 + sum2._1,sum1._2 + sum2._2))
    sum / count    
  }

/**增量法求方差Reference: [[http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance]]
 * 求每列标准差, 方差公式  S^2 = (1/n)[(X1-E)^2 + (X2-E)^2....(Xn-E)^2]，标准差S = sqrt(S^2)
   average 输入 */
  def getVariance(input:RDD[LabeledPoint],Col:Array[Int], maxAndMin:HashMap[Int,(Double,Double)]):(BDV[Double],BDV[Double]) = {
    val sc = input.context 
    val n = Col.length
    val getM2n_Mean_CountMap = (value: Iterator[LabeledPoint]) => {
      
      var mean: BDV[Double] = BDV.zeros(n)
      var m2n: BDV[Double] = BDV.zeros(n) 
      var count :Long = 0L
      while(value.hasNext){
        val line = value.next.features
        count += 1
        for(i <- 0 until n) {
           val index = Col(i)
           val data = line(index)
           val (max,min) = maxAndMin(index)
           if(data != max && data != min){
             val currMean = mean(i) + (data - mean(i)) / count
             val currM2n = m2n(i) +(data - mean(i)) * (data - currMean)
             mean(i) = currMean
             m2n(i) = currM2n 
           }              
        }
      }
      (m2n, mean ,count)
    } 
  
  def getM2n_Mean_CountReduce(v1:(BDV[Double],BDV[Double],Long) ,v2:(BDV[Double],BDV[Double],Long)):(BDV[Double],BDV[Double],Long) = {
    var tempV = v1
    if(v1._3 != 0 && v2._3 != 0){
      val n = v1._1.length
      val deltaMean: BDV[Double] = v1._2 - v2._2
      for(i <- 0 until n){
         val n = v1._3
         val m = v2._3
         if(v2._2(i) != 0){
      	   v1._2(i) = (v1._2(i) * n + v2._2(i) * m) / (m + n)
         }
         if(m + n != 0) {
           v1._1(i) += v2._1(i) + deltaMean(i) * deltaMean(i) * n * m /
           (n + m)    	   
         }
      }
      tempV = v1
    } else if(v1._3 == 0 && v2._3 != 0) tempV = v2
    tempV
  }
  
  val (m2n ,mean, count) = sc.runJob(input, getM2n_Mean_CountMap).reduce(getM2n_Mean_CountReduce)
  
    val realVariance = BDV.zeros[Double](n)

    val denominator = count - 1.0

    if (denominator > 0.0) {
      val deltaMean = mean
      var i = 0
      while (i < mean.size) {
        realVariance(i) = m2n(i)
        realVariance(i) /= denominator
        i += 1
      }
    }
  (realVariance,mean)
 }
  
/**
 *  Z-Trainsformation（z-变换）x' = (x - e)/d , e为平均值，d 为标准差
 **/
  def z_Trainsformation(input:RDD[LabeledPoint],Col:Array[Int],mean:BDV[Double], variance:BDV[Double]):RDD[LabeledPoint]={
    val std = variance.toArray.map(value =>math.sqrt(value))
    val data = input.map(line=>{
      val features = line.features
      val dataStr = for(elem<-Col) yield features(elem)
      val dat = {for(i<- 0 until dataStr.length) yield {
        val tempData = dataStr(i)
        val value = (tempData - mean(i))/std(i)
        if(math.abs(value) < 1) 1.0
        else if(math.abs(value) < 2) 2.0
        else if(math.abs(value) < 3) 3.0
        else if(math.abs(value) < 4) 4.0
        else 5
      }}.toArray
      var temp = line.features.toArray
      for(i<- 0 until Col.length) yield temp(Col(i))= dat(i)
      new LabeledPoint(line.label,Vectors.dense(temp))  
    })
   data
  }
  
  def FindOutlierValueInContinous(Input:RDD[LabeledPoint], classNum :Int = 1 , col:Array[Int]): Array[(Int,HashMap[Double,(Int,Int)])] = {
    val colId = col.filter(_ != targetColId)
    val myresult = Input.mapPartitions(FindOutlierMapContinous(_, classNum ,colId)).reduce(FindOutlierReduce)    
    colId.zip(myresult).toArray
  }
  /**
   * 找到异常分类字段的map阶段
   * */
  def FindOutlierMapContinous(iter:Iterator[LabeledPoint], classNum: Double ,col:Array[Int]):Iterator[Array[HashMap[Double, (Int, Int)]]] = {
    val len = col.length
    val valueMap = new Array[HashMap[Double,(Int,Int)]](len)
    iter.foreach{
      labeled => {
        val label = labeled.label
        val features = labeled.features
        for(i <- 0 until len) {
          if(valueMap(i) == null) valueMap(i) = new HashMap[Double,(Int,Int)]
          val value = features(col(i))
          if(valueMap(i).contains(value)) {
        	 val (rightCnt,totalCnt) = valueMap(i)(value)
             if(label == classNum) {
              valueMap(i).update(value, (rightCnt + 1, totalCnt + 1))
            } else 
              valueMap(i).update(value, (rightCnt, totalCnt + 1))           
          }  else if (label == classNum) valueMap(i)(value) = (1, 1)
            else valueMap(i)(value) = (0, 1)
        }
      }
    }
    Iterator(valueMap)
  }

  
}  
