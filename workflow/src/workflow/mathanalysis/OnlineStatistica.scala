package workflow.mathanalysis

import breeze.linalg.{DenseVector => BDV}
import org.apache.spark.mllib.linalg.{Vectors,Vector}
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus

/**运用增量法获取字段的基础统计量，包含，平均值，方差，最大值，最小值，
 * 空值个数，非数值个数，0值个数，和唯一值个数
 * 
 * */

class OnlineStatistica extends Serializable{
    
  private var n         :Int         = 0
  private var totalCnt  :Long        = 0           
  private var currMean  :BDV[Double] = _     
  private var currM2n   :BDV[Double] = _      
  private var currMax   :BDV[Double] = _      
  private var currMin   :BDV[Double] = _               
  private var num       :BDV[Double] = _ 
  private var notNum    :BDV[Double] = _ 
  private var uniqueNum :BDV[HyperLogLogPlus] = _ 
  
  def add(sample:Vector): this.type = {
    if(n == 0){
      n = sample.size
      
      currMean = BDV.zeros[Double](n)
      currM2n  = BDV.zeros[Double](n)
      currMax  = BDV.fill(n)(Double.MinValue)
      currMin  = BDV.fill(n)(Double.MaxValue)
      num      = BDV.zeros[Double](n)
      notNum   = BDV.zeros[Double](n)
      
      val p = math.ceil(2.0 * math.log(1.054 / 0.005) / math.log(2)).toInt
      uniqueNum = BDV.fill(n)(new HyperLogLogPlus(p,0))
    }
    
    require(n == sample.size, s"Dimensions mismatch when adding new sample." +
      s" Expecting $n but got ${sample.size}.")
      
    totalCnt += 1
    for(i <- 0 until n){
         val value = sample(i)
         if(value > currMax(i)) currMax(i) = value
         if(value < currMin(i)) currMin(i) = value
         if(value != 0.0) {
           num(i) += 1
    	   val tmpPrevMean = currMean(i)
           currMean(i) += (value - currMean(i)) / num(i) 
           currM2n(i) += (value - currMean(i)) * (value - tmpPrevMean)
         }
         
        uniqueNum(i).offer(sample(i)) 
    }
    
    this
  }
  

  def add[T](sample:Array[T]): this.type = {
    if(n == 0){
      n = sample.size
      
      currMean = BDV.zeros[Double](n)
      currM2n = BDV.zeros[Double](n)
      currMax = BDV.fill(n)(Double.MinValue)
      currMin = BDV.fill(n)(Double.MaxValue)
      num = BDV.zeros[Double](n)
      notNum   = BDV.zeros[Double](n)

      
      val p = math.ceil(2.0 * math.log(1.054 / 0.005) / math.log(2)).toInt
      uniqueNum = BDV.fill(n)(new HyperLogLogPlus(p,0))
    }
    
    require(n == sample.size, s"新加入的样本维度不匹配，" +
      s" 应该是 $n 确是 ${sample.size} 样本为:"+ sample.mkString("[",",","]"))
      
    totalCnt += 1
    for(i <- 0 until n){
         var value:Double = 0
         sample(i) match{
           case e:String => 
             try{
               value = e.toDouble
             } catch {
               case ex:NumberFormatException => value = 0
               notNum(i) += 1
             }
           case e:Double => value = e
           case e:Int => value = e
           case _ => throw new Exception("计算基础统计量输入格式有误")
         }
         if(value > currMax(i)) currMax(i) = value
         if(value < currMin(i)) currMin(i) = value
         if(value != 0.0) {
           num(i) += 1
    	   val tmpPrevMean = currMean(i)
           currMean(i) += (value - currMean(i)) / num(i) 
           currM2n(i) += (value - currMean(i)) * (value - tmpPrevMean)
         }
         
        uniqueNum(i).offer(sample(i)) 
    }
    
    this
  }
  
  
  
  def merge(other: OnlineStatistica): this.type = {
    if(this.totalCnt != 0 && other.totalCnt != 0){
      require(n == other.n, s"Dimensions mismatch when merging with another summarizer. " +
        s"Expecting $n but got ${other.n}.")
      
      totalCnt += other.totalCnt
      val deltaMean: BDV[Double] = currMean - other.currMean

      for(i <- 0 until n){
         val n = num(i)
         val m = other.num(i)
    	 if(other.currMean(i) != 0){
    	   currMean(i) = (currMean(i) * n + other.currMean(i) * m) / (m + n)
    	 }
    	 if(m + n != 0) {
          currM2n(i) += other.currM2n(i) + deltaMean(i) * deltaMean(i) * n * m /
            (n + m)
    	   
    	 }
    	   
    	 if(currMax(i) < other.currMax(i)) currMax(i) = other.currMax(i)
    	 if(currMin(i) > other.currMin(i)) currMin(i) = other.currMin(i)
    	 

    	 num(i)    += other.num(i)
    	 notNum(i) += other.notNum(i)
    	 uniqueNum(i).addAll(other.uniqueNum(i))      
      }
      
    } else if(this.totalCnt == 0 && other.totalCnt != 0){
      this.n         = other.n
      this.totalCnt  = other.totalCnt
      this.currMean  = other.currMean.copy
      this.currM2n   = other.currM2n.copy
      this.currMax   = other.currMax.copy
      this.currMin   = other.currMin.copy
      this.num       = other.num.copy
      this.notNum    = other.notNum
      this.uniqueNum = other.uniqueNum.copy
    }
    
    this
  }
  
  def columnNum:Int = n
  
  def count: Long = totalCnt
  
  def means: Vector = {

    val realMean = BDV.zeros[Double](n)
    var i = 0
    while (i < n) {
      realMean(i) = currMean(i) * (num(i) / totalCnt)
      i += 1
    }

    Vectors.dense(realMean.data)

  }
  
  def variance: Vector = {

    val realVariance = BDV.zeros[Double](n)

    val denominator = totalCnt - 1.0

    if (denominator > 0.0) {
      val deltaMean = currMean
      var i = 0
      while (i < currM2n.size) {
        realVariance(i) =
          currM2n(i) + deltaMean(i) * deltaMean(i) * num(i) * (totalCnt - num(i)) / totalCnt
        realVariance(i) /= denominator
        i += 1
      }
    }
    
    Vectors.dense(realVariance.data)    
  }
  
  def max: Vector = {

    var i = 0
    while (i < n) {
      if ((num(i) < totalCnt) && (currMax(i) < 0.0)) currMax(i) = 0.0
      i += 1
    }
    Vectors.dense(currMax.data)     
  }
  
  def min: Vector = {
    var i = 0
    while (i < n) {
      if ((num(i) < totalCnt) && (currMin(i) > 0.0)) currMin(i) = 0.0
      i += 1
    }

      Vectors.dense(currMin.data)     
  }
    
  
  def zeroNumCount: Vector = {
    val zeroNum = BDV.zeros[Double](n)
    
    var i = 0
    while(i < n){
      zeroNum(i) = totalCnt - num(i)
      i += 1
    }
    
    Vectors.dense(zeroNum.data)        
  }
  
  def uniqueNumCount: Array[Long] = {
      uniqueNum.map(_.cardinality).data
  }
  
  def notNumCount: Vector = {
    Vectors.dense(notNum.data)
  }
  
}
