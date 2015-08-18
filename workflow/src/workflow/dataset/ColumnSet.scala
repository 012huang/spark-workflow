package workflow.dataset

import scala.collection.mutable.HashMap


/**
 * 存储所有列信息的集合
 * @params ColNum: 存储空间大小
 * */
class ColumnSet(var columnCollect:Array[Column]) extends Serializable{
    
  def length:Int = columnCollect.length
  
  def getNameToIdMap = {
      (getColumnName zip getAllId).toMap
  }
  
/**
 * 名字转ID
 * */  
  def nameToId(name:Array[String]):Array[Int] = {
    val nameMap = getNameToIdMap
    val ids = for(elem <- name) yield {
         var id:Int = 0
         try{
           id = nameMap(elem)               
         } catch {          
           case ex:NoSuchElementException => throw new Exception("在输入数据的字段名中，没有字段名字为 "+elem+"的字段")            
         }
         id
       }
    val idsTemp = ids.distinct
    idsTemp
  }
  
  def nameToId(name:String):Int = {
    val nameMap = getNameToIdMap
    var id:Int = 0
    try{
        id = nameMap(name)               
    }catch {          
        case ex:NoSuchElementException => throw new Exception("在输入数据的字段名中，没有字段名字为 "+name+"的字段")            
    }
    id
  }
    
  
  
  def getIdToNameMap = {
    (getAllId zip getColumnName).toMap    
  }

/**
 * 字段id转字段名字
 * */  
  def idToName(id:Int):String = {
    if(!isEmpty){
      getIdToNameMap(id)
    } else throw new Exception("当前数据集合为空!")
    
  }


  def isEmpty = columnCollect.isEmpty
  def size = length
 
  def getColumn = columnCollect.clone

  /**
   * 获取连续型字段的id
   * */
  def getAllIdOfContinuous:Array[Int] = {
    var col:Array[Int] = Array()
    if(!isEmpty){
      col = for(column <- columnCollect if column.isContinuous) yield column.id 
    } 
    col
  }
  
  /**
   * 获取分类字段的id
   * */
  def getAllIdOfCategorical:Array[Int] = {
    var col:Array[Int] = Array()
    if(!isEmpty){
      col = for(column <- columnCollect if column.isCategorical) yield column.id
    } 
    col
  }
  
  /**
   * 根据所给条件 选出列Id
   * */
  def getIdOfByCondition(f:Column => Boolean): Array[Int] = {
    var col:Array[Int] = Array()
    if(!isEmpty){
      col = for(column <- columnCollect if f(column)) yield column.id
    } 
    col
     
    
  }
  
  def getAllCategoricalInfo:Array[HashMap[String,Int]] = for(column <- columnCollect) yield column.getCategoricalFeaturesInfo
  def getColumnName:Array[String] = for(column <- columnCollect) yield column.columnName
  def getAllId:Array[Int] = for(column <- columnCollect) yield column.id 
  
  def getCategoricalIdAndClassesNumWithoutTarget:Map[Int,Int] = {
    var col:Array[(Int,Int)] = Array()
    if(!isEmpty){
      col = for(column <- columnCollect if column.isCategorical && column.isInput) yield (column.id,column.getCategoricalFeaturesInfo.size)
    } 
    col.toMap    
  }
  
  def getAllCategoricalIdAndClassesNum:Map[Int,Int] = {
    var col:Array[(Int,Int)] = Array()
    if(!isEmpty){
      col = for(column <- columnCollect if column.isCategorical) yield (column.id,column.getCategoricalFeaturesInfo.size)
    } 
    col.toMap    
    
  }
  /**
   * 获取输入字段
   * */
  def getAllIdOfInput:Array[Int] = {
    var col:Array[Int] = Array()
    if(!isEmpty){
       col = for(column <- columnCollect if column.isInput) yield column.id
    }
    col
  }
  
  /**
   * 获取目标字段
   * */
  def getIdOfTarget:Int = {
    var col:Array[Int] = Array()
    if(!isEmpty){
       col = for(column <- columnCollect if column.isTarget) yield column.id
    }
    if(col.isEmpty) throw new Exception("目标列为空，请设置目标列！")
    col(0)
    
  }

  
/**
 * 获得给定字段id的最小值
 * */
  def getMinWithColumnId(ColId:Array[Int]): Array[(Int,Double)] = {
    var minArr = Array[(Int,Double)]()
    if(!isEmpty && !ColId.isEmpty){
      minArr = for(elem <- ColId) yield (elem,getMaxWithColumnId(elem))
    }
    minArr
  }
  
  def getMinWithColumnId(ColId:Int):Double = columnCollect(ColId).getMin
  
  
/**
 * 获得给定字段id的最小值
 * */ 
  def getMaxWithColumnId(ColId:Array[Int]): Array[(Int,Double)] = {
    var minArr = Array[(Int,Double)]()
    if(!isEmpty && !ColId.isEmpty){
      minArr = for(elem <- ColId) yield (elem,getMaxWithColumnId(elem))
    }
    minArr
  }
  
  def getMaxWithColumnId(ColId:Int):Double = {
    if(columnCollect(ColId).isAvlidStatis){
      columnCollect(ColId).getMax
    }else{
      throw new Exception("无法获取最大值，最小值，请先计算统计信息")
    } 
      
  }
 
/**
 * 获得给定字段id的分类信息
 * */  
  def getCategoricalInfoById(ColId:Array[Int]):Array[(Int, HashMap[String,Int])] = {
    var minArr = Array[(Int, HashMap[String,Int])]()
    if(!isEmpty && !ColId.isEmpty){
      minArr = for(elem <- ColId) yield (elem,getCategoricalInfoById(elem))
    }
    minArr
    
  }
  
  def getCategoricalInfoById(ColId:Int):HashMap[String,Int] = {
    columnCollect(ColId).getCategoricalFeaturesInfo
  }
  
  
  
/**
 * 更改字段角色 ， 设置输入角色 
 * */
  def setInputRole(names:Array[String])  {
    val col = nameToId(names)
    for(elem <- col) {
      columnCollect(elem).setRoleInput
    }
  }
  
  def setTargetRole(names:String) {
    val col = nameToId(names)
    columnCollect(col).setRoleTarget
  }
  
  def setVoidRole(names:Array[String]) {
    val col = nameToId(names)
    for(elem <- col) columnCollect(elem).setRoleVoid
  }
  
/**
 * 设置字段类别属性
 * */
  
  def setCategoricalColumn(columnName:Array[String]){
    val col = nameToId(columnName)
    for(elem<- col) columnCollect(elem).setDataTypeCategorical
  }
  
  def setContinuousColumn(columnName:Array[String]){
    val col = nameToId(columnName)
    for(elem<- col) columnCollect(elem).setDataTypeContinuous
    
  }
  
  def setIDColumn(columnName:Array[String]){
    val col = nameToId(columnName)
    for(elem<- col) columnCollect(elem).setDataTypeID
    
  }
  
  def setAllColumnToContinuous = if(!isEmpty) columnCollect.map(_.setDataTypeContinuous)
  def setAllColumnToInput = if(!isEmpty) columnCollect.map(_.setRoleInput)
  
  def setCategoricalFeaturesInfo(idWithMap:Array[(Int,HashMap[String,Int])]) = {
    if(!isEmpty){
       for((id,map)<- idWithMap) {
         columnCollect(id).setCategoricalFeaturesInfo(map)
         columnCollect(id).setCategoricalNum(map.size)
       }
    }
  }
  
/**
 * 更换所有的column
 * */
  def changeAllColumn(addAll:Array[Column]) = {
    columnCollect = addAll
  }
  
  def addColumn(addCol:Column):Boolean = {
	   columnCollect ++= Array(addCol)
	   true
      }
  
  
  def remove(id:Int):Boolean = {
    var length = this.length
    if(length > 0){
      var j = 0
      while (j< length && columnCollect(j).id != id) j += 1
      if(j < length){
        while (j< length-1) columnCollect(j) = columnCollect(j+1)
        length -= 1
      }
      true
    } else {
      false
    }
  }
  
  
  def refreshColumnId():this.type = {
    var i = 0 
    while(i < length) {
      columnCollect(i).id = i
      i += 1
    }
    this
  }
  
  
  /**
   * 过滤字段表
   * */
  def filter(f:Column => Boolean):this.type = {
     columnCollect = columnCollect.filter(f)    
     this
  }
  
  def filterById(stayCol:Array[Int]):this.type = {
    filter(
        col =>
          stayCol.contains(col.id)
      )
    this
  }
  
  def judgeType(index:Int,f:Column => Boolean):Boolean = {
    f(columnCollect(index))
  }
  
  def isExistTarget:Boolean = columnCollect.map(_.isTarget).reduce(_ || _)
  
  def merge(other:ColumnSet):this.type = {
    columnCollect ++= other.columnCollect
    for(i<- 0 until columnCollect.length) {
      columnCollect(i).id = i
    }
    this
  }
  
  override def clone:ColumnSet = {
    new ColumnSet(columnCollect.map(_.clone))
  }
}

object ColumnSet extends Serializable{
  
  def ColumnSetBuilder(Columns:Array[Column]):ColumnSet = {
      new ColumnSet(Columns)
  }
  
}
