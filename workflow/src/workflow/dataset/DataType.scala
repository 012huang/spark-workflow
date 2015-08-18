package workflow.dataset

/**
 * 枚举来选择数据的输入类型是连续 ，还是分了
 **/
object DataType extends Enumeration{
  type DataType = Value  
  val Continuous, Categorical, ID = Value
  
  private[dataset] def fromString(name:String):DataType = name match{
    case "Continuous" => Continuous
    case "Categorical" => Categorical
    case "ID" => ID
    case _ => throw new IllegalArgumentException(s"无法识别的数据类型:${name}")
  }
  

}


/**
 *枚举来确定数据的存储类型
 **/
object StoreType extends Enumeration{
  type StoreType = Value  
  val IntType, DoubleType, StringType = Value
  
  private[dataset] def fromString(name:String):StoreType = name match{
    case "Int" => IntType
    case "Double" => DoubleType
    case "String" => StringType
    case _ => throw new IllegalArgumentException(s"无法识别的数据类型:${name}")
  }
  
}  
/**
 *枚举来确定字段角色分配
 **/  
object RoleType extends Enumeration {
  type Role = Value
  val Target, Input ,Void = Value
  
  private[dataset] def fromString(name:String): Role = name match {
    case "Target" => Target
    case "Input" => Input
    case "Void" => Void
    case _ => throw new IllegalArgumentException(s"无法识别的数据类型:${name}")
  }
}

