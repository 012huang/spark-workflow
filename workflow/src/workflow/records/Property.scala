package workflow.records


/**
 * <p>
 *    A generic name=value string pair
 * </p>
 * 
 * @author <a href="https://github.com/three4clavin">Sander Gates</a>
 *
 */
 class Property[T](
	   private var name :String = null,
	   private var value:T = null
       ) {
	
	/**
	 * <p>
	 *    Returns name of property
	 * </p>
	 * 
	 * @return Name of property
	 */
	def getName():String = name
	
	/**
	 * <p>
	 *    Sets the name of this property
	 * </p>
	 * 
	 * @param name Name of property
	 */
	def setName(name:String){
		this.name = name;
	}
	
	/**
	 * <p>
	 *    Gets the value of this property
	 * </p>
	 * 
	 * @return Value of this property
	 */
	def getValue():T = value
	
	/**
	 * <p>
	 *    Sets the value of this property
	 * </p>
	 * 
	 * @param value Value of this property
	 */
	def setValue(value: T){
		this.value = value
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	override def toString():String = {
		"[Property name='" + name + "' value='" + value + "']";
	}
}
