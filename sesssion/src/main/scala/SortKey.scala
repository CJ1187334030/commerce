
case class SortKey(clickCount:Long,order:Long,pay:Long) extends Ordered[SortKey]{


  override def compare(that: SortKey): Int = {

    if (this.clickCount - that.clickCount !=0)
      return (this.clickCount - that.clickCount).toInt
    else if(this.order - that.order != 0 )
      return (this.order - that.order).toInt
    else
      return (this.pay - that.pay).toInt

  }

}
