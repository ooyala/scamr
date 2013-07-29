package scamr.io.tuples

import java.io.{DataOutput, DataInput}
import org.apache.hadoop.io.{WritableComparable, Writable}

abstract class Tuple2WritableComparable[A <: Writable with Comparable[_], B <: Writable with Comparable[_]]
    (val tuple: (A, B))
extends Writable
with Comparable[Tuple2WritableComparable[A, B]]
with WritableComparable[Tuple2WritableComparable[A, B]] {

  def _1: A = tuple._1
  def _2: B = tuple._2

  override def write(dataOutput: DataOutput) {
    tuple._1.write(dataOutput)
    tuple._2.write(dataOutput)
  }

  override def readFields(dataInput: DataInput) {
    tuple._1.readFields(dataInput)
    tuple._2.readFields(dataInput)
  }

  override def compareTo(that: Tuple2WritableComparable[A, B]): Int = {
    val cmp1 = this.tuple._1.asInstanceOf[Comparable[A]].compareTo(that.tuple._1)
    if (cmp1 != 0) cmp1 else this.tuple._2.asInstanceOf[Comparable[B]].compareTo(that.tuple._2)
  }

  override def hashCode(): Int = tuple.hashCode

  override def equals(that: Any): Boolean =
    if (this.eq(that.asInstanceOf[AnyRef])) {
      true
    } else if (that == null || this.getClass != that.getClass) {
      false
    } else {
      try {
        this.tuple == that.asInstanceOf[Tuple2WritableComparable[A, B]].tuple
      } catch {
        case e: ClassCastException => false
      }
    }

  override def toString: String = tuple.toString
}
