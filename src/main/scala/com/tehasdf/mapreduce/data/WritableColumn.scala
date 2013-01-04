package com.tehasdf.mapreduce.data

import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.Writable
import java.io.DataInput
import java.io.DataOutput
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.WritableComparable

object ColumnState extends Enumeration {
  val Normal, Deleted = Value
}

object WritableColumn {
  def deleted(name: BytesWritable, timestamp: LongWritable) = WritableColumn(ColumnState.Deleted, name, null, timestamp)
}

case class WritableColumn(var state: ColumnState.Value, var name: BytesWritable, var data: BytesWritable, var timestamp: LongWritable) extends WritableComparable[WritableColumn] {
  def this() = this(null, null, null, null)

  def readFields(in: DataInput) {
    state = ColumnState(in.readInt())
    val n = new BytesWritable
    n.readFields(in)
    name = n
    
    state match {
      case ColumnState.Normal =>
        val d = new BytesWritable
        d.readFields(in)
        data = d
      case ColumnState.Deleted =>
    }
    
    val ts = new LongWritable
    ts.readFields(in)
    timestamp = ts
  }

  def write(out: DataOutput) {
    out.writeInt(state.id)
    name.write(out)

    state match {
      case ColumnState.Normal =>
        data.write(out)
      case ColumnState.Deleted =>
    }
    timestamp.write(out)
  }
  
  def compareTo(other: WritableColumn) = {
    timestamp.compareTo(other.timestamp)
  }
  
  override def toString() = {
    state match {
      case ColumnState.Normal => "Column("+name+","+data+","+timestamp+")"
      case ColumnState.Deleted => "Deleted("+name+")"
    }
  }
}