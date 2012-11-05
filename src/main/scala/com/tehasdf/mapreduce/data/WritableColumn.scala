package com.tehasdf.mapreduce.data

import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.Writable
import java.io.DataInput
import java.io.DataOutput

object ColumnState extends Enumeration {
  val Normal, Deleted = Value
}

object WritableColumn {
  def deleted(name: BytesWritable) = WritableColumn(ColumnState.Deleted, name, null)
}

case class WritableColumn(var state: ColumnState.Value, var name: BytesWritable, var data: BytesWritable) extends Writable {
  def this() = this(null, null, null)

  def readFields(in: DataInput) {
    state = ColumnState(in.readInt())
    val nameLength = in.readInt()
    val nameBuf = new Array[Byte](nameLength)
    in.readFully(nameBuf)
    name = new BytesWritable(nameBuf)

    state match {
      case ColumnState.Normal =>
        val dataLength = in.readInt()
        val dataBuf = new Array[Byte](dataLength)
        in.readFully(dataBuf)
        data = new BytesWritable(dataBuf)
      case ColumnState.Deleted =>
    }
  }

  def write(out: DataOutput) {
    out.writeInt(state.id)
    out.writeInt(name.getLength())
    out.write(name.getBytes())

    state match {
      case ColumnState.Normal =>
        out.writeInt(data.getLength())
        out.write(data.getBytes())
      case ColumnState.Deleted =>
    }
  }
}