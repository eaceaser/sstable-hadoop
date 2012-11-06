package com.tehasdf.mapreduce.load

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{Text, Writable}
import org.apache.hadoop.mapreduce.InputSplit
import java.io.{DataInput, DataOutput}
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream

// TODO: get rid of these public vars.
case class CompressedSSTableSplit(
    var path: Path,
    var start: Long,
    var length: Long,
    var firstKeyPosition: Long,
    var compressionOffsets: Seq[Long],
    var hosts: Array[String]) extends InputSplit with Writable {
  def this() = this(null, 0L, 0L, 0L, null, null)

  def readFields(in: DataInput) {
    path = new Path(Text.readString(in))
    start = in.readLong()
    length = in.readLong()
    firstKeyPosition = in.readLong()
    val compressionOffsetsLength = in.readInt()
    val offsets = new Array[Long](compressionOffsetsLength)
    for ( i <- 0 until compressionOffsetsLength) {
      offsets(i) = in.readLong()
    }

    compressionOffsets = offsets.toSeq
  }

  def write(out: DataOutput) {
    Text.writeString(out, path.toString())
    out.writeLong(start)
    out.writeLong(length)
    out.writeLong(firstKeyPosition)
    out.writeInt(compressionOffsets.size)
    compressionOffsets.foreach { out.writeLong(_) }
  }

  def getLocations() = hosts
  def getLength() = length

  def toBytes = {
    val baos = new ByteArrayOutputStream
    val dw =  new DataOutputStream(baos)
    write(dw)
    baos.toByteArray()
  }
}
