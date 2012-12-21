package com.tehasdf.mapreduce.load

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{Text, Writable}
import org.apache.hadoop.mapreduce.InputSplit
import java.io.{DataInput, DataOutput}
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import org.apache.hadoop.io.LongWritable

case class SSTableDataSplit(var path: Path, var fileOffset: LongWritable, var fileLength: LongWritable, var innerOffset: LongWritable, var innerLength: LongWritable, var hosts: Array[String]) extends InputSplit with Writable {
  def this(p: Path, fo: Long, fl: Long, ino: Long, il: Long, h: Array[String]) = this(p, new LongWritable(fo), new LongWritable(fl), new LongWritable(ino), new LongWritable(il), h)
  def this() = this(null, null, null, null, null, null)
  def getLength() = fileLength.get
  def getLocations = hosts
  
  def readFields(in: DataInput) {
    path = new Path(Text.readString(in))
    val fo = new LongWritable
    fo.readFields(in)
    fileOffset = fo
    val fl = new LongWritable
    fl.readFields(in)
    fileLength = fl
    val os = new LongWritable
    os.readFields(in)
    innerOffset = os
    val il = new LongWritable
    il.readFields(in)
    innerLength = il
  }
  
  def write(out: DataOutput) {
    Text.writeString(out, path.toString())
    fileOffset.write(out)
    fileLength.write(out)
    innerOffset.write(out)
    innerLength.write(out)
  }
}
