package com.tehasdf.mapreduce.load

import com.twitter.mapreduce.load.SSTableDataRecordReader
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.ArrayWritable
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.mapreduce.{RecordReader, JobContext, InputSplit, TaskAttemptContext}
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigFileInputFormat
import scala.collection.JavaConversions._
import com.tehasdf.mapreduce.mapred.ColumnArrayWritable

class SSTableDataInputFormat extends PigFileInputFormat[BytesWritable, ColumnArrayWritable] {
  override def listStatus(job: JobContext) = {
    val list = super.listStatus(job)
    list.filter { fs =>
      fs.getPath.getName.endsWith("-Data.db")
    }
  }

  def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[BytesWritable, ColumnArrayWritable] = new SSTableDataRecordReader
  override def isSplitable(context: JobContext, filename: Path) = false
}