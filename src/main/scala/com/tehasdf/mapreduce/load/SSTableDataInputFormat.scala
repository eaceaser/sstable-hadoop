package com.tehasdf.mapreduce.load

import com.twitter.mapreduce.load.SSTableDataRecordReader

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{MapWritable, Text}
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, TaskAttemptContext}
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigFileInputFormat
import scala.collection.JavaConversions._

class SSTableDataInputFormat extends PigFileInputFormat[Text, MapWritable] {
  def createRecordReader(split: InputSplit, context: TaskAttemptContext) = new SSTableDataRecordReader

  override def isSplitable(context: JobContext, filename: Path) = false
  override def listStatus(job: JobContext) = {
    val list = super.listStatus(job)
    list.filterNot { fs =>
      fs.getPath().getName().endsWith("-CompressionInfo.db")
    }
  }


}
