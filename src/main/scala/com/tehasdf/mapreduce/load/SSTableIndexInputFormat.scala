package com.tehasdf.mapreduce.load

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, TaskAttemptContext}
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigFileInputFormat

import scala.collection.JavaConversions.{asScalaBuffer, bufferAsJavaList}

class SSTableIndexInputFormat extends PigFileInputFormat[Text, LongWritable] {
  override def listStatus(job: JobContext) = {
    val list = super.listStatus(job)
    list.filter { fs =>
      fs.getPath().getName().endsWith("-Index.db")
    }
  }
  override protected def isSplitable(context: JobContext, filename: Path) = false
  def createRecordReader(split: InputSplit, context: TaskAttemptContext) = new SSTableIndexRecordReader
}