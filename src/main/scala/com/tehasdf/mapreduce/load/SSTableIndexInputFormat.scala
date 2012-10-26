package com.tehasdf.mapreduce.load

import org.apache.hadoop.fs.Path

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, TaskAttemptContext}
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigFileInputFormat

class SSTableIndexInputFormat extends PigFileInputFormat[Text, LongWritable] {
  override protected def isSplitable(context: JobContext, filename: Path) = false
  def createRecordReader(split: InputSplit, context: TaskAttemptContext) = new SSTableIndexRecordReader
}