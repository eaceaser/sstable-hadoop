package com.tehasdf.mapreduce.load

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.JobContext
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.LongWritable

class SSTableIndexInputFormat extends FileInputFormat[Text, LongWritable] {
  override protected def isSplitable(context: JobContext, filename: Path) = false
  def createRecordReader(split: InputSplit, context: TaskAttemptContext) = new SSTableIndexRecordReader
}