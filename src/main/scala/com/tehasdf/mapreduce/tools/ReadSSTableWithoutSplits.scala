package com.tehasdf.mapreduce.tools

import com.tehasdf.mapreduce.data.WritableColumn
import com.tehasdf.mapreduce.load.SSTableDataInputFormat
import com.tehasdf.mapreduce.mapred._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{ArrayWritable, BytesWritable, NullWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.GenericOptionsParser

object ReadSSTableWithoutSplits {
  def main(rawArgs: Array[String]) {
    val conf = new Configuration
    val args = new GenericOptionsParser(conf, rawArgs).getRemainingArgs
    val inputPath = new Path(args(0))
    val job = new Job(conf)
    job.setJobName("Read SSTableDataFile")
    job.setJarByClass(this.getClass)
    job.setInputFormatClass(classOf[SSTableDataInputFormat])
    job.setMapperClass(classOf[GroupedSSTableDataMapper])
    job.setReducerClass(classOf[MsgPackSSTableDataReducer])
    job.setNumReduceTasks(128)
    job.setMapOutputKeyClass(classOf[BytesWritable])
    job.setMapOutputValueClass(classOf[ColumnArrayWritable])
    job.setOutputKeyClass(classOf[NullWritable])
    job.setOutputValueClass(classOf[Text])
    FileInputFormat.setInputPaths(job, inputPath)
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    System.exit(if (job.waitForCompletion(true)) 0 else 1)
  }
}