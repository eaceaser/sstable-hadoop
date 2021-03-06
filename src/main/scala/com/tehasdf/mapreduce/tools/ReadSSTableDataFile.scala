package com.tehasdf.mapreduce.tools

import com.tehasdf.mapreduce.data.WritableColumn
import com.tehasdf.mapreduce.load.{SplitSSTableDataInputFormat, SSTableDataInputFormat}
import com.tehasdf.mapreduce.mapred.{SSTableDataReducer, SSTableDataMapper}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.GenericOptionsParser

object ReadSSTableDataFile {
  def main(rawArgs: Array[String]) {
    val conf = new Configuration
    val args = new GenericOptionsParser(conf, rawArgs).getRemainingArgs
    val inputPath = new Path(args(0))
    conf.set("sstable.split.dir", args(2))
    
    val job = new Job(conf)
    job.setJobName("Read SSTable Data File")
    job.setJarByClass(this.getClass)
    job.setInputFormatClass(classOf[SplitSSTableDataInputFormat])
    job.setMapperClass(classOf[SSTableDataMapper])
    job.setReducerClass(classOf[SSTableDataReducer])
    job.setNumReduceTasks(512)
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[WritableColumn])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])
    FileInputFormat.setInputPaths(job, inputPath)
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    System.exit(if (job.waitForCompletion(true)) 0 else 1)
  }
}