package com.tehasdf.mapreduce.tools

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.GenericOptionsParser
import org.apache.hadoop.mapreduce.Job
import com.tehasdf.mapreduce.load.SSTableDataInputFormat
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.ArrayWritable

object ReadSSTableDataFile {
  trait Mappers[KEYIN, VALUEIN, KEYOUT, VALUEOUT] extends Mapper[KEYIN, VALUEIN, KEYOUT, VALUEOUT] {
    type Context = Mapper[KEYIN, VALUEIN, KEYOUT, VALUEOUT]#Context
  }
  
  class StupidMapper extends Mapper[BytesWritable, ArrayWritable, Text, Text] with Mappers[BytesWritable, ArrayWritable, Text, Text] {
    override def map(key: BytesWritable, value: ArrayWritable, context: Context) {
      try {
        val str = new String(key.getBytes(), "UTF-8")
        val blah = value.toStrings().toList
        context.write(new Text(str), new Text(blah.toString))
      } catch {
        case ex: Throwable => println(ex); throw ex
      }
    }
  }
  
  def main(rawArgs: Array[String]) {
    val conf = new Configuration
    conf.set("sstable.split.dir", "/user/eac/test-out")
    val args = new GenericOptionsParser(conf, rawArgs).getRemainingArgs()
    val inputPath = new Path(args(0))
    
    val job = new Job(conf)
    job.setJobName("Read SSTable Data File")
    job.setJarByClass(this.getClass())
    job.setInputFormatClass(classOf[SSTableDataInputFormat])
    job.setMapperClass(classOf[StupidMapper])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[Text])
    FileInputFormat.setInputPaths(job, inputPath)
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    System.exit(if (job.waitForCompletion(true)) 0 else 1)
  }
}