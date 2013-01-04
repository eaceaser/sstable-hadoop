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
import java.lang.{Iterable => JavaIterable}
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.ArrayWritable
import org.apache.hadoop.mapreduce.Reducer
import com.tehasdf.mapreduce.data.WritableColumn

object ReadSSTableDataFile {
  trait Mappers[KEYIN, VALUEIN, KEYOUT, VALUEOUT] extends Mapper[KEYIN, VALUEIN, KEYOUT, VALUEOUT] {
    type Context = Mapper[KEYIN, VALUEIN, KEYOUT, VALUEOUT]#Context
  }
  
  trait Reducers[KEYIN, VALUEIN, KEYOUT, VALUEOUT] extends Reducer[KEYIN, VALUEIN, KEYOUT, VALUEOUT] {
    type Context = Reducer[KEYIN, VALUEIN, KEYOUT, VALUEOUT]#Context
  }
  
  class SSTableDataMapper extends Mapper[BytesWritable, ArrayWritable, Text, WritableColumn] with Mappers[BytesWritable, ArrayWritable, Text, WritableColumn] {
    override def map(key: BytesWritable, value: ArrayWritable, context: Context) {
      try {
        val str = new String(key.getBytes(), "UTF-8")
        val keytxt = new Text(str)
        value.get.toList.foreach { col =>
          context.write(keytxt, col.asInstanceOf[WritableColumn])
        }
      } catch {
        case ex: Throwable => println(ex); throw ex
      }
    }
  }
  
  class SSTableDataReducer extends Reducer[Text, WritableColumn, Text, Text] with Reducers[Text, WritableColumn, Text, Text] {
    protected override def reduce(key: Text, vals: JavaIterable[WritableColumn], context: Context) {
      try {
        var latestCol: WritableColumn = null
        val it = vals.iterator()
        while (it.hasNext()) {
          val col = it.next()
          if (latestCol == null || col.timestamp.get > latestCol.timestamp.get) latestCol = col
        }
        
        if (latestCol != null) {
          context.write(key, new Text(latestCol.toString))
        }
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
    job.setMapperClass(classOf[SSTableDataMapper])
    job.setReducerClass(classOf[SSTableDataReducer])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[WritableColumn])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])
    FileInputFormat.setInputPaths(job, inputPath)
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    System.exit(if (job.waitForCompletion(true)) 0 else 1)
  }
}