package com.tehasdf.mapreduce.tools

import java.io.IOException
import scala.collection.JavaConversions.{asScalaBuffer, bufferAsJavaList}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.{InputSplit, Job, JobContext, RecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit}
import org.apache.hadoop.mapreduce.lib.output.{TextOutputFormat, FileOutputFormat}
import org.apache.hadoop.util.GenericOptionsParser
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigFileInputFormat
import com.tehasdf.mapreduce.util.FSSeekableDataInputStream
import com.tehasdf.sstable.{IndexPosition, IndexSummaryReader}
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.NullWritable
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.mapreduce.Reducer

object GenerateSSTableIndexSplits {
  private val Log = LogFactory.getLog(this.getClass())
  case class Chunk(start: Long, len: Long)
  
  trait Mappers[KEYIN, VALUEIN, KEYOUT, VALUEOUT] extends Mapper[KEYIN, VALUEIN, KEYOUT, VALUEOUT] {
    type Context = Mapper[KEYIN, VALUEIN, KEYOUT, VALUEOUT]#Context
  }
  
  trait Reducers[KEYIN, VALUEIN, KEYOUT, VALUEOUT] extends Reducer[KEYIN, VALUEIN, KEYOUT, VALUEOUT] {
    type Context = Reducer[KEYIN, VALUEIN, KEYOUT, VALUEOUT]#Context
  }
  
  class SimpleInputFormat extends PigFileInputFormat[Text, NullWritable] {
    override def listStatus(job: JobContext) = {
      val list = super.listStatus(job)
      list.filter { fs => fs.getPath().getName.endsWith("-Summary.db") }
    }
    
    override def isSplitable(ctx: JobContext, fn: Path) = false
    
    def createRecordReader(split: InputSplit, context: TaskAttemptContext) = {
      new RecordReader[Text, NullWritable] {
        private val Log = LogFactory.getLog(this.getClass())
        private var innerSplit: FileSplit = null
        
        var read: Boolean = false
        def close() {}
        def getProgress() = { if (read) 1.0f else 0.0f }
        def getCurrentValue() = NullWritable.get()
        def getCurrentKey() = {
          read = true
          new Text(innerSplit.getPath().toString())
        }
        def nextKeyValue() = !read
        def initialize(genericSplit: InputSplit, context: TaskAttemptContext) { innerSplit = genericSplit.asInstanceOf[FileSplit] }
      }
    }
  }
  
  class IndexMapper extends Mapper[Text, NullWritable, Text, Text] with Mappers[Text, NullWritable, Text, Text] {
    private val Log = LogFactory.getLog(this.getClass())
    protected override def map(key: Text, value: NullWritable, ctx: Context): Unit = {
      try {
        val conf = ctx.getConfiguration()
        val path = new Path(key.toString())
        val indexPathStr = key.toString().replace("-Summary", "-Index")
        val fs = path.getFileSystem(conf)
        val is = fs.open(path)
        val status = fs.getFileStatus(path)
        val indexStatus = fs.getFileStatus(new Path(indexPathStr))
        val indexSize = indexStatus.getLen()
        
        val dis = new FSSeekableDataInputStream(is, status)
        val reader = new IndexSummaryReader(dis)
        
        val maxSplitSize = conf.getLong("sstable.index.max_split", 0L)
        var prevPos = 0L
        
        val keyText = new Text(indexPathStr)
        Log.info("Reading %s for split".format(key.toString()))
        val rv = reader.foreach { position =>
          val len = position.location - prevPos
          if (len > maxSplitSize) {
            Log.info("Computed split: %s\t%s".format(prevPos, len))
            val valueText = new Text("%s\t%s".format(prevPos, len))
            ctx.write(keyText, valueText)
            prevPos = position.location
          }
        }
        
        val valueText = new Text("%s\t%s".format(prevPos, indexSize - prevPos))
        ctx.write(keyText, valueText)
      } catch {
        case ex: Exception => Log.info("Error: %s".format(ex.toString)); throw ex
      }
    }
  }
  
  def main(rawArgs: Array[String]) {
    val conf = new Configuration
    val args = new GenericOptionsParser(conf, rawArgs).getRemainingArgs()
    conf.setLong("sstable.index.max_split", 1024L*1024*1024)
    val inputPath = new Path(args(0))
    val job = new Job(conf)
    job.setJobName("Generate SSTable Index Splits")
    job.setJarByClass(this.getClass)
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[Text])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])
    job.setNumReduceTasks(0)
    job.setMapperClass(classOf[IndexMapper])
    job.setInputFormatClass(classOf[SimpleInputFormat])
    job.setOutputFormatClass(classOf[TextOutputFormat[_,_]])
    FileInputFormat.setInputPaths(job, inputPath)
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    System.exit(if (job.waitForCompletion(true)) 0 else 1)
  }
}