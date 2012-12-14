package com.tehasdf.mapreduce.tools

import java.io.IOException
import java.lang.{Iterable => JavaIterable}
import scala.collection.JavaConversions.{asScalaBuffer, seqAsJavaList}
import scala.collection.mutable
import org.apache.commons.codec.binary.Base64
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit}
import org.apache.hadoop.mapreduce.lib.output.{TextOutputFormat, FileOutputFormat}
import org.apache.hadoop.util.GenericOptionsParser
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil
import com.tehasdf.mapreduce.load.SSTableIndexInputFormat
import com.tehasdf.sstable.CompressionInfoReader

object GenerateSSTableDataSplits {
  private val Log = LogFactory.getLog(this.getClass())
  trait Mappers[KEYIN, VALUEIN, KEYOUT, VALUEOUT] extends Mapper[KEYIN, VALUEIN, KEYOUT, VALUEOUT] {
    type Context = Mapper[KEYIN, VALUEIN, KEYOUT, VALUEOUT]#Context
  }
  
  trait Reducers[KEYIN, VALUEIN, KEYOUT, VALUEOUT] extends Reducer[KEYIN, VALUEIN, KEYOUT, VALUEOUT] {
    type Context = Reducer[KEYIN, VALUEIN, KEYOUT, VALUEOUT]#Context
  }
  
  class IndexMapper extends Mapper[Text, LongWritable, LongWritable, LongWritable] with Mappers[Text, LongWritable, LongWritable, LongWritable] {
    private val Log = LogFactory.getLog(this.getClass())
    private val codec = new Base64(false)
    
    protected override def map(key: Text, value: LongWritable, context: Context): Unit = {
      val split = context.getInputSplit().asInstanceOf[FileSplit]
      val indexName = split.getPath().getName()
      val rootName = indexName.stripSuffix("-Index.db")
      val chunkKey = "sstable.%s.chunk.length".format(rootName)
      val conf = context.getConfiguration()
      val chunkLength = context.getConfiguration().getLong(chunkKey, 0L)
      if (chunkLength == 0L) throw new IOException("Could not find compressionInfo for %s.".format(rootName))
      val chunkIndex = value.get() / chunkLength
      try {
        context.write(new LongWritable(chunkIndex), value)
      } catch {
        case ex: Exception => println("huh????: %s".format(ex)); throw ex
      }
    }
  }
  
  class DataSplitReducer extends Reducer[LongWritable, LongWritable, LongWritable, LongWritable] with Reducers[LongWritable, LongWritable, LongWritable, LongWritable]{
    protected override def reduce(key: LongWritable, vals: JavaIterable[LongWritable], context: Context) {
      val it = vals.iterator()
      val stuff = new mutable.ArrayBuffer[Long]()
      while (it.hasNext()) {
        val nextValue = it.next()
        stuff.append(nextValue.get())
      }
      val sorted = stuff.sorted
      context.write(new LongWritable(sorted.head), new LongWritable(sorted.last))
/*      try {
        val sortedVals = vals.toList.sortWith(_.get() < _.get())
        sortedVals.headOption.foreach { head =>
          sortedVals.lastOption.foreach { tail =>
            context.write(head, tail)
          }
        }
      } catch {
        case ex: Exception => println("huh2???: %s".format(ex))
      } */
    }
  }
  
  def computeCompressionInfo(file: Path, conf: Configuration) {
    val fname = file.getName().stripSuffix("-CompressionInfo.db")
    val fs = file.getFileSystem(conf)
    val is = fs.open(file)
    val reader = new CompressionInfoReader(is)
    Log.info("Setting compression info for %s: %s".format(fname, reader.chunkLength))
    conf.setLong("sstable.%s.chunk.length".format(fname), reader.chunkLength)
  }
  
  def crawlForCompressionInfo(root: Path, conf: Configuration) {
    val fs = root.getFileSystem(conf)
    MapRedUtil.getAllFileRecursively(List(fs.getFileStatus(root)), conf).filter(_.getPath().getName().endsWith("-CompressionInfo.db")).foreach { fi =>
  	  computeCompressionInfo(fi.getPath(), conf)
    }
  }
  
  def main(rawArgs: Array[String]) {
    val conf = new Configuration
    conf.setInt("mapreduce.job.max.split.locations", 150)
    val args = new GenericOptionsParser(conf, rawArgs).getRemainingArgs()
    val inputPath = new Path(args(0))
    crawlForCompressionInfo(inputPath, conf)

    val job = new Job(conf)
    job.setJobName("Generate SSTable Data Splits")
    job.setJarByClass(this.getClass())
    job.setMapOutputKeyClass(classOf[LongWritable])
    job.setMapOutputValueClass(classOf[LongWritable])
    job.setMapperClass(classOf[IndexMapper])
    job.setReducerClass(classOf[DataSplitReducer])
    job.setInputFormatClass(classOf[SSTableIndexInputFormat])
    job.setNumReduceTasks(128)
    job.setOutputFormatClass(classOf[TextOutputFormat[_,_]])
    
    FileInputFormat.setInputPaths(job, inputPath)
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    FileInputFormat.setMaxInputSplitSize(job, 10L*1024*1024)
    System.exit(if (job.waitForCompletion(true)) 0 else 1)
  }
}
