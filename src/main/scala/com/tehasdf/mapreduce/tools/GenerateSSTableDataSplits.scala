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
import org.apache.hadoop.io.WritableComparable
import java.io.DataInput
import java.io.DataOutput
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs
import org.apache.hadoop.io.ArrayWritable

object GenerateSSTableDataSplits {
  private val Log = LogFactory.getLog(this.getClass())
  trait Mappers[KEYIN, VALUEIN, KEYOUT, VALUEOUT] extends Mapper[KEYIN, VALUEIN, KEYOUT, VALUEOUT] {
    type Context = Mapper[KEYIN, VALUEIN, KEYOUT, VALUEOUT]#Context
  }
  
  trait Reducers[KEYIN, VALUEIN, KEYOUT, VALUEOUT] extends Reducer[KEYIN, VALUEIN, KEYOUT, VALUEOUT] {
    type Context = Reducer[KEYIN, VALUEIN, KEYOUT, VALUEOUT]#Context
  }
  
  case class ChunkIndex(var file: Text, var index: LongWritable, var rootPosition: LongWritable, var chunkLength: LongWritable, var totalUncompressedSize: LongWritable, var compressionPositions: Array[Long]) extends WritableComparable[ChunkIndex] {
    def this() = this(null, null, null, null, null, null)
    def this(f: String, i: Long, r: Long, len: Long, unc: Long, pos: Array[Long]) = this(new Text(f), new LongWritable(i), new LongWritable(r), new LongWritable(len), new LongWritable(unc), pos)
    
    def compareTo(other: ChunkIndex) = {
      val cmp = file.compareTo(other.file)
      if (cmp == 0) {
        index.compareTo(other.index)
      } else {
        cmp
      }
    }
    
    def readFields(in: DataInput) {
      val t = new Text
      t.readFields(in)
      val i = new LongWritable
      i.readFields(in)
      file = t
      index = i
      val rp = new LongWritable
      rp.readFields(in)
      val len = new LongWritable
      len.readFields(in)
      chunkLength = len
      rootPosition = rp
      val unc = new LongWritable
      unc.readFields(in)
      totalUncompressedSize = unc
      val aw = new ArrayWritable(classOf[LongWritable])
      aw.readFields(in)
      compressionPositions = aw.get().map(_.asInstanceOf[LongWritable].get())
    }
    
    def write(out: DataOutput) {
      file.write(out)
      index.write(out)
      rootPosition.write(out)
      chunkLength.write(out)
      totalUncompressedSize.write(out)
      val aw = new ArrayWritable(classOf[LongWritable])
      aw.set(compressionPositions.map(new LongWritable(_)))
      aw.write(out)
    }
  }
  
  class IndexMapper extends Mapper[Text, LongWritable, ChunkIndex, LongWritable] with Mappers[Text, LongWritable, ChunkIndex, LongWritable] {
    private val Log = LogFactory.getLog(this.getClass())
    private val codec = new Base64(false)
    
    private var chunkPositions: Array[Long] = null
    private var splits: Array[Array[Long]] = null
    private var splitName: String = null
    private var splitLength: Long = 0L
    
    protected override def setup(context: Context): Unit = try {
      val split = context.getInputSplit().asInstanceOf[FileSplit]
      val indexName = split.getPath().getName()
      val rootName = indexName.stripSuffix("-Index.db")
      splitName = rootName
      val chunkKey = "sstable.%s.chunk.length".format(rootName)
      val compressedFileSizeKey = "sstable.%s.file.length".format(rootName)
      val chunkPositionsKey = "sstable.%s.chunk.positions".format(rootName)
      val conf = context.getConfiguration()
      chunkPositions = conf.get(chunkPositionsKey).split(",").map(_.toLong)
      val chunkLength = conf.getLong(chunkKey, 0L)
      val maxSplitSize = conf.getLong("sstable.max.split.size", 0L)
      val chunksPerSplit = maxSplitSize / chunkLength
      
      splitLength = (chunksPerSplit * chunkLength)
      splits = chunkPositions.sliding(chunksPerSplit.toInt, chunksPerSplit.toInt).toArray
    } catch {
      case ex: Throwable => ex.printStackTrace(); throw ex
    }
    
    protected override def map(key: Text, value: LongWritable, context: Context): Unit = {
      val split = context.getInputSplit().asInstanceOf[FileSplit]
      val indexName = split.getPath().getName()
      val rootName = indexName.stripSuffix("-Index.db")
      if (rootName != splitName) throw new IOException("Mapper was not reinstntiated with new split?")
      val chunkKey = "sstable.%s.chunk.length".format(rootName)
      val compressedFileSizeKey = "sstable.%s.file.length".format(rootName)
      val conf = context.getConfiguration()
      val chunkLength = context.getConfiguration().getLong(chunkKey, 0L)
      val compressedFileSize = conf.getLong(compressedFileSizeKey, 0L)
      if (chunkLength == 0L) throw new IOException("Could not find compressionInfo for %s.".format(rootName))
      
      val splitIndex = value.get() / splitLength
      val splitChunks = splits(splitIndex.toInt)
      
      val rawSplitPosition = splitChunks.head.toInt
      val rawNextSplitPosition = splits.lift(splitIndex.toInt+1).map(_.head).getOrElse(compressedFileSize)
      val offset = (value.get() - (splitIndex * splitLength))
      val totalUncompressedSize = splitChunks.length * chunkLength
      try {
        context.write(new ChunkIndex(rootName, splitIndex, rawSplitPosition, rawNextSplitPosition-rawSplitPosition, totalUncompressedSize, splitChunks), new LongWritable(offset))
      } catch {
        case ex: Exception => println("huh????: %s".format(ex)); throw ex
      }
    }
  }

  class DataSplitReducer extends Reducer[ChunkIndex, LongWritable, Text, Text] with Reducers[ChunkIndex, LongWritable, Text, Text]{
    private var output: MultipleOutputs[Text, Text] = null
    override def setup(context: Context) {
      output = new MultipleOutputs(context)
    }
    
    protected override def reduce(key: ChunkIndex, vals: JavaIterable[LongWritable], context: Context) {
      val it = vals.iterator()
      val stuff = new mutable.ArrayBuffer[Long]()
      while (it.hasNext()) {
        val nextValue = it.next()
        stuff.append(nextValue.get())
      }
      val sorted = stuff.sorted
      context.write(new Text("%s\t%s".format(key.file.toString, key.index.toString)), new Text("%d\t%d\t%d\t%d\t%d\t%s".format(key.rootPosition.get, key.chunkLength.get, sorted.head, sorted.last-sorted.head, key.totalUncompressedSize.get(), key.compressionPositions.mkString(","))))
    }
  }
  
  def computeCompressionInfo(file: Path, conf: Configuration) {
    val fname = file.getName().stripSuffix("-CompressionInfo.db")
    val fs = file.getFileSystem(conf)
    val is = fs.open(file)
    val reader = new CompressionInfoReader(is)
    Log.info("Setting compression info for %s: %s".format(fname, reader.chunkLength))
    conf.setLong("sstable.%s.chunk.length".format(fname), reader.chunkLength)
    conf.setLong("sstable.%s.file.length".format(fname), reader.dataLength)
    
    val chunkArray = reader.toArray
    conf.set("sstable.%s.chunk.positions".format(fname), chunkArray.mkString(","))
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
    conf.setLong("sstable.max.split.size", 1*1024*1024)
    val args = new GenericOptionsParser(conf, rawArgs).getRemainingArgs()
    val inputPath = new Path(args(0))
    crawlForCompressionInfo(inputPath, conf)

    val job = new Job(conf)
    job.setJobName("Generate SSTable Data Splits")
    job.setJarByClass(this.getClass())
    job.setMapOutputKeyClass(classOf[ChunkIndex])
    job.setMapOutputValueClass(classOf[LongWritable])
    job.setMapperClass(classOf[IndexMapper])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])
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