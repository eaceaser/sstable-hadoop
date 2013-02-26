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
  
/*  case class ChunkIndex(var file: Text, var index: LongWritable, var rootPosition: LongWritable, var chunkLength: LongWritable, var totalUncompressedSize: LongWritable, var compressionPositions: Array[Long]) extends WritableComparable[ChunkIndex] {
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
  } */
  
  case class ChunkIndex(var file: Text, var index: LongWritable) extends WritableComparable[ChunkIndex] {
    def this() = this(null, null)
    def this(f: String, i: Long) = this(new Text(f), new LongWritable(i))
    
    def compareTo(other: ChunkIndex) = {
      val cmp = file.compareTo(other.file)
      if (cmp == 0)
        index.compareTo(other.index)
      else
        cmp
    }
    
    def readFields(in: DataInput) {
      val t = new Text
      t.readFields(in)
      val i = new LongWritable()
      i.readFields(in)
      file = t
      index = i 
    }
    
    def write(out: DataOutput) {
      file.write(out)
      index.write(out)
    }
  }
  
  class IndexMapper extends Mapper[Text, LongWritable, ChunkIndex, LongWritable] with Mappers[Text, LongWritable, ChunkIndex, LongWritable] {
    private val Log = LogFactory.getLog(this.getClass())
    private val codec = new Base64(false)
    
    private var splitName: String = null
    private var splitLength: Long = 0L
    private var chunkLength: Long = 0L
    
    private val chunkToSplitIndex: mutable.Map[Int, mutable.Seq[Int]] = mutable.Map.empty
    private val splitStarts: mutable.Map[Int, Long] = mutable.Map.empty
    
    protected override def setup(context: Context): Unit = try {
      val conf = context.getConfiguration()
      val split = context.getInputSplit().asInstanceOf[FileSplit]
      val indexName = split.getPath().getName()
      val rootName = indexName.stripSuffix("-Index.db")
      splitName = rootName
      val cInfoPathKey = rootName+"-CompressionInfo.db"
      val cInfoPath = new Path(split.getPath().getParent(), cInfoPathKey)
      val fs = cInfoPath.getFileSystem(conf)
      val is = fs.open(cInfoPath)
      val reader = new CompressionInfoReader(is)
      chunkLength = reader.chunkLength
      val maxSplitSize = conf.getLong("sstable.max.split.size", 0L)
      val chunksPerSplit = (maxSplitSize / chunkLength).toInt
      splitLength = (chunksPerSplit * chunkLength)
      
      val chunks = reader.toList.zipWithIndex
      val splits = chunks.sliding(chunksPerSplit+1, chunksPerSplit).toList
      splits.zipWithIndex.map { case (split, splitIdx) =>
        split.map { case (chunk, chunkIdx) =>
          chunkToSplitIndex.getOrElseUpdate(chunkIdx, mutable.ArraySeq()).add(splitIdx)
        }
        splitStarts += (splitIdx -> split.head._1)
      }
      
    } catch {
      case ex: Throwable => ex.printStackTrace(); throw ex
    }
    
    protected override def map(key: Text, value: LongWritable, context: Context): Unit = {
      val split = context.getInputSplit().asInstanceOf[FileSplit]
      val indexName = split.getPath().getName()
      val rootName = indexName.stripSuffix("-Index.db")
      if (rootName != splitName) throw new IOException("Mapper was not reinstntiated with new split?")
      
      if (chunkLength == 0L) throw new IOException("Could not find compressionInfo for %s.".format(rootName))
      val chunkIndex = value.get() / chunkLength
      chunkToSplitIndex(chunkIndex.toInt).map { splitIdx =>
        val offset = (value.get() - splitStarts(splitIdx))
        context.write(new ChunkIndex(rootName, splitIdx), new LongWritable(offset))
      }
    }
  }

  class DataSplitReducer extends Reducer[ChunkIndex, LongWritable, Text, Text] with Reducers[ChunkIndex, LongWritable, Text, Text]{
    private val Log = LogFactory.getLog(this.getClass())
    
    protected override def reduce(key: ChunkIndex, vals: JavaIterable[LongWritable], context: Context) {
      val conf = context.getConfiguration()
      Log.info("Handling: %s".format(key.toString))
      
      val compressionInfoPathKey = "sstable.%s.chunk.file".format(key.file)
      val compressionInfoPath = new Path(conf.get(compressionInfoPathKey))
      Log.info("Read compression info path: %s".format(compressionInfoPath.toString))
      val compressedDataPathKey = "sstable.%s.data.file".format(key.file)
      val compressedDataPath = conf.get(compressedDataPathKey)
      val dataPath = new Path(compressedDataPath)
      Log.info("Read data path: %s".format(dataPath.toString))
      
      val fs = compressionInfoPath.getFileSystem(conf)
      val is = fs.open(compressionInfoPath)
      val reader = new CompressionInfoReader(is)
      val dataStatus = fs.getFileStatus(dataPath)
      
      val compressedFileSize = dataStatus.getLen()
      Log.info("Read compressed file size of: %s".format(compressedFileSize))
      val chunkPositions = reader.toArray
      Log.info("Read %d chunk positions from compression info".format(chunkPositions.length))
      val maxSplitSize = conf.getLong("sstable.max.split.size", 0L)
      
      val chunksPerSplit = maxSplitSize / reader.chunkLength
      Log.info("Calculated %s chunks per split from a max split size of %s and a chunk length of %s".format(chunksPerSplit, maxSplitSize, reader.chunkLength))
      
      val splits = chunkPositions.sliding(chunksPerSplit.toInt+1, chunksPerSplit.toInt).toArray
      
      Log.info("Calculated %s splits total".format(splits.length))
      val splitChunks = splits.lift(key.index.get.toInt).getOrElse(Array(chunkPositions.last))
      val rawSplitPosition = splitChunks.head
      
      val rawNextSplitPosition = splits.lift(key.index.get.toInt+1).map(_.head).getOrElse(compressedFileSize)
      val length = rawNextSplitPosition - rawSplitPosition
      
      val uncompressedSize = if (key.index.get+1 == splits.length)
        reader.dataLength - (key.index.get*reader.chunkLength)
      else
        splitChunks.length * reader.chunkLength
      
      val it = vals.iterator()
      val stuff = new mutable.ArrayBuffer[Long]()
      while (it.hasNext()) {
        val nextValue = it.next()
        stuff.append(nextValue.get())
      }
      val sorted = stuff.sorted
      context.write(new Text("%s\t%s".format(key.file.toString, key.index.toString)), new Text("%d\t%d\t%d\t%d\t%d\t%s".format(rawSplitPosition, length, sorted.head, sorted.last-sorted.head, uncompressedSize, splitChunks.mkString(","))))
    }
  }
  
  def computeCompressionInfo(file: Path, conf: Configuration) {
    val fname = file.getName().stripSuffix("-CompressionInfo.db")
    val fs = file.getFileSystem(conf)
    
    Log.debug("Recording chunk file for %s: %s".format(fname, file.toString))
    conf.set("sstable.%s.chunk.file".format(fname), file.toString)
    val dataPath = new Path(file.getParent, fname+"-Data.db")
    Log.debug("Recording data file for %s: %s".format(fname, dataPath.toString))
    conf.set("sstable.%s.data.file".format(fname), dataPath.toString) 
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
    conf.setLong("sstable.max.split.size", 256*1024*1024)
    val args = new GenericOptionsParser(conf, rawArgs).getRemainingArgs()
    val inputPath = new Path(args(0))
    conf.set("mapreduce.index.splits.dir", args(2))
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
    job.setNumReduceTasks(512)
    job.setOutputFormatClass(classOf[TextOutputFormat[_,_]])
    
    FileInputFormat.setInputPaths(job, inputPath)
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    FileInputFormat.setMaxInputSplitSize(job, 256*1024*1024)
    System.exit(if (job.waitForCompletion(true)) 0 else 1)
  }
}