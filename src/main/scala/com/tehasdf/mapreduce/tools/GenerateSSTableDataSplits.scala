package com.tehasdf.mapreduce.tools

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.GenericOptionsParser
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.fs.Path
import com.tehasdf.sstable.CompressionInfoReader
import com.tehasdf.mapreduce.util.FSSeekableDataInputStream
import com.tehasdf.sstable.IndexReader
import org.apache.hadoop.io.NullWritable
import scala.collection.JavaConversions._
import com.tehasdf.mapreduce.load.CompressedSSTableSplit
import java.util.ArrayList
import org.apache.hadoop.io.GenericWritable
import org.apache.hadoop.io.Writable
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigFileInputFormat
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.JobContext
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.InputSplit
import java.io.DataInput
import java.io.DataOutput
import com.tehasdf.mapreduce.load.SSTableDataInputFormat
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import java.io.IOException
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.io.BytesWritable
import org.apache.commons.codec.binary.Base64

object GenerateSSTableDataSplits {
  trait Mappers[KEYIN, VALUEIN, KEYOUT, VALUEOUT] extends Mapper[KEYIN, VALUEIN, KEYOUT, VALUEOUT] {
    type Context = Mapper[KEYIN, VALUEIN, KEYOUT, VALUEOUT]#Context
  }

  case class FilenameSplit(var path: Path, var indexLength: Long, var indexLocations: Array[String]) extends InputSplit with Writable {
    def this() = this(null, 0L, null)

    def readFields(in: DataInput) {
      path = new Path(Text.readString(in))
      indexLength = in.readLong()
      val numLocs = in.readInt()
      indexLocations = (for (i <- 0 until numLocs) yield Text.readString(in)).toArray
    }

    def write(out: DataOutput) {
      Text.writeString(out, path.toString())
      out.writeLong(indexLength)
      out.writeInt(indexLocations.length)
      indexLocations.foreach { t => Text.writeString(out, t.toString) }
    }

    def getLocations() = indexLocations
    def getLength() = indexLength
  }

  class SimpleInputFormat extends PigFileInputFormat[Text, NullWritable] {
    override def listStatus(job: JobContext) = {
      val list = super.listStatus(job)
      list.filter { _.getPath().getName().endsWith("-Data.db") }
    }

    override def isSplitable(job: JobContext, filename: Path) = true
    def createRecordReader(split: InputSplit, ctx: TaskAttemptContext) = {
      new RecordReader[Text, NullWritable] {
        var rv: String = null
        var latch = true
        def initialize(genericSplit: InputSplit, context: TaskAttemptContext) {
          val file = genericSplit.asInstanceOf[FilenameSplit].path
          rv = Seq(file.toString, SSTableDataInputFormat.pathToIndex(file.getName), SSTableDataInputFormat.pathToCompressionInfo(file.getName())).mkString(",")
        }

        def getProgress() = if (latch) 0.0f else 1.0f

        def close() { }
        def getCurrentValue() = NullWritable.get()
        def getCurrentKey() = new Text(rv)
        def nextKeyValue() = {
          if (latch) {
            latch = false
            true
          } else { false }
        }
      }
    }

    override def getSplits(job: JobContext) = {
      val files = listStatus(job)
      files map { status =>
        val path = status.getPath()
        val parent = path.getParent()

        val indexPath = new Path(parent, SSTableDataInputFormat.pathToIndex(path.getName()))
        val fs = indexPath.getFileSystem(job.getConfiguration())

        val indexStatus = fs.getFileStatus(indexPath)
        val indexLen = indexStatus.getLen()
        val indexPositions = fs.getFileBlockLocations(indexStatus, 0, indexLen).flatMap ( _.getHosts() )

        FilenameSplit(path, indexLen, indexPositions)
      }
    }
  }

  class IndexMapper extends Mapper[Text, NullWritable, NullWritable, Text] with Mappers[Text, NullWritable, NullWritable, Text] {
    private val Log = LogFactory.getLog(this.getClass())
    private val codec = new Base64(false)

    protected override def map(key: Text, value: NullWritable, context: Context): Unit = {
      val List(filename, indexFilename, compressionInfoFilename) = key.toString.split(",").toList
      val file = new Path(filename)
      val parent = file.getParent()
      val conf = context.getConfiguration()
      val indexFile = new Path(parent, indexFilename.toString)
      val compressionInfoFile = new Path(parent, compressionInfoFilename.toString)

      val fs = indexFile.getFileSystem(conf)
      val fileStatus = fs.getFileStatus(file)
      val blockLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen())

      if (!fs.exists(compressionInfoFile)) throw new IOException("Compression info file %s for data file %s not found".format(compressionInfoFile, file))
      if (!fs.exists(indexFile)) throw new IOException("Index file %s for data file %s not found.".format(indexFile, file))

      val compressionInfoIs = fs.open(compressionInfoFile)
      val compressionInfo = new CompressionInfoReader(compressionInfoIs)
      val compressedChunks = compressionInfo.toList

      Log.info("Found %s compressed chunks".format(compressedChunks.size))
      val maxSplitSize = conf.getLong("sstable.max_split_size", 1024*1024)
      Log.info("Using a max split size of %s".format(maxSplitSize))

      val indexIs = fs.open(indexFile)
      val seekableIndex = new FSSeekableDataInputStream(indexIs, fs.getFileStatus(indexFile))
      val index = new IndexReader(seekableIndex)

      var chunkOffsetPos = -1L
      var previousPos = -1L
      val currentChunkOffsets = new ArrayList[Long]
      currentChunkOffsets.append(compressedChunks.head)

      index.foreach { key =>
        val pos = key.pos
        if (previousPos == -1L) previousPos = pos
        if (chunkOffsetPos == -1L) chunkOffsetPos = pos
        val chunkIndex = (pos / compressionInfo.chunkLength).toInt
        val closestChunkOffset = compressedChunks(chunkIndex)

        if (closestChunkOffset != currentChunkOffsets.last) {
          val currentSplitSize = closestChunkOffset - currentChunkOffsets.head

          if (currentSplitSize > maxSplitSize) {
            val computedSplits = new CompressedSSTableSplit(
                path=file,
                start=currentChunkOffsets.head,
                length=currentSplitSize,
                firstKeyPosition=chunkOffsetPos % compressionInfo.chunkLength,
                compressionOffsets=currentChunkOffsets.map(_-currentChunkOffsets.head).toSeq,
                hosts=blockLocations(0).getHosts())

            Log.info("%.3f: Computed split (%s:%s)".format(seekableIndex.position.toFloat/seekableIndex.length, computedSplits.start, computedSplits.start+computedSplits.length))
            context.write(null, new Text(codec.encodeToString(computedSplits.toBytes)))
            val lastChunk = currentChunkOffsets.last
            chunkOffsetPos = previousPos
            currentChunkOffsets.clear()
            currentChunkOffsets.append(lastChunk)
            currentChunkOffsets.append(closestChunkOffset)
          } else {
            currentChunkOffsets.append(closestChunkOffset)
          }
        }
        previousPos = key.pos
      }

      val finalSplitSize = fileStatus.getLen() - currentChunkOffsets.head
      val finalSplit = new CompressedSSTableSplit(
          path = file,
          start = currentChunkOffsets.head,
          length = finalSplitSize,
          firstKeyPosition = chunkOffsetPos % compressionInfo.chunkLength,
          compressionOffsets = currentChunkOffsets.map(_-currentChunkOffsets.head).toSeq,
          hosts=blockLocations(0).getHosts())
      context.write(null, new Text(codec.encodeToString(finalSplit.toBytes)))
      Log.info("Computed split (%s:%s)".format(finalSplit.start, finalSplitSize))
      Log.info("Done.")
    }
  }

  def main(rawArgs: Array[String]) {
    val conf = new Configuration
    conf.setInt("mapreduce.job.max.split.locations", 150)
    conf.setLong("sstable.max.split.size", 24*1024*1024)
    val args = new GenericOptionsParser(conf, rawArgs).getRemainingArgs()

    val job = new Job(conf)
    job.setJobName("Generate SSTable Splits")
    job.setJarByClass(this.getClass())
    job.setMapperClass(classOf[IndexMapper])
    job.setInputFormatClass(classOf[SimpleInputFormat])
    job.setNumReduceTasks(0)
    job.setOutputFormatClass(classOf[TextOutputFormat[_,_]])
    FileInputFormat.setInputPaths(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))

    System.exit(if (job.waitForCompletion(true)) 0 else 1)
  }
}
