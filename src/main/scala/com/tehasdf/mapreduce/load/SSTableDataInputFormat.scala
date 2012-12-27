package com.tehasdf.mapreduce.load

import com.tehasdf.mapreduce.util.FSSeekableDataInputStream
import scala.collection.mutable
import com.tehasdf.sstable.{CompressionInfoReader, IndexReader}
import com.twitter.mapreduce.load.SSTableDataRecordReader
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{MapWritable, Text}
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigFileInputFormat
import java.io.IOException
import java.util.ArrayList
import scala.collection.JavaConversions.{asScalaBuffer, bufferAsJavaList}
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.ArrayWritable
import java.io.BufferedReader
import java.io.InputStreamReader
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.io.Writable
import java.io.DataInput
import java.io.DataOutput
import org.apache.hadoop.io.LongWritable

object SSTableDataInputFormat {
  def pathToCompressionInfo(path: String) = path.replaceAll("-Data\\.db$", "-CompressionInfo.db")
  def pathToIndex(path: String) = path.replaceAll("-Data\\.db$", "-Index.db")
  def pathToRootName(path: String) = path.stripSuffix("-Data.db")

  private[SSTableDataInputFormat] val Log = LogFactory.getLog(classOf[SSTableDataInputFormat])
}

class SSTableDataInputFormat extends PigFileInputFormat[BytesWritable, ArrayWritable] {
  import SSTableDataInputFormat._
  case class SplitRecord(byteStart: Long, byteLength: Long, innerOffset: Long, innerLength: Long, uncompressedLength: Long, chunks: Seq[Long])

  def createRecordReader(split: InputSplit, context: TaskAttemptContext) = new SSTableDataRecordReader
  override def isSplitable(context: JobContext, filename: Path) = true

  private def readSplitData(root: Path, job: JobContext) = {
    val rv = new mutable.HashMap[String, mutable.Buffer[SplitRecord]]
    val conf = job.getConfiguration()
    val fs = root.getFileSystem(conf)
    val pattern = new Path(root, "part-*")
    fs.globStatus(pattern).foreach { status => 
      val is = fs.open(status.getPath())
      val buf = new BufferedReader(new InputStreamReader(is))
      var line = buf.readLine()
      while (line != null) {
        line.split('\t').toList match {
          case filename :: index:: byteStart :: byteLength :: innerOffset :: innerLength :: uncompressedLength :: compressionChunks :: Nil =>
            val chunks = compressionChunks.split(",").map(_.toLong)
            val rec = SplitRecord(byteStart.toLong, byteLength.toLong, innerOffset.toLong, innerLength.toLong, uncompressedLength.toLong, chunks)
            rv.getOrElseUpdate(filename, new ArrayBuffer[SplitRecord]).+=(rec)
          case _ =>
            throw new IOException("omg: %s".format(line))
        }
        
        line = buf.readLine()
      }
      is.close()
    }
    rv
  }
  
  override def getSplits(job: JobContext) = {
    val conf = job.getConfiguration()
    val splitDir = new Path(conf.get("sstable.split.dir"))
    val splits = readSplitData(splitDir, job)
    val rv = new ArrayList[InputSplit]
    val statuses = listStatus(job).foreach { status =>
      val filename = status.getPath().getName
      val fs = status.getPath().getFileSystem(conf)
      val rootName = pathToRootName(filename)
      val fileSplits = splits(rootName)
      fileSplits.map { split => 
        val locations = fs.getFileBlockLocations(status, split.byteStart, split.byteLength).map(_.toString)
        rv += new SSTableDataSplit(status.getPath(), split.byteStart, split.byteLength, split.innerOffset, split.innerLength, split.uncompressedLength, split.chunks.toArray, locations)
      }
    }
    rv
  }

  override def listStatus(job: JobContext) = {
    val list = super.listStatus(job)
    list.filter { fs =>
      fs.getPath().getName().endsWith("-Data.db")
    }
  }
}