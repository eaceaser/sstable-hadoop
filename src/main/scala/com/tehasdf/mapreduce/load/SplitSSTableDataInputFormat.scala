package com.tehasdf.mapreduce.load

import com.twitter.mapreduce.load.SSTableDataRecordReader
import java.io.{BufferedReader, IOException, InputStreamReader}
import java.util.ArrayList
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, TaskAttemptContext}
import scala.collection.JavaConversions._
import scala.collection.mutable

object SplitSSTableDataInputFormat {
  def pathToCompressionInfo(path: String) = path.replaceAll("-Data\\.db$", "-CompressionInfo.db")
  def pathToIndex(path: String) = path.replaceAll("-Data\\.db$", "-Index.db")
  def pathToRootName(path: String) = path.stripSuffix("-Data.db")

  private[SplitSSTableDataInputFormat] val Log = LogFactory.getLog(classOf[SSTableDataInputFormat])
}

class SplitSSTableDataInputFormat extends SSTableDataInputFormat {

  import SplitSSTableDataInputFormat._

  case class SplitRecord(byteStart: Long, byteLength: Long)

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext) = new SSTableDataRecordReader
  override def isSplitable(context: JobContext, filename: Path) = true
  
  private def readSplitData(root: Path, job: JobContext) = {
    val rv = new mutable.HashMap[String, mutable.Buffer[SplitRecord]]
    val conf = job.getConfiguration
    val fs = root.getFileSystem(conf)
    val pattern = new Path(root, "part-*")
    fs.globStatus(pattern).foreach { status => 
      val is = fs.open(status.getPath)
      val buf = new BufferedReader(new InputStreamReader(is))
      var line = buf.readLine()
      while (line != null) {
        line.split('\t').toList match {
          case filename :: index:: byteStart :: byteLength :: innerOffset :: innerLength :: uncompressedLength :: compressionChunks :: Nil =>
            val rec = SplitRecord(byteStart.toLong, byteLength.toLong)
            rv.getOrElseUpdate(filename, new mutable.ArrayBuffer[SplitRecord]).+=(rec)
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
    val conf = job.getConfiguration
    val splitDir = new Path(conf.get("sstable.split.dir"))
    val splits = readSplitData(splitDir, job)
    val rv = new ArrayList[InputSplit]
    val statuses = listStatus(job).foreach { status =>
      val filename = status.getPath.getName
      val fs = status.getPath.getFileSystem(conf)
      val rootName = pathToRootName(filename)
      val fileSplits = splits(rootName)
      fileSplits.map { split => 
        val locations = fs.getFileBlockLocations(status, split.byteStart, split.byteLength).flatMap(_.getHosts)
        rv += new FileSplit(status.getPath, split.byteStart, split.byteLength, locations)
      }
    }
    rv
  }
}