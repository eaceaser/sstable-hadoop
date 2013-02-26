package com.tehasdf.mapreduce.load

import scala.collection.JavaConversions.{asScalaBuffer, bufferAsJavaList, seqAsJavaList}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import scala.collection.mutable
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigFileInputFormat
import com.tehasdf.mapreduce.util.FSSeekableDataInputStream
import com.tehasdf.sstable.IndexSummaryReader
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.commons.logging.LogFactory
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.IOException

class SSTableIndexInputFormat extends PigFileInputFormat[Text, LongWritable] {
  private val Log = LogFactory.getLog(this.getClass())

  override def listStatus(job: JobContext) = {
    val list = super.listStatus(job)
    list.filter { fs =>
      fs.getPath().getName().endsWith("-Index.db")
    }
  }

  override protected def isSplitable(context: JobContext, filename: Path) = true
  def createRecordReader(split: InputSplit, context: TaskAttemptContext) = new SSTableIndexRecordReader

  override def getSplits(job: JobContext) = {
    val conf = job.getConfiguration()
    val splitsDir = conf.get("mapreduce.index.splits.dir")
    val splitsPath = new Path(splitsDir)
    val fs = splitsPath.getFileSystem(conf)
    val files = MapRedUtil.getAllFileRecursively(List(fs.getFileStatus(splitsPath)), conf)
    files.flatMap { status =>
      val is = fs.open(status.getPath)
      val bufReader = new BufferedReader(new InputStreamReader(is))
      var line = bufReader.readLine()
      val out = new mutable.ListBuffer[FileSplit]()
      while (line != null) {
        line.split('\t').toList match {
          case file :: startStr :: lengthStr :: Nil =>
            val start = startStr.toLong
            val length = lengthStr.toLong
            val path = new Path(file)
            val locs = fs.getFileBlockLocations(fs.getFileStatus(path), start, length)
            out.append(new FileSplit(path, start, length, locs.flatMap(_.getHosts)))
          case _ => throw new IOException("Malformed Line")
        }
        line = bufReader.readLine()
      }
      
      out
    }
  }
}