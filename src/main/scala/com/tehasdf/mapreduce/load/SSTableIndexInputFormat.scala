package com.tehasdf.mapreduce.load

import scala.collection.JavaConversions.{asScalaBuffer, bufferAsJavaList, seqAsJavaList}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigFileInputFormat
import com.tehasdf.mapreduce.util.FSSeekableDataInputStream
import com.tehasdf.sstable.IndexSummaryReader
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat

class SSTableIndexInputFormat extends PigFileInputFormat[Text, LongWritable] {
  override def listStatus(job: JobContext) = {
    val list = super.listStatus(job)
    list.filter { fs =>
      fs.getPath().getName().endsWith("-Index.db")
    }
  }
  override protected def isSplitable(context: JobContext, filename: Path) = true
  def createRecordReader(split: InputSplit, context: TaskAttemptContext) = new SSTableIndexRecordReader
  
  override def getSplits(job: JobContext) = {
    val maxSplitSize = FileInputFormat.getMaxSplitSize(job)
    val minSplitSize = FileInputFormat.getMinSplitSize(job)
    
    val conf = job.getConfiguration()
    val splits = listStatus(job).flatMap { file =>
      val fn = file.getPath().getName().replace("-Index", "-Summary")
      val indexPath = file.getPath()
      val summaryPath = new Path(file.getPath().getParent(), fn)
      val indexFs = indexPath.getFileSystem(conf)
      val summaryFs = summaryPath.getFileSystem(conf)
      val indexStatus = indexFs.getFileStatus(indexPath)
      val summaryStatus = summaryFs.getFileStatus(summaryPath)
      
      val is = summaryFs.open(summaryPath)
      val dis = new FSSeekableDataInputStream(is, summaryStatus)
      val reader = new IndexSummaryReader(dis)
      
      var prevPos = 0L
      val rv = reader.flatMap { position =>
        val len = position.location - prevPos
        if (len > maxSplitSize) {
          val locs = indexFs.getFileBlockLocations(indexStatus, prevPos, len)
          val split = new FileSplit(indexPath, prevPos, len, locs.map(_.toString))
          prevPos = position.location
          Some(split)
        } else { 
          None
        }
      }
      
      rv
    }
    splits
  }
}