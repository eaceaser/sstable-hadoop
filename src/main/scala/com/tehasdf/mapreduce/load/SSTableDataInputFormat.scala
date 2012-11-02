package com.tehasdf.mapreduce.load

import com.tehasdf.mapreduce.util.FSSeekableDataInputStream
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

object SSTableDataInputFormat {
  def pathToCompressionInfo(path: String) = path.replaceAll("-Data\\.db$", "-CompressionInfo.db")
  def pathToIndex(path: String) = path.replaceAll("-Data\\.db$", "-Index.db")

  private[SSTableDataInputFormat] val Log = LogFactory.getLog(classOf[SSTableDataInputFormat])
}

class SSTableDataInputFormat extends PigFileInputFormat[Text, MapWritable] {
  import SSTableDataInputFormat._

  def createRecordReader(split: InputSplit, context: TaskAttemptContext) = new SSTableDataRecordReader

  override def isSplitable(context: JobContext, filename: Path) = true

  override def getSplits(job: JobContext) = {
    val files = listStatus(job)
    val rv = new ArrayList[InputSplit]

    files.foreach { fileStatus =>
      val file = fileStatus.getPath()
      val indexFile = SSTableDataInputFormat.pathToIndex(file.getName())
      val compressionInfoFile = SSTableDataInputFormat.pathToCompressionInfo(file.getName())
      val fs = file.getFileSystem(job.getConfiguration())
      val blockLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen())

      val indexPath = new Path(file.getParent(), indexFile)
      val compressionInfoPath = new Path(file.getParent(), compressionInfoFile)

      if (!fs.exists(indexPath)) {
        throw new IOException("index file %s does not exist".format(indexPath.toString()))
      }

      if (!fs.exists(compressionInfoPath)) {
        throw new IOException("compression info file %s does not exist".format(compressionInfoPath.toString()))
      }

      val compressionInfoIs = fs.open(compressionInfoPath)
      val compressionInfo = new CompressionInfoReader(compressionInfoIs)
      val compressedChunks = compressionInfo.toList

      val maxSplitSize = FileInputFormat.getMaxSplitSize(job)

      val indexIs = fs.open(indexPath)
      val seekableIndex = new FSSeekableDataInputStream(indexIs, fs.getFileStatus(indexPath))
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
            val split = new CompressedSSTableSplit(
                path=file,
                start=currentChunkOffsets.head,
                length=currentSplitSize,
                firstKeyPosition=chunkOffsetPos % compressionInfo.chunkLength,
                compressionOffsets=currentChunkOffsets.map(_-currentChunkOffsets.head).toSeq,
                hosts=blockLocations(0).getHosts())
            rv.add(split)
            val lastChunk = currentChunkOffsets.last
            chunkOffsetPos = previousPos
            currentChunkOffsets.clear()
            currentChunkOffsets.append(lastChunk)
            currentChunkOffsets.append(closestChunkOffset)
            Log.debug("Added split: %s".format(split))
          } else {
            currentChunkOffsets.append(closestChunkOffset)
          }
        }
        previousPos = key.pos
      }

      val finalSplitSize = fileStatus.getLen() - currentChunkOffsets.head
      val split = new CompressedSSTableSplit(
          path = file,
          start = currentChunkOffsets.head,
          length = finalSplitSize,
          firstKeyPosition = chunkOffsetPos % compressionInfo.chunkLength,
          compressionOffsets = currentChunkOffsets.map(_-currentChunkOffsets.head).toSeq,
          hosts=blockLocations(0).getHosts())
      rv.add(split)
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
