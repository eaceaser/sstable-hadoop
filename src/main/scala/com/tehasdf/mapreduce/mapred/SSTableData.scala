package com.tehasdf.mapreduce.mapred

import com.tehasdf.mapreduce.data.WritableColumn
import java.lang.{Iterable => JavaIterable}
import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.{Mapper, Reducer}
import org.msgpack.MessagePack
import org.msgpack.annotation.Message
import java.util.Arrays

trait Mappers[KEYIN, VALUEIN, KEYOUT, VALUEOUT] extends Mapper[KEYIN, VALUEIN, KEYOUT, VALUEOUT] {
  type Context = Mapper[KEYIN, VALUEIN, KEYOUT, VALUEOUT]#Context
}

trait Reducers[KEYIN, VALUEIN, KEYOUT, VALUEOUT] extends Reducer[KEYIN, VALUEIN, KEYOUT, VALUEOUT] {
  type Context = Reducer[KEYIN, VALUEIN, KEYOUT, VALUEOUT]#Context
}


class SSTableDataMapper extends Mapper[BytesWritable, ArrayWritable, Text, WritableColumn] with Mappers[BytesWritable, ArrayWritable, Text, WritableColumn] {
  override def map(key: BytesWritable, value: ArrayWritable, context: Context) {
    try {
      val str = new String(key.getBytes(), "UTF-8")
      val keytxt = new Text(str)
      value.get.toList.foreach { col =>
        context.write(keytxt, col.asInstanceOf[WritableColumn])
      }
    } catch {
      case ex: Throwable => println(ex); throw ex
    }
  }
}

class GroupedSSTableDataMapper extends Mapper[BytesWritable, ColumnArrayWritable, BytesWritable, ColumnArrayWritable] with Mappers[BytesWritable, ColumnArrayWritable, BytesWritable, ColumnArrayWritable] {
  override def map(key: BytesWritable, value: ColumnArrayWritable, context: Context) {
    try {
      context.write(key, value)
    } catch {
      case ex: Throwable => println(ex); throw ex
    }
  }
}

class SSTableDataReducer extends Reducer[Text, WritableColumn, Text, Text] with Reducers[Text, WritableColumn, Text, Text] {
  protected override def reduce(key: Text, vals: JavaIterable[WritableColumn], context: Context) {
    try {
      var latestCol: WritableColumn = null
      val it = vals.iterator()
      while (it.hasNext()) {
        val col = it.next()
        if (latestCol == null || col.timestamp.get > latestCol.timestamp.get) latestCol = col
      }

      if (latestCol != null) {
        context.write(key, new Text(latestCol.toString))
      }
    } catch {
      case ex: Throwable => println(ex); throw ex
    }
  }
}

@Message
class MsgPackCol {
  var key: Array[Byte] = null
  var value: Array[Byte] = null
  var ts: Long = 0L
  var expiresMillis: Long = 0L
  var ttlSecs: Long = 0L
}

@Message
class MsgPackRow {
  var key: Array[Byte] = null
  var cols: Array[MsgPackCol] = null
}

class MsgPackSSTableDataReducer extends Reducer[BytesWritable, ColumnArrayWritable, NullWritable, Text] with Reducers[BytesWritable, ColumnArrayWritable, NullWritable, Text] {
  private val msgPack = new MessagePack
  private val base64 = new Base64(Int.MaxValue, "".getBytes("UTF-8"), false)

  protected override def reduce(key: BytesWritable, vals: JavaIterable[ColumnArrayWritable], context: Context) {
    try {
      val finalState = new java.util.HashMap[String, WritableColumn]

      val it = vals.iterator()
      while (it.hasNext()) {
        val cols = it.next()
        cols.get().toList.map(_.asInstanceOf[WritableColumn]).foreach { col =>
          val colName = Base64.encodeBase64String(Arrays.copyOfRange(col.name.getBytes, 0, col.name.getLength))
          val latestCol = finalState.get(colName)
          if (latestCol == null || col.timestamp.get > latestCol.timestamp.get) finalState.put(colName, col)
        }
      }

      val rv = new MsgPackRow
      rv.key = Arrays.copyOfRange(key.getBytes, 0, key.getLength)
      rv.cols = finalState.values().toArray(new Array[WritableColumn](0)).map { col =>
        val colrv = new MsgPackCol
        colrv.key = Arrays.copyOfRange(col.name.getBytes, 0, col.name.getLength)
        if (col.state == WritableColumn.State.NORMAL || col.state == WritableColumn.State.EXPIRING) {
          colrv.value = Arrays.copyOfRange(col.data.getBytes, 0, col.data.getLength)
          colrv.ts = col.timestamp.get()
        }

        if (col.state == WritableColumn.State.EXPIRING) {
          colrv.ttlSecs = col.ttl.get()
          colrv.expiresMillis = col.expiration.get()
        }

        colrv
      }

      context.write(null, new Text(base64.encodeToString(msgPack.write(rv))))
    } catch {
      case ex: Throwable => println(ex); throw ex
    }
  }
}