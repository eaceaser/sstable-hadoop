package com.tehasdf.pig.load

import com.tehasdf.mapreduce.util.BinaryConverter
import org.apache.hadoop.io.Text

object StringConverter extends BinaryConverter[Text] {
  def convert(bytes: Array[Byte]) = new Text(new String(bytes))
}

class SSTableStringDataLoader extends SSTableDataLoader[Text, Text, Text]()(StringConverter, StringConverter, StringConverter)