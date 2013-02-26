package com.tehasdf.mapreduce.mapred;

import com.tehasdf.mapreduce.data.WritableColumn;
import org.apache.hadoop.io.ArrayWritable;

public class ColumnArrayWritable extends ArrayWritable {
  public ColumnArrayWritable() { super(WritableColumn.class); }
  public ColumnArrayWritable(WritableColumn[] values) { super(WritableColumn.class, values); }
}
