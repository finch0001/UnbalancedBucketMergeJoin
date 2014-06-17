package com.cloudera.sa.UbMrgJoin;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UbMrgJoinMapper extends Mapper<LongWritable, Text, NullWritable, Text>{

  Text newValue = new Text();
  
  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    
    newValue.set(key.get() + "|" + value);
    
    context.write(NullWritable.get(), newValue);
  }
}
