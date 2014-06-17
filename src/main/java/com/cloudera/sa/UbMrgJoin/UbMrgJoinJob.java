package com.cloudera.sa.UbMrgJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class UbMrgJoinJob extends Configured implements Tool 
{
    public static void main( String[] args ) throws Exception
    {
    	int res = ToolRunner.run(new Configuration(), new UbMrgJoinJob(), args);
    }

	public int run(String[] args) throws Exception {
		
    if (args.length == 0) {
      System.out.println("UnkJoinJob <input1Path> <input2Path> <outputPath>");
      return -1;
    }

    Job job = new Job();

    // Get values from args
    String input1Path = args[0];
    String input2Path = args[1];
    String outputPath = args[2];
    
    // Create job
    job.setJarByClass(UbMrgJoinJob.class);
    job.setJobName("UnkJoinJob:");
    // Define input format and path
    job.setInputFormatClass(UbMrgMergeInputFormat.class);
    UbMrgMergeInputFormat.addInputPath(job, new Path(input1Path));
    UbMrgMergeInputFormat.setJoiningPath(job, input2Path, "/tmp/joiningStats.txt");
    
    // Define output format and path
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    SequenceFileOutputFormat.setOutputPath(job, new Path(outputPath));
    SequenceFileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);

    // Define the mapper and reducer
    job.setMapperClass(UbMrgJoinMapper.class);

    // Define the key and value format
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(Text.class);

    job.setNumReduceTasks(0);

    // Exit
    job.waitForCompletion(true);
	  
		return 0;
	}
}
