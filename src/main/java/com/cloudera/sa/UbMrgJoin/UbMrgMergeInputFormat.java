package com.cloudera.sa.UbMrgJoin;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class UbMrgMergeInputFormat extends FileInputFormat<LongWritable, Text>{

	public static final String JOINING_PATH = "joining.path";
	public static final String JOINING_STATS_OUTPUT_PATH = "joining.stats.output.path";
	
	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new UbMrgMergeRecordReader(context, context.getConfiguration().get(JOINING_STATS_OUTPUT_PATH));
	}

	public static void setJoiningPath(Job job, String joiningPath, String joiningStatsOutputPath) throws IOException {
		job.getConfiguration().set(JOINING_PATH, joiningPath);
		job.getConfiguration().set(JOINING_STATS_OUTPUT_PATH, joiningStatsOutputPath);
		
		ScanDirectory.runScan(joiningPath, joiningStatsOutputPath, 1);
	}
}
