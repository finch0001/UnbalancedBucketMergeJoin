package com.cloudera.sa.UbMrgJoin;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.SequenceFile.Reader.Option;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import org.apache.hadoop.util.StringUtils;

public class UbMrgMergeRecordReader extends RecordReader<LongWritable, Text> {

	private SequenceFileRecordReader<LongWritable, Text> sequenceFileRecordReader;

	private LongWritable mainKey = new LongWritable();
	private Text value = new Text();
	private LongWritable joiningKey = new LongWritable(-1);
	private long lastSuccessJoinKey = -1;
	private Text joinValue = new Text();
	ArrayList<String> joinValues = new ArrayList<String>();
	ArrayList<String> currentJoinValues;
	int currentJoinValuesIndex = 0;
	private Iterator<Entry<Long, Path>> sortedJoinedFiles;
	SequenceFile.Reader joinReader = null;
	String lastKey = null;
	boolean isFirst = true;
	Entry<Long, Path> currentJoinFile;
	Entry<Long, Path> nextJoinFile;
	TaskAttemptContext context;
	FileSystem fs;

	public UbMrgMergeRecordReader(TaskAttemptContext context, String joiningStatsOutputPath) throws IOException {
		
		this.context = context;
		fs = FileSystem.get(context.getConfiguration());
		sequenceFileRecordReader = new SequenceFileRecordReader<LongWritable, Text>();
		
		populateSortedFilesByKeyMap(context, joiningStatsOutputPath);
		
		
	}

	private void populateSortedFilesByKeyMap(TaskAttemptContext context, String joiningStatsOutputPath) throws IOException {
		
		
		BufferedReader reader = new BufferedReader( new InputStreamReader(fs.open(new Path(joiningStatsOutputPath))));
		
		String line;
		TreeMap<Long, Path> sortedFilesByKey = new TreeMap<Long, Path>();
		while ((line = reader.readLine()) != null) {
			String[] parts = StringUtils.split(line, ',');
			
			sortedFilesByKey.put(Long.parseLong(parts[2]), new Path(parts[0]));
		}
		
		sortedJoinedFiles = sortedFilesByKey.entrySet().iterator();
		
		reader.close();
		
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {

		sequenceFileRecordReader.initialize(split, context);
	}
	
	public ArrayList<String> getNextJoinKeyList(long coreKey) throws IOException {
	  
	  if (lastSuccessJoinKey == coreKey) {
	    //System.out.println("getNextJoin.1" + lastSuccessJoinKey);
			return joinValues;
		} else {
		  joinValues.clear();
		}
		
		boolean changedCurrentJoinFile = false;
		
		//first time
		if (currentJoinFile == null) {
		  //System.out.println("getNextJoin.2 null");
			changedCurrentJoinFile = true;
			currentJoinFile = sortedJoinedFiles.next();
			nextJoinFile = sortedJoinedFiles.next();
		}
		
		//Make sure the next file has a great id
		while (nextJoinFile != null && coreKey >= nextJoinFile.getKey()) {
		  //System.out.println("getNextJoin.3 " + nextJoinFile.getKey());
			if (!sortedJoinedFiles.hasNext()) {
			  //System.out.println("getNextJoin.4 " + null);
				changedCurrentJoinFile = true;
				currentJoinFile = nextJoinFile;
				nextJoinFile = null;
				break;
			}
			//System.out.println("getNextJoin.5 " + null);
			changedCurrentJoinFile = true;
			currentJoinFile = nextJoinFile;
			nextJoinFile = sortedJoinedFiles.next();
		}
		
		//If the currentJoinFile has changed
		if (changedCurrentJoinFile) {
		  if (joinReader != null) {
		    joinReader.close();
		  }
		  //System.out.println("getNextJoin.6 " + currentJoinFile.getValue());
			Option fileOption = SequenceFile.Reader.file(currentJoinFile.getValue());
			joinReader = new SequenceFile.Reader(context.getConfiguration(), fileOption);
		}
		
		while (joiningKey.get() < coreKey ) {
		  
		  //System.out.println("getNextJoin.7 " + joiningKey.get());
		  
			if (!joinReader.next(joiningKey, joinValue)) {
				return null;
			}
		}
		
		if (joiningKey.get() == coreKey) {
		  //System.out.println("getNextJoin.8 " + joiningKey.get());
			lastSuccessJoinKey = coreKey;
			joinValues.add(joinValue.toString());
			while (joiningKey.get() == coreKey) {
				if (!joinReader.next(joiningKey, joinValue)) {
				  //System.out.println("getNextJoin.9 " + null);
					break;
				} else {
				  //System.out.println("getNextJoin.10 " + joinValue.toString());
					joinValues.add(joinValue.toString());		
				}
			}
			return joinValues;
		}
		//System.out.println("getNextJoin.X " + null);
		return null;
		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		
		if (currentJoinValues != null && currentJoinValues.size() -1 > currentJoinValuesIndex) {
		  //System.out.println("coreByPass: " + currentJoinValuesIndex);
			value.set(sequenceFileRecordReader.getCurrentValue().toString() + "|" + currentJoinValues.get(currentJoinValuesIndex++));
			return true;
		}
		
		while (sequenceFileRecordReader.nextKeyValue()) {
			
		  long keyLong = sequenceFileRecordReader.getCurrentKey().get();
		  mainKey.set(keyLong);
		
  		//System.out.println("coreRead: " + keyLong);
  		if ((currentJoinValues = getNextJoinKeyList(keyLong)) != null ) {
  			currentJoinValuesIndex = 0;
  			value.set(sequenceFileRecordReader.getCurrentValue().toString() + "|" + currentJoinValues.get(currentJoinValuesIndex++));
  			return true;
  		}
		}
		//System.out.println("no match: ");
		return false;
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return mainKey;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return sequenceFileRecordReader.getProgress();
	}

	@Override
	public void close() throws IOException {
		sequenceFileRecordReader.close();
		if (joinReader != null) {
      joinReader.close();
    }
	}
	

}
