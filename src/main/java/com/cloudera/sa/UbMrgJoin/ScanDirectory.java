package com.cloudera.sa.UbMrgJoin;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Reader.Option;

public class ScanDirectory {
  
  public static BufferedWriter writer;
  public static ExecutorService executorService = Executors.newFixedThreadPool(20);
  
	public static void main(String[] args) throws IOException, InterruptedException {
		if (args.length == 0) {
			System.out.println("ScanDirectory <inputFolder> <outputFile> <numberOfSamplesPerFile>");
		}
		String inputFolder = args[0];
		String outputFile = args[1];
		int numberOfSamplePerFile = Integer.parseInt(args[2]);
		
		runScan(inputFolder, outputFile, numberOfSamplePerFile);
	}
	
	public static void runScan(String inputFolder, String outputFile, int numberOfSamplePerFile) throws IOException, InterruptedException {
		
		Configuration conf = new Configuration();
		
		FileSystem fs = FileSystem.get(conf);
		
		writer = new BufferedWriter( new OutputStreamWriter(fs.create(new Path(outputFile), true)));
		
		FileStatus[] fileStatuses = fs.listStatus(new Path(inputFolder));
		
		LongWritable key = new LongWritable();
		
		for (FileStatus fileStatus: fileStatuses) {
		  ReadSeqFileHead thread = new ReadSeqFileHead(numberOfSamplePerFile,
      conf, key, fileStatus);
		  
		  executorService.execute(thread);
		}
		executorService.shutdown();
		executorService.awaitTermination(10000, TimeUnit.SECONDS);
		writer.close();
	}

  private static void readSeqMeta(int numberOfSamplePerFile,
      Configuration conf, LongWritable key, FileStatus fileStatus)
      throws IOException {
    StringBuilder strBuilder = new StringBuilder();
    
    strBuilder.append(fileStatus.getPath() + "," + fileStatus.getLen() + ",");
    
    Option fileOption = SequenceFile.Reader.file(fileStatus.getPath()); 
    Reader reader = new SequenceFile.Reader(conf, fileOption);
    
    reader.next(key);
    
    strBuilder.append(key.toString() + ",");
    
    if (numberOfSamplePerFile > 1) {
    	long seekLength = fileStatus.getLen()/numberOfSamplePerFile;
    	for (int i = 1; i < numberOfSamplePerFile; i++) {
    		reader.seek(seekLength * i);
    		reader.next(key);
    		strBuilder.append(key.toString() + ",");
    	}
    }
    
    writeSeqMetaData(strBuilder.toString());
    reader.close();
  }
	
	public synchronized static void writeSeqMetaData(String line) throws IOException {
	  writer.append(line);
	  writer.newLine();
	}
	
	public static class ReadSeqFileHead implements Runnable {

	  int numberOfSamplePerFile;
	  Configuration conf;
	  LongWritable key;
	  FileStatus fileStatus;
	  
	  public ReadSeqFileHead(int numberOfSamplePerFile,
    Configuration conf, LongWritable key, FileStatus fileStatus) {
	    this.numberOfSamplePerFile = numberOfSamplePerFile;
	    this.conf = conf;
	    this.key = key;
	    this.fileStatus = fileStatus;
	  }
	  
    public void run() {
      try {
        ScanDirectory.readSeqMeta( numberOfSamplePerFile,
            conf, key, fileStatus);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
	}
}
