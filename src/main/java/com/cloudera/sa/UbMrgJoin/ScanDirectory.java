package com.cloudera.sa.UbMrgJoin;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Reader.Option;

public class ScanDirectory {
	public static void main(String[] args) throws IOException {
		if (args.length == 0) {
			System.out.println("ScanDirectory <inputFolder> <outputFile> <numberOfSamplesPerFile>");
		}
		String inputFolder = args[0];
		String outputFile = args[1];
		int numberOfSamplePerFile = Integer.parseInt(args[2]);
		
		runScan(inputFolder, outputFile, numberOfSamplePerFile);
	}
	
	public static void runScan(String inputFolder, String outputFile, int numberOfSamplePerFile) throws IOException {
		
		Configuration conf = new Configuration();
		
		FileSystem fs = FileSystem.get(conf);
		
		BufferedWriter writer = new BufferedWriter( new OutputStreamWriter(fs.create(new Path(outputFile), true)));
		
		FileStatus[] fileStatuses = fs.listStatus(new Path(inputFolder));
		
		LongWritable key = new LongWritable();
		
		for (FileStatus fileStatus: fileStatuses) {
			writer.append(fileStatus.getPath() + "," + fileStatus.getLen() + ",");
			
			Option fileOption = SequenceFile.Reader.file(fileStatus.getPath()); 
			Reader reader = new SequenceFile.Reader(conf, fileOption);
			
			reader.next(key);
			
			writer.append(key.toString() + ",");
			
			if (numberOfSamplePerFile > 1) {
				long seekLength = fileStatus.getLen()/numberOfSamplePerFile;
				for (int i = 1; i < numberOfSamplePerFile; i++) {
					reader.seek(seekLength * i);
					reader.next(key);
					writer.append(key.toString() + ",");
				}
			}
			
			writer.newLine();
			reader.close();
			
		}
		
		writer.close();
		
	}
	
	
}
