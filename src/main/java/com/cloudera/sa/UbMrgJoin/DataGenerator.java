package com.cloudera.sa.UbMrgJoin;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class DataGenerator {
	public static void main(String[] args) throws IOException {
		if (args.length == 0) {
			System.out
					.println("DataGenerator <output1> <output2> <totalIDs> <fileCount1> <fileCount2> <oddOfRepeat1> <oddOfRepeat2> <oddOfSkip1> <oddOfSkip2>");
			return;
		}
		
		String output1 = args[0];
		String output2 = args[1];
		long totalIds = Long.parseLong(args[2]);
		int filesCount1 = Integer.parseInt(args[3]);
		int filesCount2 = Integer.parseInt(args[4]);
		int oddOfRepeat1 = Integer.parseInt(args[5]);
		int oddOfRepeat2 = Integer.parseInt(args[6]);
		int oddOfSkip1 = Integer.parseInt(args[7]);
		int oddOfSkip2 = Integer.parseInt(args[8]);
		
		Configuration config = new Configuration();
		
		FileSystem fs = FileSystem.get(config);
		
		WriteDataFiles(output1, totalIds, filesCount1, oddOfRepeat1, oddOfSkip1,
        config, fs);
		
		WriteDataFiles(output2, totalIds, filesCount2, oddOfRepeat2, oddOfSkip2,
        config, fs);
		
	}

  private static void WriteDataFiles(String output1, long totalIds,
      int filesCount1, int oddOfRepeat1, int oddOfSkip1, Configuration config,
      FileSystem fs) throws IOException {
    long linesPerFile1File = totalIds/filesCount1;
		
		LongWritable key = new LongWritable();
		Text val = new Text();
		Random random = new Random();
		
		long counter = 0;
		
		for (int i = 0; i< filesCount1; i++) {
		  System.out.println();
      System.out.println("new File" + output1 + "_" + i);
      
		  SequenceFile.Writer writer1 = new SequenceFile.Writer(fs, config, new Path(output1 + "_" + i), LongWritable.class, Text.class);
		  val.set("File1_" + i);
		  for (int j = 0; j < linesPerFile1File; j++) {
		    
		    if (counter++ % 5000 == 0) {
		      System.out.print(".");
		    }
		    
		    if (random.nextInt(100) > oddOfSkip1) {
		      key.set(j + linesPerFile1File * i);
		      writer1.append(key, val);
		      if (random.nextInt(100) < oddOfRepeat1) {
		        writer1.append(key, val);  
		      }
		    }
		  }
		  writer1.close();
		}
  }
}
