package com.cloudera.sa.UbMrgJoin;

public class Main {
	public static void main(String[] args) throws Exception {
		
	  if (args.length == 0) {
	    System.out.println("Commands:");
	    System.out.println("ScanDirectory");
	    System.out.println("DataGenerator");
	    System.out.println("UnkJoinJob");
	    return; 
	  }
	  
		String command = args[0];
		
		String[] subCmd = new String[args.length -1];
		
		System.arraycopy(args, 1, subCmd, 0, subCmd.length);
		
		if (command.equals("ScanDirectory")) {
			ScanDirectory.main(subCmd);
		} else if (command.equals("DataGenerator")) {
		  DataGenerator.main(subCmd);
		} else if (command.equals("UnkJoinJob")) {
		  UbMrgJoinJob.main(subCmd);
		} else {
		  System.out.println("not a function: " + command);
		}
		
	}
}
