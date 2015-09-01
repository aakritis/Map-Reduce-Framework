package edu.upenn.cis455.mapreduce.job;

import java.util.StringTokenizer;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;

public class WordCount implements Job {

	public void map(String key, String value, Context context)
	{
		// Your map function for WordCount goes here
		StringTokenizer tokens = new StringTokenizer(value);
		while(tokens.hasMoreTokens()) {
			String k = tokens.nextToken();
			// by default assign 1 to each token
			String v = "1";
			context.write(k, v);
		}
	}

	public void reduce(String key, String[] values, Context context)
	{
		// Your reduce function for WordCount goes here
		try{
			if(key.equals("")||values.equals(null))
				return;
			int countVal = 0;
			for(String v : values)
				countVal += Integer.parseInt(v);
			
			context.write(key,Integer.toString(countVal));
		}
		catch(NumberFormatException e) {
			System.out.println("[ERROR] While writing context file +" + e);
		}



	}

}
