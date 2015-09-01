package edu.upenn.cis455.mapreduce.job;

import java.util.HashMap;
import java.util.Map;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;

public class AnotherWordCount implements Job {

	@Override
	public void map(String key, String value, Context context) {
		String[] words = value.split("\\s+");
		Map<String, Integer> records = new HashMap<String, Integer>();
		for (String word : words) {
			if (records.get(word) == null) {
				records.put(word, 1);
			} else {
				records.put(word, records.get(word) + 1);
			}
		}
		
		for (String word : records.keySet()) {
			context.write("###" + word, records.get(word).toString());
		}
	}

	@Override
	public void reduce(String key, String[] values, Context context) {
		int sum = 0;
		for (String value : values) {
			sum += Integer.parseInt(value);
		}
		context.write(key, Integer.toString(sum));
	}

}
