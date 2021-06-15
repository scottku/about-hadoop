package com.kbj.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.kbj.common.AirlinePerformanceParser;

public class ArrivalDelayCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	private final static IntWritable one = new IntWritable(1);
	private Text textkey = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		AirlinePerformanceParser parser = new AirlinePerformanceParser(value);
		textkey.set(parser.getYear() + "," + parser.getMonth()); // "2012,01", "2012,02" 와 같은 방식으로 key 설정
		if(parser.getArriveDelayTime() > 0) {
			context.write(textkey,  one);
		}
		
	}
	
	
}
