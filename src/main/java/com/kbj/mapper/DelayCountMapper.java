package com.kbj.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.kbj.common.AirlinePerformanceParser;

public class DelayCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private String workType;
	private final static IntWritable outputValue = new IntWritable(1);
	private Text outputkey = new Text();
	
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) // mapper 객체가 실행될 때 딱 한번만 실행되는 메서드
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		workType = context.getConfiguration().get("workType");
	}

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		AirlinePerformanceParser parser = new AirlinePerformanceParser(value);
		outputkey.set(parser.getYear() + "," + parser.getMonth());
		
		if(workType.equals("departure")) {
			if (parser.getDepartureDelayTime() > 0) {
				context.write(outputkey, outputValue);
			}
		} else if(workType.equals("arrival")) {
			if(parser.getArriveDelayTime() > 0) {
				context.write(outputkey, outputValue);
			}
			
		}
	}
	
	
	
	

}
