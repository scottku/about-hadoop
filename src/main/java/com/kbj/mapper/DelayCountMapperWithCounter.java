package com.kbj.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.kbj.common.AirlinePerformanceParser;
import com.kbj.counter.DelayCounters;

public class DelayCountMapperWithCounter extends Mapper<LongWritable, Text, Text, IntWritable> {
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
		
		
		if(workType.equals("departure")) {
			if(parser.isDepartureDelayAvailable()) {
				if (parser.getDepartureDelayTime() > 0) {
					outputkey.set(parser.getYear() + "," + parser.getMonth());
					context.write(outputkey, outputValue);
				} else if(parser.getDepartureDelayTime() == 0) {
					context.getCounter(DelayCounters.scheduled_departure).increment(1);
				} else if(parser.getDepartureDelayTime() < 0) {
					context.getCounter(DelayCounters.early_departure).increment(1);
				}
			} else {
				context.getCounter(DelayCounters.not_available_departure).increment(1);
			}	
		} else if(workType.equals("arrival")) {
			if (parser.isArriveDelayAvailable()) {
				if(parser.getArriveDelayTime() > 0) {
					outputkey.set(parser.getYear() + "," + parser.getMonth());
					context.write(outputkey, outputValue);
				} else if(parser.getArriveDelayTime() == 0) {
				context.getCounter(DelayCounters.scheduled_arrival).increment(1);
				} else if(parser.getArriveDelayTime() < 0) {
				context.getCounter(DelayCounters.early_arrival).increment(1);
				}
			} else {
				context.getCounter(DelayCounters.not_available_arrival).increment(1);
			}
		}
	}
}
