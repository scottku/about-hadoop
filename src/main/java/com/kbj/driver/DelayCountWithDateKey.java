package com.kbj.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.kbj.common.DateKey;
import com.kbj.common.DateKeyComparator;
import com.kbj.common.GroupKeyComparator;
import com.kbj.common.GroupKeyPartitioner;
import com.kbj.mapper.DelayCountMapperWithDateKey;
import com.kbj.reducer.DelayCountReducerWithDateKey;

public class DelayCountWithDateKey extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int res = ToolRunner.run(new Configuration(), new DelayCountWithDateKey(), args);
		System.out.println("MR-Job Result:" + res);
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();
		
		if (otherArgs.length != 2) {
			System.out.println("Usage: DelayCountWithDateKey <in> <out>");
			System.exit(2);
		}

		Job job = Job.getInstance(getConf(), "DelayCountWithDateKey");

		job.setJarByClass(DelayCountWithDateKey.class);
		job.setMapperClass(DelayCountMapperWithDateKey.class);
		job.setReducerClass(DelayCountReducerWithDateKey.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(DateKey.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		MultipleOutputs.addNamedOutput(job, "departure", TextOutputFormat.class, Text.class, IntWritable.class);
		MultipleOutputs.addNamedOutput(job, "arrival", TextOutputFormat.class, Text.class, IntWritable.class);
		
		
		// 추가
		job.setPartitionerClass(GroupKeyPartitioner.class);
		job.setGroupingComparatorClass(GroupKeyComparator.class);
		job.setSortComparatorClass(DateKeyComparator.class);
		job.setMapOutputKeyClass(DateKey.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.waitForCompletion(true);
		return 0;
	}
	
	

}
