package com.kbj.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.kbj.common.DateKey;

public class DelayCountReducerWithDateKey extends Reducer<DateKey, IntWritable, DateKey, IntWritable> {

	private MultipleOutputs<DateKey, IntWritable> mos;
	private DateKey outputKey = new DateKey();
	private IntWritable result = new IntWritable();

	@Override
	protected void setup(Reducer<DateKey, IntWritable, DateKey, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		mos = new MultipleOutputs<DateKey, IntWritable>(context);
	}

	@Override
	protected void reduce(DateKey key, Iterable<IntWritable> values,
			Reducer<DateKey, IntWritable, DateKey, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] columns = key.getYear().split(","); // 데이터는 연도와 월별로 sorting 되어 들어옴

		int sum = 0;
		Integer bMonth = key.getMonth(); // (1) 일단 현재 key의 month 값을 bMonth에 할당

		if (columns[0].equals("D")) {
			for (IntWritable value : values) {
				if (bMonth != key.getMonth()) { // (3) 그러다가 key data의 month가 다르게 들어오면 (ex. 12년도 3월 data가 들어오다가 8월 data가
												// 들어오면)
					result.set(sum); // 일단 지금까지 더했던 12년 3월 data를 result에 설정
					outputKey.setYear(key.getYear().substring(2)); // 년도 값을 가져와서 set (앞에 쓰여있는 "D,"를 없애느라 substring 사용)
					outputKey.setMonth(bMonth); // month 값이 바뀌었으니까 그 전 month 값을 설정해둔 bMonth에서 month를 가져와 set
					mos.write("departure", outputKey, result); // mos에다가 세팅
					sum = 0; // sum 초기화 -> 이후 반복
				}
				sum += value.get(); // (2) bMonth 값과 들어오는 key data의 month값이 동일하면 일단 계속 더해줌
				bMonth = key.getMonth();
			}
			if (key.getMonth() == bMonth) {
				outputKey.setYear(key.getYear().substring(2));
				outputKey.setMonth(bMonth);
				mos.write("departure", outputKey, result);
			}
		} else {
			for (IntWritable value : values) {
				if (bMonth != key.getMonth()) { // (3) 그러다가 key data의 month가 다르게 들어오면 (ex. 12년도 3월 data가 들어오다가 8월 data가
												// 들어오면)
					result.set(sum); // 일단 지금까지 더했던 12년 3월 data를 result에 설정
					outputKey.setYear(key.getYear().substring(2)); // 년도 값을 가져와서 set (앞에 쓰여있는 "A,"를 없애느라 substring 사용)
					outputKey.setMonth(bMonth); // month 값이 바뀌었으니까 그 전 month 값을 설정해둔 bMonth에서 month를 가져와 set
					mos.write("arrival", outputKey, result); // mos에다가 세팅
					sum = 0; // sum 초기화 -> 이후 반복
				}
				sum += value.get(); // (2) bMonth 값과 들어오는 key data의 month값이 동일하면 일단 계속 더해줌
				bMonth = key.getMonth();
			}
			if (key.getMonth() == bMonth) {
				outputKey.setYear(key.getYear().substring(2));
				outputKey.setMonth(bMonth);
				mos.write("arrival", outputKey, result);
			}
		}
	}

	@Override
	protected void cleanup(Reducer<DateKey, IntWritable, DateKey, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		mos.close();
	}

}
