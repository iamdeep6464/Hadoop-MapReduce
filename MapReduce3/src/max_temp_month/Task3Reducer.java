package max_temp_month;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class Task3Reducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>
{	
	IntWritable maxVal;
	
	@Override
	public void setup(Context context) {
		maxVal = new IntWritable();
	}
	
	@Override
	public void reduce(IntWritable key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
	{
		int maxValue = Integer.MIN_VALUE;
		
		for (IntWritable value : values) {
			if (value.get() > maxValue) {
				maxValue = value.get();
			}
		}
		
		maxVal.set(maxValue);
		context.write(key, maxVal);
	}
}
