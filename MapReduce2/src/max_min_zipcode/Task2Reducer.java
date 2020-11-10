package max_min_zipcode;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task2Reducer extends Reducer<IntWritable, IntWritable, IntWritable, Text>
{	
	Text minMaxVal;
	
	@Override
	public void setup(Context context) {
		minMaxVal = new Text();
	}
	
	@Override
	// IP key = zip, ip value = list of temp
	// op key = zip, op value = max, min of temp
	public void reduce(IntWritable key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
	{
		int maxVal = Integer.MIN_VALUE;
		int minVal = Integer.MAX_VALUE;
		
		//10,8,15,20
		for (IntWritable value : values) {
			if (value.get() > maxVal) {
				maxVal = value.get();
			}
			if (value.get() < minVal) {
				minVal = value.get();
			}
		}
		
		minMaxVal.set("Max Temp: " + maxVal + ", Min Temp: " + minVal);
		context.write(key, minMaxVal);
	}
}
