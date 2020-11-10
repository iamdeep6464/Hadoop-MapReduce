package max_temp_month;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

public class Task3Mapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
	
	IntWritable month;
	IntWritable temp;

	@Override
	public void setup(Context context) {
		month = new IntWritable();
		temp = new IntWritable();
	}

	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] lineArray = value.toString().split(",");
	
		month.set(Integer.parseInt(lineArray[0].split("-")[1]));
		temp.set(Integer.parseInt(lineArray[2]));
	
		context.write(month, temp);
	}
}
