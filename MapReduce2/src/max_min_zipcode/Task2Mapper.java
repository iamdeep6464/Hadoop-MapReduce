package max_min_zipcode;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public class Task2Mapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
		
	IntWritable zip;
	IntWritable temp;
	
	@Override
	public void setup(Context context) {
		zip = new IntWritable();
		temp = new IntWritable();
	}
	
	@Override
	// offset,  10-01-1990,123112,10
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] lineArray = value.toString().split(",");
		
		zip.set(Integer.parseInt(lineArray[1]));  // 123112 ------- > zipcode
		temp.set(Integer.parseInt(lineArray[2])); // 10 ---------- > temparature
		
		context.write(zip, temp); // Passing to next stage ------------- > shuffle and sort.
	}
}
