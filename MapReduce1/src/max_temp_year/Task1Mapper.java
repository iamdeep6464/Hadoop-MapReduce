package max_temp_year;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public class Task1Mapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		//10-01-1990,123112,10 ------ > entire line/row is considered as value			
		String[] lineArray = value.toString().split(","); //----- > delimiter is split
		
		//specifying actual key - value pair
		IntWritable year = new IntWritable(Integer.parseInt(lineArray[0].split("-")[2])); // Key
		IntWritable temp = new IntWritable(Integer.parseInt(lineArray[2])); // Value
		
		context.write(year, temp);
	}
}
			