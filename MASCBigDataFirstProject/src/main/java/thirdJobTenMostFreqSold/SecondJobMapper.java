package thirdJobTenMostFreqSold;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondJobMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		
		line = line.replace("\t", ",");
		//line = line.replace(",", "");
		StringTokenizer tokenizer = new StringTokenizer(line, ",");
		String x1 = tokenizer.nextToken();
		String x2 = tokenizer.nextToken();
		String num = tokenizer.nextToken();
		line = num + "," + x1 + "," + x2;
		
		
		context.write(new Text(line), NullWritable.get());	
		}
		
}
