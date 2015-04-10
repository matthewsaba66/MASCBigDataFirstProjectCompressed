package secondJobVenditePrimoTrimestre;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Mapper;

public class ThreeSellMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	public void map(LongWritable arg0, Text arg1,
			OutputCollector<Text, IntWritable> arg2, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		
	}

	private Text word = new Text();
	private static final IntWritable one = new IntWritable(1);
	
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output,  Context context)
			throws IOException, InterruptedException {
		
		
		String line = value.toString();
		int month = 0;
		if (line.charAt(6) == '-')	{
			month = Integer.parseInt(line.substring(5,5));
		}
		
		String products = null;
		
		if (month == 1 || month == 2 || month == 3)	{
			if (line.charAt(8) == '-')	{
				products = line.substring(9, 100);
			}
			else if (line.charAt(9) == '-')	{
				products = line.substring(10, 100);
			}
			else if (line.charAt(10) == '-')	{
				products = line.substring(11, 100);
			}
			
		}
		
		StringTokenizer tokenizer = new StringTokenizer(products);

		while (tokenizer.hasMoreTokens()) {
			word.set(tokenizer.nextToken());
			context.write(word, one);
		}

	}

}

/*
 * 
 * 	private static final IntWritable one = new IntWritable(1);
	private Text word = new Text();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);

		while (tokenizer.hasMoreTokens()) {
			word.set(tokenizer.nextToken());
			context.write(word, one);
		}

	}
 */
