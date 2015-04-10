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

public class FirstJobMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	
	private Text word = new Text();
	private static final IntWritable one = new IntWritable(1);
	
	public void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		
		String line = value.toString();
		String date = "";
		String product = "";
		
		if (line.charAt(6)=='-' && (line.charAt(5)=='1' || line.charAt(5)=='2' 
				|| line.charAt(5)=='3') && line.substring(0, 4).equals("2015"))	{
			date = line.charAt(5)+ "/" +line.substring(0,4);
			if (line.charAt(8)==','){
				product = line.substring(9);
			}
			else {
				product = line.substring(10);
			}
			
		}
		
		StringTokenizer tokenizer = new StringTokenizer(product,",");

		while (tokenizer.hasMoreTokens()) {
			word.set(tokenizer.nextToken());
			Text data = new Text(word.toString()+" "+date);
			
			context.write(data, one);
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
