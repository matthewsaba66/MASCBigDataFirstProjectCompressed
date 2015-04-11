package secondJobVenditePrimoTrimestre;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondJobMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Text word = new Text();
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		
		line = line.replace("\t", ":");
		
		StringTokenizer tokenizer = new StringTokenizer(line," ");
		word.set(tokenizer.nextToken());
		
		context.write(word, new Text(tokenizer.nextToken()));
		//context.write(new Text("eseguito map"), new Text("eseguito Map"));
		}
			
	

}
