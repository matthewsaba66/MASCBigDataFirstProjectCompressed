package thirdJobTenMostFreqSold;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class FirstJobMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private static final IntWritable one = new IntWritable(1);
	private Text word = new Text();

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String regex = "(19|20)[0-9][0-9][--.]([0-9]|[1][0-2])[--.]([0-9]|1[0-9]|2[0-9]|3[0-1]),";
		line = line.replaceAll(regex, "");
		
		StringTokenizer tokenizer = new StringTokenizer(line, ",");
		
		List<String> list = new ArrayList<String>();
		while (tokenizer.hasMoreTokens()) {
			list.add(tokenizer.nextToken());
		}
		
		for (int i = 0; i < list.size(); i++) {
			   for (int j = i+1; j < list.size(); j++) {
			       String ei = list.get( i);
			       String ej = list.get( j);
			       String conc = ei + "," + ej;
			       word.set(conc);
			       context.write(word, one);
			   }
			}
		
		
	}

}
