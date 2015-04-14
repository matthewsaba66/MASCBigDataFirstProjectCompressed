package fourthJobCouples;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ThirdJobCouplesMapper extends
		Mapper<LongWritable, Text,Text, DoubleWritable  > {

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text,Text, DoubleWritable >.Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		line = line.replace("\t", ",");
		line = line.replace(" ", ",");
		String[] words = line.split(",");
		int i = 0;
		int k = 1;
		int j = 2;

		for (j = 2; j<words.length; j = j+2){
			Text word = new Text(words[0]+","+words[j]);
			DoubleWritable num = new DoubleWritable(Double.parseDouble(words[j+1])/Double.parseDouble(words[k]));
			context.write(word,num);
		}
		
	}
	
	

}
