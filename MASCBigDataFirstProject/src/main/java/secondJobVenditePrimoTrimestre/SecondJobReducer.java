package secondJobVenditePrimoTrimestre;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class SecondJobReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		
		String  trim = "";

		for (Text value : values) {
			trim = trim.concat(" "+value.toString());
		}
		StringTokenizer tokenizer = new StringTokenizer(trim," ");
		List<String> list = new ArrayList<String>();
		while (tokenizer.hasMoreTokens()){
			list.add(tokenizer.nextToken());
			
		}
		Collections.sort(list);
		
		String value = "";
		for (String s : list)
			value = value.concat(s + " ");

		context.write(key, new Text(value));
		
	}
	

}
