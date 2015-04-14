package fourthJobCouples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class SecondJobJoinReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		
	/*	List<Text> list = new ArrayList<Text>();
		while(values.hasNext()){
			list.add(values.next());
		}
		String merge = "";
		int i =0;
		 for (Text value : list) {
		  if(i == 0){
		   merge = value.toString()+", XXX ";
		  }
		  else{
		   merge += value.toString();
		  }
		  i++;
		 }
		 Text word = new Text(merge);
		 output.collect(key, word);
		*/
		
		String s = "";
		
		while(values.hasNext()){
			
			s = s.concat(values.next().toString()+" ");
		}
		
		String[] words=s.split(" ");
		if (isInt(words[0])){
			s = s.replace(words[0] + " ", "");
			output.collect(new Text(key +"," + words[0]), new Text(s));
		}
		else {
			s = s.replace(" " + words[words.length-1], "");
			
			output.collect(new Text(key + "," + words[words.length-1]), new Text(s));
		}
		
	}
	
	
	public static boolean isInt(String str) {

		try{
			int iCheck = Integer.parseInt(str);
			return true;
		}
		catch(NumberFormatException e) { return false; }

	}

	
}
