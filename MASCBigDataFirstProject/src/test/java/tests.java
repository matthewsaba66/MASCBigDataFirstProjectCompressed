import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;





public class tests {
	public static void main(String[] args){

		/*	HashMap<String,Double> map = new HashMap<String,Double>();
		ValueComparator bvc =  new ValueComparator(map);
		TreeMap<String,Double> sorted_map = new TreeMap<String,Double>(bvc);

		map.put("A",99.5);
		map.put("B",67.4);
		map.put("C",67.4);
		map.put("D",67.3);

		System.out.println("unsorted map: "+map);

		sorted_map.putAll(map);

	/*	System.out.println("results: "+sorted_map);*/
	/*	String value = "dolce,formaggio	235";
		String line = value.toString();

		line = line.replace("\t", " ");
		//line = line.replace(",", "");

		StringTokenizer tokenizer = new StringTokenizer(line, " ");

		System.out.println(line);
		System.out.println(tokenizer.nextToken());
		System.out.println((Integer.parseInt((tokenizer.nextToken()))));*/
		
		String line1="34,pane,formaggio";
		String line2="65,dfgv,formaggio";
		StringTokenizer tok1 =new StringTokenizer(line1,",");
		StringTokenizer tok2 =new StringTokenizer(line2,",");
		Integer num1 = Integer.parseInt(tok1.nextToken());
		Integer num2 = Integer.parseInt(tok2.nextToken());
		System.out.println( num2.compareTo(num1));
		System.out.println( num1.compareTo(num2));
		System.out.println( num2.compareTo(num2));

		
		
		
	}
}

