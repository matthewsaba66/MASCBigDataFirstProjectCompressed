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

		/*String value = "345,sde,dolce,formaggio	235";
		String line=value.toString();
		line = line.replace("\t", ",");
		String[] words=line.split(",");
		//Text word = new Text(words[0]);
		System.out.print((words[words.length-1]));
*/
		Double num = 0.7704402515723271;
		num = Math.floor(num);
		System.out.println(num);




	}

	public static boolean isInt(String str) {

		try{
			int iCheck = Integer.parseInt(str);
			return true;
		}
		catch(NumberFormatException e) { return false; }

	}
}

