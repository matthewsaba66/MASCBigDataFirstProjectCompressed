import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;


public class tests {
	public static void main(String[] args){

		String value = "3/2015:23 2/2015:35 1/2015:18";
		String line = value.toString();
		//line = line.replace("\t", ":");
		System.out.println(line);
		StringTokenizer tokenizer = new StringTokenizer(line," ");
		List<String> list = new ArrayList<String>();
		while (tokenizer.hasMoreTokens()){
			list.add(tokenizer.nextToken());
			
		}
		Collections.sort(list);
		
		for (String s : list)
			System.out.println(s);
		
		
	}
}
