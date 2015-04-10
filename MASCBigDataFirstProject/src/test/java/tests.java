import java.util.StringTokenizer;


public class tests {
	public static void main(String[] args){

		String value = "2015-3-29,ciao,pippo,pluto,cdf";
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
		
		//StringTokenizer tokenizer = new StringTokenizer(product,",");
		
		System.out.println(line.charAt(6)=='-');
		System.out.println(line.charAt(5)=='3');
		System.out.println(line.substring(0, 4).equals("2015"));
		System.out.println(line.charAt(9)==',');
		System.out.println(date);
		System.out.println(product);
		
	}
}
