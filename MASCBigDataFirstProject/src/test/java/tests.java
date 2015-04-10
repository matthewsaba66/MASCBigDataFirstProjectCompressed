
public class tests {
	public static void main(String[] args){
		String line = "2015-3-15,fcggtrgh";
		String regex = "(19|20)[0-9][0-9][--.]([0-9]|[1][0-2])[--.]([0-9]|1[0-9]|2[0-9]|3[0-1]),";
		line = line.replaceAll(regex, "");
		System.out.println(line);
		
	}
}
