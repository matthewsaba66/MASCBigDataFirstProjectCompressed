
public class Main {

	public static void main(String[] args) {
		String s = "prova\n\nprsva";
		System.out.println(s);
		System.out.println("-----------------------");
		s = s.replaceFirst("(?m)^[ \t]*\r?\n", "");
		
		System.out.println(s);

		



	}

}
