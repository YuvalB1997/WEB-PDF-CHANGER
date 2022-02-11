package common;
public class StringUtils {
    public static String spaceSeparatedString(String[] args, int start, int end) {
        String out = "";
		for (; start < end; start++) {
			out += " " + args[start];
		}
		return out.substring(1);
    }

}
