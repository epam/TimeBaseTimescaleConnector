package deltix.timebase.connector.util;

public class DiscoveryUtils {

    private static final String ASTERISK_CHAR = "*";
    private static final String QUESTION_MARK_CHAR = "?";
    private static final String DOT_CHAR = ".";

    public static boolean isExpression(String text) {
        if (text.contains(ASTERISK_CHAR) || text.contains(QUESTION_MARK_CHAR) || text.contains(DOT_CHAR)) {
            return true;
        } else {
            return false;
        }
    }

    public static String generateRegExp(String wildcard) {
        StringBuilder out = new StringBuilder("^");
        for (int i = 0; i < wildcard.length(); ++i) {
            final char c = wildcard.charAt(i);
            switch (c) {
                case '*':
                    out.append(".*");
                    break;
                case '?':
                    out.append('.');
                    break;
                case '.':
                    out.append("\\.");
                    break;
                case '\\':
                    out.append("\\\\");
                    break;
                default:
                    out.append(c);
            }
        }
        out.append('$');
        return out.toString();
    }
}
