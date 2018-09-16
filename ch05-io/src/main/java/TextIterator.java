// cc TextIterator Iterating over the characters in a Text object

import org.apache.hadoop.io.Text;

import java.nio.ByteBuffer;

// vv TextIterator
public class TextIterator {

    public static void main(String[] args) {
        Text t = new Text("\u0041\u00DF\u6771\uD801\uDC00");

        ByteBuffer buf = ByteBuffer.wrap(t.getBytes(), 0, t.getLength());
        int cp;
        while (buf.hasRemaining() && (cp = Text.bytesToCodePoint(buf)) != -1) {
            System.out.println(Integer.toHexString(cp));
        }
    }
}
// ^^ TextIterator
