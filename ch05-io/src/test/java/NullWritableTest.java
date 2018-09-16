import org.apache.hadoop.io.NullWritable;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertThat;

public class NullWritableTest extends WritableTestBase {

    @Test
    public void test() throws IOException {
        NullWritable writable = NullWritable.get();
        assertThat(serialize(writable).length, is(0));
    }
}
