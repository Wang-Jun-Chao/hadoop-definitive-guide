package com.hadoopbook.pig;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertThat;

public class RangeTest {

    @Test
    public void parsesEmptyRangeSpec() {
        assertThat(Range.parse("").size(), is(0));
    }

    @Test
    public void parsesSingleRangeSpec() {
        List<Range> ranges = Range.parse("1-3");
        assertThat(ranges.size(), is(1));
        assertThat(ranges.get(0), is(new Range(1, 3)));
    }

    @Test
    public void parsesMultipleRangeSpec() {
        List<Range> ranges = Range.parse("1-3,5-10");
        assertThat(ranges.size(), is(2));
        assertThat(ranges.get(0), is(new Range(1, 3)));
        assertThat(ranges.get(1), is(new Range(5, 10)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void failsOnInvalidSpec() {
        Range.parse("1-n");
    }
}
