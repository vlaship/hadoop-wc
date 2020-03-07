package vlaship.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HadoopTest {

    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;

    private static final IntWritable INT_ONE = new IntWritable(1);
    private static final LongWritable LONG_ONE = new LongWritable(1);

    @Before
    public void setup() {
        mapDriver = MapDriver.newMapDriver(new WordCountMapper());
        reduceDriver = ReduceDriver.newReduceDriver(new WordCountReducer());
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(LONG_ONE, new Text("cat dog"));
        mapDriver.withOutput(new Text("cat"), INT_ONE);
        mapDriver.withOutput(new Text("dog"), INT_ONE);
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws Exception{
        List<IntWritable> values = new ArrayList<>();
        values.add(INT_ONE);
        values.add(INT_ONE);
        reduceDriver.withInput(new Text("test"), values);
        reduceDriver.withOutput(new Text("test"), new IntWritable(2));
        reduceDriver.runTest();
    }
}
