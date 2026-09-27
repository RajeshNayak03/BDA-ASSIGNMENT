import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RegionSeasonReducer
        extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private DoubleWritable result = new DoubleWritable();

    public void reduce(Text key, Iterable<DoubleWritable> values,
                       Context context)
            throws IOException, InterruptedException {

        double total = 0.0;

        for (DoubleWritable value : values) {
            total += value.get();
        }

        result.set(total);
        context.write(key, result);
    }
}
