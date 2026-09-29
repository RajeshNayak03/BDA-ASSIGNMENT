import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Top3RegionTrendDriver {

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println(
                    "Usage: Top3RegionTrendDriver <input> <output>"
            );
            System.exit(1);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(
                conf,
                "P5 - Top 3 Producing Regions and Seasonal Trends"
        );

        job.setJarByClass(Top3RegionTrendDriver.class);

        job.setMapperClass(Top3RegionTrendMapper.class);
        job.setReducerClass(Top3RegionTrendReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(
                job,
                new Path(args[0])
        );

        FileOutputFormat.setOutputPath(
                job,
                new Path(args[1])
        );

        // One reducer is used so the global top 3 can be selected.
        job.setNumReduceTasks(1);

        System.exit(
                job.waitForCompletion(true) ? 0 : 1
        );
    }
}
