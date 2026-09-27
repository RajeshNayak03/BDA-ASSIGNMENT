import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RegionConsistencyDriver {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf,
                "Most Consistent Region Yield");

        job.setJarByClass(RegionConsistencyDriver.class);

        job.setMapperClass(RegionConsistencyMapper.class);
        job.setReducerClass(RegionConsistencyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job,
                new Path(args[0]));

        FileOutputFormat.setOutputPath(job,
                new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
