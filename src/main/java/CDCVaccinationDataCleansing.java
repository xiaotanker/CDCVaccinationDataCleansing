import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CDCVaccinationDataCleansing {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: CDCVaccinationDataCleansing <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "cleansing");
        job.setJarByClass(CDCVaccinationDataCleansing.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setReducerClass(VaccinationReducer.class);
        job.setMapperClass(VaccinationMapper.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.waitForCompletion(true);

    }
}
