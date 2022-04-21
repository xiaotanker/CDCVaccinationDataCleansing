
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class VaccinationMapper
        extends Mapper<LongWritable, Text, NullWritable, Text>{
        @Override
        public void map(LongWritable key, Text value, Mapper.Context context)
        throws IOException, InterruptedException {

        String line = value.toString();
        line = line + " ";

        String[] split = line.split(",");
        split[65] = split[65].substring(0,split[65].length()-1);


            String output = String.join(",",split[0],split[3],split[5],split[7],split[17],split[29]);

            context.write(NullWritable.get(), new Text(output));


    }
}
