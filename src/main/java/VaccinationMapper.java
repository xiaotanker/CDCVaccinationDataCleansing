
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class VaccinationMapper
        extends Mapper<LongWritable, Text, Text, Text>{
        @Override
        public void map(LongWritable key, Text value, Mapper.Context context)
        throws IOException, InterruptedException {

        String line = value.toString();
        line = line + " ";

        String[] split = line.split(",");
        split[65] = split[65].substring(0,split[65].length()-1);

        if(split[0].equals("Date")){
            return;
        }
        String[] date = split[0].split("/");
        String month = String.join("-",date[2],date[0]);
        String output = String.join(",", month ,split[3],split[5],split[7],split[17],split[29]);


        if(split[5].length()>0 || split[7].length() > 0 || split[17].length() >0 || split[29].length() >0) {
            if (split[5].equals("0") && split[7].equals("0") && split[17].equals("0") && split[29].equals("0")) {
                return;
            }
            context.write(new Text(String.join(",", month, split[3])), new Text(output));
        }
    }
}
