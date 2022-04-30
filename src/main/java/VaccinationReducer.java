import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class VaccinationReducer extends Reducer<Text, Text, NullWritable, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        List<String[]> list = new ArrayList<>();
        for (Text value : values) {
            String str = value.toString();
            str = str + " ";
            String[] split = str.split(",");
            split[5] = split[5].substring(0, split[5].length() - 1);
            list.add(split);
        }
        double[] sum = new double[4];
        int[] count = new int[4];
        for(String[] split: list){
            for(int i = 2; i <= 5;i++){
                if(split[i].length()>0){
                    count[i-2]++;
                    sum[i-2]+=Double.parseDouble(split[i]);
                }
            }
        }
        String[] first = list.get(0);
        String out = String.join(",",first[0],first[1],
                count[0]==0?"":String.format("%.2f",sum[0]/count[0]),
                count[1]==0?"":String.format("%.2f",sum[1]/count[1]),
                count[2]==0?"":String.format("%.2f",sum[2]/count[2]),
                count[3]==0?"":String.format("%.2f",sum[3]/count[3]));
        context.write(NullWritable.get(),new Text(out));


    }
}
