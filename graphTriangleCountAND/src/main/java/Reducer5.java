import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * created by dmyan on 17-11-21
 */
public class Reducer5 extends Reducer<Text, Text, Text, Text> {
    private static long  result = 0;
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long count = 0;
        boolean flag = false;
        for (Text value:values){
            if(value.toString().trim().equals("link")){
                flag = true;
            }
            else{
                count++;
            }
        }
        if(flag)result += count;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new Text("The sum of triangle in social networks is"),new Text(result+""));
    }
}
