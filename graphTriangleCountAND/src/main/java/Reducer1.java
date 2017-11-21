import org.apache.hadoop.io.Text;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * 去重
 * created by dmyan on 17-11-21
 */
public class Reducer1 extends Reducer<Text, Text, Text, Text> {
    private Text value = new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        value.set("");
        context.write(key,value);
    }
}
