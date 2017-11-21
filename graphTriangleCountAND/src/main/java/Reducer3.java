import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * created by dmyan on 17-11-21
 */
public class Reducer3 extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        boolean isLink = false;
        boolean isSearch = false;
        for(Text value:values){
            String rel = value.toString();
            if(rel.equals("link")){
                isLink = true;
            }else if(rel.equals("search")){
                isSearch = true;
            }
        }
        if(isLink&&isSearch){
            String[] edge = key.toString().split("-");
            if(edge[0].compareTo(edge[1])<0){
                context.write(new Text(edge[0]),new Text(edge[1]));
            }
        }
    }
}
