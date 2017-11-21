import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * created by dmyan on 17-11-21
 */
public class Reducer4 extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        ArrayList<String> endList = new ArrayList<String>();
        Text edge = new Text();
        Text rel = new Text();
        rel.set("link");
        for(Text value:values){
            endList.add(value.toString());
            edge.set(key.toString()+"-"+value.toString());
            context.write(edge,rel);
        }
        rel.set("search");
        for(int i = 0; i<endList.size();i++){
            for(int j = i+1; j<endList.size();j++){
                String a = endList.get(i);
                String b = endList.get(j);
                if(a.compareTo(b)<0){
                    edge.set(a+"-"+b);

                }else{
                    edge.set(b+"-"+a);
                }
                context.write(edge,rel);
            }
            context.progress();
        }
    }
}
