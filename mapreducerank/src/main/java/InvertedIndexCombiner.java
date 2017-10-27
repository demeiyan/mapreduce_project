import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by dmyan on 17-10-27.
 */
public class InvertedIndexCombiner extends Reducer<Text,Text,Text,Text> {

    @Override
    public void reduce(Text key,Iterable<Text> values ,Context context) throws IOException, InterruptedException {
        int sum = 0;
        for(Text value : values){
            sum += Integer.parseInt(value.toString());
        }
        int index = key.toString().indexOf(",");
        String fileInfo = key.toString().substring(index+1);//+":"+sum;
        key.set(key.toString().substring(0,index));
        if(fileInfo.contains(".")){
            index = fileInfo.indexOf(".");
        }else {
            index = fileInfo.length();
        }
        fileInfo = fileInfo.substring(0,index)+":"+sum;
        context.write(key,new Text(fileInfo));
    }
}