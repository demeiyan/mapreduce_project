import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;

/**
 * Created by dmyan on 17-10-26.
 */
public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {


    @Override
    protected void reduce(Text key, Iterable<Text> values,
                          Context context) throws IOException,InterruptedException {

/*        Map<String,Integer> map = new HashMap<String,Integer>();
        int fren = 0;
        int sum ;
        String fileName;
        int num;
        for (Text val : values) {
            fileName = val.toString().split(":")[0];
            num = Integer.parseInt(val.toString().split(":")[1].trim());
            fren+=num;
            if(map.containsKey(fileName)){
                map.put(fileName,num+map.get(fileName));
            }else {
                map.put(fileName,num);
            }
            //sum += val.get();
        }
        sum = map.size();
        key = new Text(key.toString()+"\t"+fren/(sum*1.0));
        StringBuilder all = new StringBuilder();
        for(String k :map.keySet()){
            all.append(k+":"+map.get(k)+";");
        }
        Text word = new Text();*/

        StringBuilder all = new StringBuilder();
        int fren = 0;
        int sum = 0;
        int index;
        Iterator<Text> iter = values.iterator();
        String str;
        if(iter.hasNext()){
            str = iter.next().toString();
            index = str.indexOf(":");
            fren+=Integer.parseInt(str.substring(index+1));
            sum++;
            all.append(str);
        }
        while(iter.hasNext()){
            all.append(";");
            str = iter.next().toString();
            index = str.indexOf(":");
            fren+=Integer.parseInt(str.substring(index+1));
            sum++;
            all.append(str);
        }
        context.write(key,new Text(new DecimalFormat("######0.00").format(fren/(sum*1.0))+","+all.toString()));

    }

    /*@Override
    public void run(Reducer<Text, IntWritable, Text, Text>.Context context) throws IOException, InterruptedException {
        super.setup(context);
        try{
            while(context.nextKey()){

                Text currentKey = context.getCurrentKey();
                Log.info(context.getJobName() + " " + context.getJobID() + currentKey.toString());
                String fileName = currentKey.toString().split(",")[1];
                Iterable<IntWritable> currentValues = context.getValues();
                List<Text> list = new ArrayList<Text>();
                for(IntWritable val:currentValues){
                    list.add(new Text(fileName+":"+val.get()));
                }
                reduce(new Text(currentKey.toString().split(",")[0]),list,context);

            }
            Iterator<IntWritable> iter = context.getValues().iterator();
            if (iter instanceof ReduceContext.ValueIterator) {
                ((ReduceContext.ValueIterator)iter).resetBackupStore();
            }
        }finally{
            super.cleanup(context);
        }
    }*/
}
