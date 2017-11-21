import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * created by dmyan on 17-11-21
 */
public class TriangleCountAnd {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Job job1 = Job.getInstance();
        job1.setJobName("Job1");
        job1.setJarByClass(TriangleCountAnd.class);
        job1.setMapperClass(Mapper1.class);
        job1.setReducerClass(Reducer1.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1,new Path(args[0]+"/gplus_combined.unique.txt"));
        FileOutputFormat.setOutputPath(job1,new Path(args[1]+"/tmp/step1"));
        job1.waitForCompletion(true);


        Job job2 = Job.getInstance();
        job2.setJobName("Job2");
        job2.setJarByClass(TriangleCountAnd.class);
        job2.setMapperClass(Mapper2.class);
        job2.setReducerClass(Reducer2.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2,new Path(args[1]+"/tmp/step1/part-r-00000"));
        FileOutputFormat.setOutputPath(job2,new Path(args[1]+"/tmp/step2"));
        job2.waitForCompletion(job1.isComplete());

        Job job3 = Job.getInstance();
        job3.setJobName("Job3");
        job3.setJarByClass(TriangleCountAnd.class);
        job3.setMapperClass(Mapper3.class);
        job3.setReducerClass(Reducer3.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job3,new Path(args[1]+"/tmp/step2/part-r-00000"));
        FileOutputFormat.setOutputPath(job3,new Path(args[1]+"/tmp/step3"));
        job3.waitForCompletion(job2.isComplete());

        Job job4 = Job.getInstance();
        job4.setJobName("Job4");
        job4.setJarByClass(TriangleCountAnd.class);
        job4.setMapperClass(Mapper4.class);
        job4.setReducerClass(Reducer4.class);
        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(Text.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job4,new Path(args[1]+"/tmp/step3/part-r-00000"));
        FileOutputFormat.setOutputPath(job4,new Path(args[1]+"/tmp/step4"));
        job4.waitForCompletion(job3.isComplete());

        Job job5 = Job.getInstance();
        job5.setJobName("Job5");
        job5.setJarByClass(TriangleCountAnd.class);
        job5.setMapperClass(Mapper5.class);
        job5.setReducerClass(Reducer5.class);
        job5.setMapOutputKeyClass(Text.class);
        job5.setMapOutputValueClass(Text.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job5,new Path(args[1]+"/tmp/step4/part-r-00000"));
        FileOutputFormat.setOutputPath(job5,new Path("gplus_CountAnd"));
        job5.waitForCompletion(job4.isComplete());
    }
}
