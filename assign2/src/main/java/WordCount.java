import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordCount {
    static Map<String, Integer> cntMap=new HashMap<>();
    static PriorityQueue<String> minHeap=new PriorityQueue<>(
            (o1, o2)->!cntMap.get(o1).equals(cntMap.get(o2))
                    ?Integer.compare(cntMap.get(o1), cntMap.get(o2))
                    :o2.compareTo(o1));

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), " +");
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class CollectorReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException{
            String keyStr=key.toString();
            minHeap.add(keyStr);
            Iterator<IntWritable> ite=values.iterator();
            cntMap.put(keyStr, ite.next().get());
            if(minHeap.size()>100) {
                String removed=minHeap.poll();
                cntMap.remove(removed);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        long startTime=System.currentTimeMillis();
        // job1
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "word count");
        job1.setJarByClass(WordCount.class);
        job1.setMapperClass(TokenizerMapper.class);
        job1.setCombinerClass(IntSumReducer.class);
        job1.setReducerClass(IntSumReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setNumReduceTasks(100);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path("/tempPath"));
        job1.waitForCompletion(true);
        // job2
        Configuration conf2=new Configuration();
        Job job2=Job.getInstance(conf2, "word pick");
        job2.setJarByClass(WordCount.class);
        job2.setMapperClass(Mapper.class);
        job2.setReducerClass(CollectorReducer.class);
        job2.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job2, new Path("/tempPath"));

        long endTime=System.currentTimeMillis();
        System.out.printf("Total Execution Time is: %d s\n", (endTime-startTime)/1000);
    }
}