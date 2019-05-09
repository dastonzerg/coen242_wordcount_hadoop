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

    public static class TopNReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {

        private Map<String, Integer> cntMap=new HashMap<>();
        private PriorityQueue<String> minHeap=new PriorityQueue<>(
                (o1, o2)->!cntMap.get(o1).equals(cntMap.get(o2))
                        ?Integer.compare(cntMap.get(o1), cntMap.get(o2))
                        :o2.compareTo(o1));
        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            cntMap.put(key.toString(), sum);
            minHeap.add(key.toString());
            if(minHeap.size()>100) {
                String removed=minHeap.poll();
                cntMap.remove(removed);
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            Text tempText=new Text();
            IntWritable cntWrite=new IntWritable();
            List<String> tempLst=new ArrayList<>();
            while(!minHeap.isEmpty()) {
                tempLst.add(minHeap.poll());
            }
            for(String word:tempLst) {
                tempText.set(word);
                cntWrite.set(cntMap.get(word));
                context.write(tempText, cntWrite);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        long startTime=System.currentTimeMillis();
        // job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(TopNReducer.class);
        job.setReducerClass(TopNReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(100);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean success=job.waitForCompletion(true);

        long endTime=System.currentTimeMillis();
        System.out.printf("Total Execution Time is: %d s\n", (endTime-startTime)/1000);
        System.exit(success?0:1);
    }
}