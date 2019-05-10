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
    private static class WordCntPair {
        String word;
        int cnt;
        WordCntPair(String _word, int _cnt) {
            word=_word;
            cnt=_cnt;
        }
    }

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
        // data structures
        private Comparator<WordCntPair> comparator =new Comparator<WordCntPair>() {
            @Override
            public int compare(WordCntPair o1, WordCntPair o2) {
                if(o1.cnt!=o2.cnt) {
                    return Integer.compare(o1.cnt, o2.cnt);
                }
                return o2.word.compareTo(o1.word);
            }
        };
        private Map<String, Integer> cntMap=new HashMap<>();
        private PriorityQueue<WordCntPair> minHeap=new PriorityQueue<>(100, comparator);

        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            if(!cntMap.containsKey(key.toString())) {
                cntMap.put(key.toString(), sum);
            }
            else {
                cntMap.put(key.toString(), cntMap.get(key.toString())+sum);
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            for(Map.Entry<String, Integer> entry:cntMap.entrySet()) {
                minHeap.add(new WordCntPair(entry.getKey(), entry.getValue()));
                if(minHeap.size()>100) {
                    minHeap.poll();
                }
            }

            Text tempText=new Text();
            IntWritable cntWrite=new IntWritable();
            while(!minHeap.isEmpty()) {
                WordCntPair pair=minHeap.poll();
                tempText.set(pair.word);
                cntWrite.set(pair.cnt);
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
        conf.set("mapreduce.input.fileinputformat.split.maxsize", "100");
        conf.set("mapreduce.input.fileinputformat.split.minsize", "100");
        job.setNumReduceTasks(10);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean success=job.waitForCompletion(true);

        long endTime=System.currentTimeMillis();
        System.out.printf("Total Execution Time is: %d s\n", (endTime-startTime)/1000);
        System.exit(success?0:1);
    }
}