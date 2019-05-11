import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {
    private static class WordCntPair {
        final Text word;
        final IntWritable cnt;
        WordCntPair(Text _word, IntWritable _cnt) {
            word=_word;
            cnt=_cnt;
        }
    }

    public static class TopNMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
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
                if(o1.cnt.get()!=o2.cnt.get()) {
                    return Integer.compare(o1.cnt.get(), o2.cnt.get());
                }
                return o2.word.toString().compareTo(o1.word.toString());
            }
        };
        private java.util.HashMap<String, Integer> cntMap=new HashMap<>();
        private PriorityQueue<WordCntPair> minHeap=new PriorityQueue<>(100, comparator);

        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) {
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
                minHeap.add(new WordCntPair(new Text(entry.getKey()), new IntWritable(entry.getValue())));
                if(minHeap.size()>100) {
                    minHeap.poll();
                }
            }

            while(!minHeap.isEmpty()) {
                WordCntPair pair=minHeap.poll();
                context.write(pair.word, pair.cnt);
            }
        }
    }

//    public static class CollectMapper
//            extends Mapper<Text, Text, Text, IntWritable> {
//        private Comparator<WordCntPair> comparator =new Comparator<WordCntPair>() {
//            @Override
//            public int compare(WordCntPair o1, WordCntPair o2) {
//                if(o1.cnt!=o2.cnt) {
//                    return Integer.compare(o1.cnt, o2.cnt);
//                }
//                return o2.word.compareTo(o1.word);
//            }
//        };
//        private PriorityQueue<WordCntPair> minHeap=new PriorityQueue<>(100, comparator);
//
//        @Override
//        public void map(Text key, Text value, Context context
//        ) {
//            String word=key.toString();
//            int cnt=Integer.valueOf(value.toString());
//            minHeap.add(new WordCntPair(word, cnt));
//            if(minHeap.size()>100) {
//                minHeap.poll();
//            }
//        }
//
//        @Override
//        protected void cleanup(Context context)
//                throws IOException, InterruptedException {
//            Text tempText=new Text();
//            IntWritable cntWrite=new IntWritable();
//            while(!minHeap.isEmpty()) {
//                WordCntPair pair=minHeap.poll();
//                tempText.set(pair.word);
//                cntWrite.set(pair.cnt);
//                context.write(tempText, cntWrite);
//            }
//        }
//    }
//
//    public static class CollectReducer
//            extends Reducer<Text,IntWritable,Text,IntWritable> {
//        private Comparator<WordCntPair> comparator =new Comparator<WordCntPair>() {
//            @Override
//            public int compare(WordCntPair o1, WordCntPair o2) {
//                if(o1.cnt!=o2.cnt) {
//                    return Integer.compare(o1.cnt, o2.cnt);
//                }
//                return o2.word.compareTo(o1.word);
//            }
//        };
//        private PriorityQueue<WordCntPair> minHeap=new PriorityQueue<>(100, comparator);
//
//        @Override
//        public void reduce(Text key, Iterable<IntWritable> values,
//                           Context context) {
//            String word=key.toString();
//            Iterator<IntWritable> ite=values.iterator();
//            int cnt=ite.next().get();
//            minHeap.add(new WordCntPair(word, cnt));
//            if(minHeap.size()>100) {
//                minHeap.poll();
//            }
//        }
//
//        @Override
//        protected void cleanup(Context context)
//                throws IOException, InterruptedException {
//            Text tempText=new Text();
//            IntWritable cntWrite=new IntWritable();
//            LinkedList<WordCntPair> tempLst=new LinkedList<>();
//            while(!minHeap.isEmpty()) {
//                tempLst.addFirst(minHeap.poll());
//            }
//            for(WordCntPair pair:tempLst) {
//                tempText.set(pair.word);
//                cntWrite.set(pair.cnt);
//                context.write(tempText, cntWrite);
//            }
//        }
//    }

    public static void main(String[] args) throws Exception {
        long startTime=System.currentTimeMillis();
        Configuration conf = new Configuration();
        // job1
        Job job1 = Job.getInstance(conf, "top_words");
        job1.setJarByClass(WordCount.class);
        job1.setMapperClass(TopNMapper.class);
        job1.setCombinerClass(TopNReducer.class);
        job1.setReducerClass(TopNReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        boolean success=job1.waitForCompletion(true);

//        // job2
//        Job job2 = Job.getInstance(conf, "collect_words");
//        job2.setJarByClass(WordCount.class);
//        job2.setMapperClass(CollectMapper.class);
//        job2.setReducerClass(CollectReducer.class);
//        job2.setOutputKeyClass(Text.class);
//        job2.setOutputValueClass(IntWritable.class);
//        job2.setNumReduceTasks(1);
//        FileInputFormat.addInputPath(job2, tempPath);
//        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
//        job2.setInputFormatClass(KeyValueTextInputFormat.class);
//        job2.setOutputFormatClass(TextOutputFormat.class);
//
//        boolean success=job2.waitForCompletion(true);
        long endTime=System.currentTimeMillis();
        System.out.printf("Total Execution Time is: %d s\n", (endTime-startTime)/1000);
        System.exit(success?0:1);
    }
}