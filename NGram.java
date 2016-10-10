import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.List;
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
import org.apache.hadoop.util.Tool;

public class NGram {

  public static class NGramMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
	  private Text word = new Text();
    static int cnt = 0;
    List ls = new ArrayList();
    @SuppressWarnings("unchecked")
	
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer dt = new StringTokenizer(value.toString(), " ");
      
      while (dt.hasMoreTokens()) {
        ls.add(dt.nextToken());
      }
    }

    @Override
      protected void cleanup(Context context) throws IOException, InterruptedException {
        int n = Integer.parseInt(context.getConfiguration().get("num"));
        StringBuilder str = new StringBuilder("");
        for (int i = 0; i < ls.size() - n; i++) {
          int k = i;
          for(int j = 0; j < n; j++) 
          {
            if(j == 0) {
              str = str.append(ls.get(k));              
            } 
            else 
            {
              str = str.append(" ");
              str = str.append(ls.get(k)); 
            }
            k++;
          }
          word.set(str.toString());
          str=new StringBuilder("");
          context.write(word, one);
        }
      }
  }

  public static class NGramReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

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

  public static void main(String[] args) throws Exception {

  	Configuration conf = new Configuration();
  	conf.set("num", args[2]);
  	
    Job job = Job.getInstance(conf, "NGram");
    job.setJarByClass(NGram.class);
    job.setMapperClass(NGramMapper.class);
    job.setReducerClass(NGramReducer.class);
	  job.setCombinerClass(NGramReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }
}
