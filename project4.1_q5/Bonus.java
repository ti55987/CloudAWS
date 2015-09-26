import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.net.URI;
import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.filecache.DistributedCache;


public class Bonus {
  public static HashSet<String> stop_list;

  public static class BonusMapper extends MapReduceBase
      implements Mapper<LongWritable, Text, Text, Text> {

    private final static Text word = new Text();
    private final static Text location = new Text();
    private Path[] localFiles;
       
       public void configure(JobConf job) {
         // Get the cached archives/files
         try {
          localFiles = DistributedCache.getLocalCacheFiles(job);
          parseSkipFile(localFiles[0]);
         } catch (IOException e) {
          e.printStackTrace();
        }

         
    }

    private static void parseSkipFile(Path patternsFile) {
        try {
          BufferedReader buf = new BufferedReader(new FileReader(new File(patternsFile.getName())));
          String pattern = null;
          
          while ((pattern = buf.readLine()) != null) {
            stop_list.add(pattern);
          }
        
        } catch (IOException e) {
          e.printStackTrace();
        }
    }

    public void map(LongWritable key, Text val,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {

      FileSplit fileSplit = (FileSplit)reporter.getInputSplit();
      String fileName = fileSplit.getPath().getName();
      location.set(fileName);

      String line = val.toString();
      StringTokenizer itr = new StringTokenizer(line.toLowerCase());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        output.collect(word, location);
      }
    }
  }



  public static class BonusReducer extends MapReduceBase
      implements Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterator<Text> values,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {
      StringBuilder word = new StringBuilder();
      word.append(key.toString());
      word.append(" : ");
      boolean first = true;
      StringBuilder toReturn = new StringBuilder();
      while (values.hasNext()){
        if (!first)
          toReturn.append(" ");
        first=false;
        toReturn.append(values.next().toString());
      }

      output.collect(new Text(word.toString()), new Text(toReturn.toString()));
    }
  }


  /**
   * The actual main() method for our program; this is the
   * "driver" for the MapReduce job.
   */
  public static void main(String[] args) {
    JobClient client = new JobClient();
    JobConf conf = new JobConf(Bonus.class);
    try {
      DistributedCache.addCacheFile(new URI("/mnt/stop"), conf);
    } catch (Exception e) {
      e.printStackTrace();
    }
    conf.setJobName("Bonus");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    conf.setMapperClass(BonusMapper.class);
    conf.setReducerClass(BonusReducer.class);

    client.setConf(conf);

    try {
      JobClient.runJob(conf);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}