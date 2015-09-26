 import java.io.IOException;  
 import java.util.HashMap;  
 import java.util.Iterator;  
 import java.util.Map;  
 import java.util.Map.Entry;  
 import org.apache.hadoop.conf.Configuration;  
 import org.apache.hadoop.fs.Path;  
 import org.apache.hadoop.hbase.HBaseConfiguration;  
 import org.apache.hadoop.hbase.client.HTable;  
 import org.apache.hadoop.hbase.client.Put;  
 import org.apache.hadoop.hbase.io.ImmutableBytesWritable;  
 import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;  
 import org.apache.hadoop.hbase.util.Bytes;  
 import org.apache.hadoop.io.LongWritable;  
 import org.apache.hadoop.io.Text;  
 import org.apache.hadoop.mapreduce.Job;  
 import org.apache.hadoop.mapreduce.Mapper;  
 import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
 import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;  
 import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
 public class HBaseBulkLoad {  
      public static class BulkLoadMap extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {       
           public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {  
                String line = value.toString();
                String[] words = line.split("\t");       
                String rowKey = words[0] ;
                
                ImmutableBytesWritable HKey = new ImmutableBytesWritable(Bytes.toBytes(rowKey));  
                Put HPut = new Put(Bytes.toBytes(rowKey));  
                HPut.add(Bytes.toBytes("wordpairs"), Bytes.toBytes("p"), Bytes.toBytes(words[1])); 
                context.write(HKey,HPut);  
           }   
      }  
      public static void main(String[] args) throws Exception {  
           Configuration conf = HBaseConfiguration.create();  
           String inputPath = args[0];  
           String outputPath = args[1];  
           HTable hTable = new HTable(conf, args[2]);  
           Job job = new Job(conf,"HBaseBulkLoad");        
           job.setMapOutputKeyClass(ImmutableBytesWritable.class);  
           job.setMapOutputValueClass(Put.class);  
           job.setSpeculativeExecution(false);  
           job.setReduceSpeculativeExecution(false);  
           job.setInputFormatClass(TextInputFormat.class);  
           job.setOutputFormatClass(HFileOutputFormat.class);  
           job.setJarByClass(HBaseBulkLoad.class);  
           job.setMapperClass(HBaseBulkLoad.BulkLoadMap.class);  
           FileInputFormat.setInputPaths(job, new Path(inputPath));  
           FileOutputFormat.setOutputPath(job,new Path(outputPath));             
           HFileOutputFormat.configureIncrementalLoad(job, hTable);  
           System.exit(job.waitForCompletion(true) ? 0 : 1);  
      }  
 }  