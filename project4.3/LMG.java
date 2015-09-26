import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.client.Put;  
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;  

public class LMG {
	

	
	public static class T_Map extends Mapper<LongWritable, Text, Text, Text>
	{
		private final static Text count = new Text();
		private final static Text word_c = new Text();
		private Text phrase = new Text();
		private int t=0;
		public void map(LongWritable key, Text value, Context context) throws 
		IOException, InterruptedException{
			
			String line = value.toString();
			String [] words=line.split("\\s+");
			String[] copyTo ;
			int len = words.length;
			if( len > 1 && Integer.parseInt(words[len-1]) > t){
				if(len < 6){
					copyTo = (String[]) java.util.Arrays.copyOfRange(words, 0, len-1);
					phrase.set(StringUtils.join(" " , copyTo));
					count.set(words[len-1]);
					context.write(phrase, count);
				}
				
				if(len > 2){
					copyTo = (String[]) java.util.Arrays.copyOfRange(words, 0, len-2);
					phrase.set(StringUtils.join(" " , copyTo));
					copyTo = (String[]) java.util.Arrays.copyOfRange(words, len-2, len);
					word_c.set(StringUtils.join(" " , copyTo));
					context.write(phrase, word_c);
				}
			}
		}
        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            Configuration conf = context.getConfiguration();
            t = conf.getInt("t",-1);
        }
        
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text>
	{
		private Text out = new Text();
		private int n,t;
		public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException{
			double phra_count = 0;
			double pr = 0;
			String phrase = key.toString();
			List<String[]> cache = new ArrayList<String[]>();
			HashMap<String, Double> hmap = new HashMap<String, Double>();

			for(Text val : values){
				String[] word = val.toString().split("\\s+");
				if(word.length == 1) {
					phra_count = Double.parseDouble(word[0]);
				}
				else cache.add(word);
			}
			
			if(phra_count > 0){
				for(String[] val : cache){					
					if(val.length > 1) {
						pr = Double.parseDouble(val[1])/phra_count;
						hmap.put(val[0],pr);
					}
				}
				String or_out = sortByValues(hmap,n);			

				if(or_out != ""){
					out.set(phrase); 
					context.write(out, new Text(or_out));

				}
			}		
		}
        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            Configuration conf = context.getConfiguration();
            n = conf.getInt("n",100);
            t = conf.getInt("t",-1);
        }
        public static <K extends Comparable,V extends Comparable> String sortByValues(Map<K,V> map, int n){
	        List<Map.Entry<K,V>> entries = new LinkedList<Map.Entry<K,V>>(map.entrySet());
	        
	        Collections.sort(entries, Collections.reverseOrder(new Comparator<Map.Entry<K,V>>() {
	        	
	            public int compare(Entry<K, V> o1, Entry<K, V> o2) {
	                return o1.getValue().compareTo(o2.getValue());
	            }
	        }));	      

	        String or_out="";
	        int count = 0;
	        for(Map.Entry<K,V> entry: entries){
	        	if(count == n) break; 
	        	or_out = or_out +entry.getKey()+";";
	            count++;
	        }      
	        return or_out;
	    }
        
        
	}
	
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		int t;
		int n;
		Configuration conf  = new Configuration();
		t = Integer.parseInt(args[2]);
		n = Integer.parseInt(args[3]);
		conf.setInt("t", t);
		conf.setInt("n", n);
		
		Job job = new Job(conf,"LMG");
		job.setJarByClass(LMG.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(T_Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

	}
}
