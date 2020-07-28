	
	
	
	                                                                                           
	import java.io.IOException;                                                                
	import java.util.*;                                                                  
	
	import org.apache.hadoop.conf.Configuration;                                               
	import org.apache.hadoop.fs.Path;                                                          
	import org.apache.hadoop.io.IntWritable;                                                   
	import org.apache.hadoop.io.LongWritable;                                           
	import org.apache.hadoop.mapreduce.Job;                                                    
	import org.apache.hadoop.mapreduce.Mapper;
	import org.apache.hadoop.mapreduce.Reducer;
	import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;                              
	import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;                              
	import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;                            
	import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;                            
	import org.apache.hadoop.io.Text;                                                          
	//import org.apache.hadoop.util.GenericOptionsParser;                                      
	import org.json.*;                                                                         
	                                                                                           
	//import javax.security.auth.callback.TextInputCallback;                                   
	                                                                                           
	public class BGState {                                                                
	                                                                                           
		public static class Map extends Mapper<LongWritable, Text, Text, IntWritable > {    
			                                                                           
			private final static IntWritable  one = new IntWritable(1);                  
			                                                                           
			public void map(LongWritable key, Text value, Context context)             
			throws IOException, InterruptedException{                                  
				
				
				String bloodgroup;                                                     
	            String state; 
	            //int refbook;
	            String line = value.toString();                                                
	            String[] tuple = line.split("\\n");                                            
	            try{                                                                           
	                for(int i=0;i<tuple.length; i++){                                          
	                    JSONObject obj = new JSONObject(tuple[i]);
			    bloodgroup = obj.getString("blood_group");  
	                    state = obj.getString("state");
	                    //refbook = obj.getInt("Refbook");
	                    
	                    
	                    String s = (bloodgroup + "\t" + state + "\t" );
	                    
	                    Text abcd = new Text(s);
	                    //IntWritable efgh = new IntWritable(refbook);
	                    context.write(abcd,one);
	                    
	                    
	                }
					                                                   
					} catch(JSONException e) {                                         
					e.printStackTrace();                                       
				}                                                                  
				                                                                   
			}                                                                          
		}                                                                                  
		 
		public static class Reduce extends Reducer<Text, IntWritable , Text, IntWritable > { 
	        
			private IntWritable	emitValue = new IntWritable();                     
			                                                                           
			public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException{                                  
				                                                                   
	/*long sum = 0L;  
	while (values.hasNext()) {
	    sum += values.next().get();
	}*/
		            int sum = 0;                                                                       
		        for (IntWritable val : values) {                                           
		        	int currentValue = val.get();                                      
		            sum += currentValue;                                                   
		                                                                                   
		        }                                                                       
		        emitValue.set(sum);                                                        
		        context.write(key, new IntWritable(sum));                                             
			}                                                                          
		} 
		
		
		
		public static void main(String[] args) throws Exception {                          
	        Configuration conf = new Configuration();                                          
	        if (args.length != 2) {                                                            
	            System.err.println("Usage: BGState <in> <out>");                          
	            System.exit(2);                                                                
	        }                                                                                  
	                                                                                           
	                                                           
	        Job job = Job.getInstance(conf, "BGState");                                   
	                                                                                           
	        job.setJarByClass(BGState.class);                                             
	        job.setMapperClass(Map.class);                                                     
	        job.setReducerClass(Reduce.class);                                                 
	        job.setMapOutputKeyClass(Text.class);                                              
	        job.setMapOutputValueClass(IntWritable .class);                                     
	        job.setOutputKeyClass(Text.class);                                                 
	        job.setOutputValueClass(IntWritable.class);                                        
	        job.setInputFormatClass(TextInputFormat.class);                                    
	        job.setOutputFormatClass(TextOutputFormat.class);                                  
	                                                                                           
	        FileInputFormat.addInputPath(job, new Path(args[0]));                              
	        FileOutputFormat.setOutputPath(job, new Path(args[1]));                            
	                                                                                           
	        System.exit(job.waitForCompletion(true) ? 0 : 1);                                  
	    }                                                                                      
	                                                                                           
	}                                                                                          