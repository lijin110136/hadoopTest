/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.laizuozuoba;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] splits = value.toString().split("\t");
			String queryStr = splits[8];
			Map<String, String> params = getParams(queryStr);
			String pkg = params.get("pkg");
			String token = params.get("tk");
			
			//统计pkg的pv
//			if(pkg != null && !"".equals(pkg)){
//				word.set(pkg);
//				context.write(word, one);
//			}
			//统计pkg的uv
			if(pkg != null && !"".equals(pkg)){
				word.set(pkg + "\t" + token);
				context.write(word, one);
			}
		}
		
		public Map<String, String> getParams(String queryStr){
			Map<String, String> map = new HashMap<String, String>();
			String[] splits = queryStr.split("&");
			for(String pair : splits){
				String[] pairs = pair.split("=");
				if(pairs.length == 2){
					map.put(pairs[0], pairs[1]);
				}
			}
			return map;
		}
	}
	
	public static class UVMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			word.set(split[0]);
			context.write(word, one);
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable(0);

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			//统计pkg的pv
//			int sum = 0;
//			for (IntWritable val : values) {
//				sum += val.get();
//			}
//			result.set(sum);
//			context.write(key, result);
			//统计pkg的uv
			context.write(key, result);
		}
	}
	
	public static class UVReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
			private IntWritable result = new IntWritable(0);

			public void reduce(Text key, Iterable<IntWritable> values,
					Context context) throws IOException, InterruptedException {
				int sum = 0;
				for (IntWritable val : values) {
					sum += val.get();
				}
				result.set(sum);
				context.write(key, result);
			}
	}

	public static void main(String[] args) throws Exception {
		// System.setProperty("hadoop.home.dir", "D:\\hadoop-2.2.0");
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		Job job2 = new Job(conf, "uv");
		job2.setJarByClass(WordCount.class);
		job2.setMapperClass(UVMapper.class);
		job2.setCombinerClass(UVReducer.class);
		job2.setReducerClass(UVReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job2, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job2, new Path("hdfs://10.18.106.67:9100/result2"));
		
		ControlledJob controlledJob = new ControlledJob(job.getConfiguration());
		ControlledJob controlledJob2 = new ControlledJob(job2.getConfiguration());
		controlledJob2.addDependingJob(controlledJob);
		JobControl jc = new JobControl("123");
		jc.addJob(controlledJob);
		jc.addJob(controlledJob2);
		
		Thread jcThread = new Thread(jc);
		jcThread.start();  
        while(true){
            if(jc.allFinished()){
                System.out.println(jc.getSuccessfulJobList());  
                jc.stop();
                break;
            }
            if(jc.getFailedJobList().size() > 0){
                System.out.println(jc.getFailedJobList());
                jc.stop();
                break;
            }
            Thread.sleep(1000);
        }
		System.out.println("Finished!!!!!!!!!!!!!!!!!!!!!!!");
	}
}
