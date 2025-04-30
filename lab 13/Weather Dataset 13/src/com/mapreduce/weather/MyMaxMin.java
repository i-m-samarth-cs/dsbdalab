package com.mapreduce.weather;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;

public class MyMaxMin {

    public static class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, Text> {
        
        public static final int MISSING = 9999;
        
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            
            // Skip empty lines
            if (line.isEmpty()) {
                return;
            }
            
            try {
                // Write debugging information
                context.write(new Text("DEBUG-LineLength"), new Text(String.valueOf(line.length())));
                
                // Only process lines that are long enough
                if (line.length() < 53) {
                    context.write(new Text("DEBUG-ShortLine"), new Text(line));
                    return;
                }
                
                // Extract what should be the date and temperatures
                // Store these for debugging regardless of whether they can be parsed
                String dateStr = line.substring(Math.min(6, line.length()), 
                                              Math.min(14, line.length()));
                String maxTempStr = line.substring(Math.min(39, line.length()), 
                                                 Math.min(45, line.length())).trim();
                String minTempStr = line.substring(Math.min(47, line.length()), 
                                                 Math.min(53, line.length())).trim();
                
                // Output all extracted substrings for debugging
                context.write(new Text("DEBUG-Date"), new Text(dateStr));
                context.write(new Text("DEBUG-MaxTempStr"), new Text(maxTempStr));
                context.write(new Text("DEBUG-MinTempStr"), new Text(minTempStr));
                
                // Try to parse the temperatures - only proceed if both can be parsed
                float maxTemp;
                float minTemp;
                
                try {
                    // Try to handle common formatting issues
                    maxTempStr = maxTempStr.replace("FM-", "").replace("FM", "");
                    minTempStr = minTempStr.replace("FM-", "").replace("FM", "");
                    
                    maxTemp = parseTemperature(maxTempStr);
                    minTemp = parseTemperature(minTempStr);
                    
                    // Output the parsed values for debugging
                    context.write(new Text("DEBUG-ParsedMaxTemp"), new Text(String.valueOf(maxTemp)));
                    context.write(new Text("DEBUG-ParsedMinTemp"), new Text(String.valueOf(minTemp)));
                    
                    // Now output the actual results
                    if (maxTemp > 30.0) {
                        context.write(new Text("The Day is Hot Day :" + dateStr), 
                                     new Text(String.valueOf(maxTemp)));
                    }
                    
                    if (minTemp < 15.0) {
                        context.write(new Text("The Day is Cold Day :" + dateStr), 
                                     new Text(String.valueOf(minTemp)));
                    }
                    
                } catch (NumberFormatException e) {
                    context.write(new Text("ERROR-Parsing"), 
                                 new Text(e.getMessage() + " - Max: " + maxTempStr + ", Min: " + minTempStr));
                }
                
            } catch (Exception e) {
                // Catch-all for any other errors
                context.write(new Text("ERROR-General"), new Text(e.getClass().getName() + ": " + e.getMessage()));
            }
        }
        
        private float parseTemperature(String tempStr) {
            // Remove any non-digit characters except decimal point and minus sign
            StringBuilder sb = new StringBuilder();
            boolean hasDecimal = false;
            boolean hasSign = false;
            
            for (char c : tempStr.toCharArray()) {
                if (Character.isDigit(c)) {
                    sb.append(c);
                } else if (c == '.' && !hasDecimal) {
                    sb.append(c);
                    hasDecimal = true;
                } else if ((c == '-' || c == '+') && sb.length() == 0) {
                    sb.append(c);
                    hasSign = true;
                }
            }
            
            if (sb.length() == 0) {
                return 0.0f;  // Default value
            }
            
            float temp = Float.parseFloat(sb.toString());
            
            // Most weather data uses tenths of degrees
            if (Math.abs(temp) > 100) {
                temp = temp / 10.0f;
            }
            
            return temp;
        }
    }
    
    public static class MaxTemperatureReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterator<Text> values, Context context) throws IOException, InterruptedException {
            // For regular results, just output the first value
            // For DEBUG lines, collect all values
            if (key.toString().startsWith("DEBUG-") || key.toString().startsWith("ERROR-")) {
                StringBuilder sb = new StringBuilder();
                while (values.hasNext()) {
                    if (sb.length() > 0) {
                        sb.append(", ");
                    }
                    sb.append(values.next().toString());
                }
                context.write(key, new Text(sb.toString()));
            } else {
                // For regular results (hot/cold days), just take the first value
                if (values.hasNext()) {
                    context.write(key, new Text(values.next().toString()));
                }
            }
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "weather example flexible");
        job.setJarByClass(MyMaxMin.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(MaxTemperatureMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        Path outputPath = new Path(args[1]);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}