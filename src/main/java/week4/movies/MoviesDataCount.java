/*
 * Copyright (c) 2023, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package week4.movies;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import week4.count.CountNumUsersByState;

import java.io.IOException;
import java.util.ArrayList;

import static week4.CSVUtils.ReadCSVFile;

public class MoviesDataCount {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: CountNumUsersByState <users> <out>");
            System.exit(2);
        }

        Path input = new Path(otherArgs[0]);
        Path outputDir = new Path(otherArgs[1]);

        Job job = Job.getInstance(conf, "Count Num Users By State");
        job.setJarByClass(CountNumUsersByState.class);

        job.setMapperClass(CountNumUsersByState.CountNumUsersByStateMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, outputDir);

        int code = job.waitForCompletion(true) ? 0 : 1;

        if (code == 0) {
            for (Counter counter : job.getCounters().getGroup(
                    CountNumUsersByState.CountNumUsersByStateMapper.STATE_COUNTER_GROUP)) {
                System.out.println(counter.getDisplayName() + "\t"
                        + counter.getValue());
            }
        }

        // Clean up empty output directory
        FileSystem.get(conf).delete(outputDir, true);

        System.exit(code);
    }

    public static class CountNumUsersByStateMapper extends
            Mapper<Object, Text, NullWritable, NullWritable> {

        public static final String MOVIES = "Movies";


        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {


            ArrayList<String[]> movies = ReadCSVFile("");

            int i = 0;
            int popularityCount = 0;
            int voteAVGCount = 0;
            int voteCount = 0;

            try {
                for (String[] movie : movies) {

                    String popularity = movie[5];
                    if (i != 0 && popularity != null && !popularity.isEmpty() && !popularity.equals(" ")) {
                        double d = Double.parseDouble(popularity);
                        if (d > 500) {
                            popularityCount++;
                            //System.out.println("popularity> 500 : " + popularityCount);
                            context.getCounter(MOVIES, "Popularity Over 500.0 :")
                                    .increment(1);
                        }
                    }

                    String vote_avg = movie[7];
                    if (i != 0 && vote_avg != null && !vote_avg.isEmpty() && !vote_avg.equals(" ")) {
                        double d = Double.parseDouble(vote_avg);
                        if (d > 8.0) {
                            voteAVGCount++;
                            //System.out.println("vote_avg > 8.0 : " + voteAVGCount);
                            context.getCounter(MOVIES, "Vote Average Over 8.0 :")
                                    .increment(1);
                        }
                    }

                    String vote_count = movie[8];
                    if (i != 0 && vote_count != null && !vote_count.isEmpty() && !vote_count.equals(" ")) {
                        double d = Double.parseDouble(vote_count);
                        if (d > 10000) {
                            voteCount++;
                            //System.out.println("voteCount > 10000 : " + vote_count);
                            context.getCounter(MOVIES, "Vote Count :")
                                    .increment(1);
                        }
                    }
                    i++;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
