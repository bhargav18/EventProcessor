/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package EventProcessor;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author Donga
 */
public class Driver extends Configured implements Tool {

    public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException, Exception {
        if (args.length != 5) {
            System.err.println(" Usage:hadoop jar <...>.jar -libjars <path-to-org.json.jar> </path/to/ad_events> </path/to/assets> </output/path>");
            System.exit(-1);
        }
        int res = ToolRunner.run(new Configuration(), new Driver(), args);
        System.exit(res);

    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf);
        job.setJobName("Assets to Events");
        job.setJarByClass(Driver.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, EventMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AssetMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setReducerClass(EventsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
