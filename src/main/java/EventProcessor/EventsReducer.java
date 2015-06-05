/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package EventProcessor;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author Donga
 */
public class EventsReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        int assets = 0;
        int clicks = 0;
        int views = 0;
        
        for (Text value : values) {

            if (value.toString().equals("asset")) {
                assets++;
            } else if (value.toString().equals("view")) {
                views++;
            } else if (value.toString().equals("click")) {
                clicks++;
            }
        }
        context.write(key, new Text(" " + assets + " " + views + " " + clicks));

    }

}
