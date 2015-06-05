/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package EventProcessor;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONObject;

/**
 *
 * @author Donga
 */
public class AssetMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable l, Text line, Context context) {
        try {
            String s = line.toString();
            
            Pattern p = Pattern.compile(",.*\\},.(\\{.*\\})");
            Matcher m = p.matcher(s);
            String json = "{}";
            if (m.find()) {
                json = m.group(1);
            }
            JSONObject js = new JSONObject(json);
            context.write(new Text(js.getString("pv")), new Text("asset"));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
