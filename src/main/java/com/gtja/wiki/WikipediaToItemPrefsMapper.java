package com.gtja.wiki;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.VarLongWritable;
import java.io.IOException;
import java.util.regex.Pattern;

public final class WikipediaToItemPrefsMapper extends
        Mapper<LongWritable, Text, VarLongWritable, VarLongWritable> {
    private static final Pattern NUMBERS = Pattern.compile("(\\d+)");

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, VarLongWritable, VarLongWritable>.Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(" ");
        for(int i =1; i< words.length; i++ ){
            context.write(new VarLongWritable
                    (Long.parseLong(words[0])),new VarLongWritable(Long.parseLong(words[i])));
        }
        /*Matcher m = NUMBERS.matcher(line);
        m.find();
        VarLongWritable userID = new VarLongWritable(Long.parseLong(m.group()));
        VarLongWritable itemID = new VarLongWritable();
        while (m.find()) {
            itemID.set(Long.parseLong(m.group()));
            context.write(userID, itemID);
        }*/
    }
}
