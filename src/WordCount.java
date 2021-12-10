import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;

public class WordCount {

  public static class WordCountMapper extends Mapper<Object, Text, Text, Text>{	  
	  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		ArrayList<String> check = new ArrayList<String>();
		getStopWord(check);
		Text word = new Text();
		char[] trash = {',', '(', ')', '"', '[', ']', '�', ';'};
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        String daTa = value.toString();
        String[] wordSplit = daTa.split(" ");
        for (String s : wordSplit) {
	    	for (int i = 0; i < s.length(); i++) {
	    		for (char c : trash) {
	    			if (s.charAt(i) == c) {
	    				s = new String (removeCharAt(s, i)); 
	    				i--;
	    				break;
	    			}
	    		}
	    	}
	    	if (check.contains(s) == false && s.isEmpty() == false) { 
	    		word.set(s.toLowerCase().trim());
	    		context.write(word, new Text(fileName));
        	}
        }
	  }
    }

  public static class WordCountReducer extends Reducer<Text, Text, Text, Text> {
	  
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	ArrayList<String> list = new ArrayList<String>();
    	int count = 0;
    	for (Text t : values) {
    		count++;
    		String word = String.valueOf(t);
            if (list.contains(word) == false) {
                list.add(word);
            }
        } 
    	String fiNalCount = String.valueOf(count);
    	String fiNalList = list.toString();
    	context.write(new Text(key + "	" + fiNalCount), new Text(fiNalList));
		}
  }
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Word Count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(WordCountMapper.class);
    job.setReducerClass(WordCountReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    File folder = new File("C:\\hadoop-2.7.3\\input\\");
    File[] txtFiles = folder.listFiles(new TxtFileFilter());
    for (File txtFile : txtFiles) {
    	FileInputFormat.addInputPath(job, new Path(txtFile.getAbsolutePath()));
    }
    FileOutputFormat.setOutputPath(job, new Path("C:\\hadoop-2.7.3\\output"+System.currentTimeMillis()));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
  
  public static ArrayList<String> getStopWord (ArrayList<String> a) throws FileNotFoundException {
	  File myObj = new File("C:\\Users\\bevau\\eclipse-workspace\\BTL\\src\\StopWord.txt");
	  Scanner myReader = new Scanner(myObj);
	  while (myReader.hasNextLine()) {
	    a.add(myReader.nextLine());
	  }
	  myReader.close();
	  return a;
  }
  
  public static class TxtFileFilter implements FileFilter {
	@Override
	public boolean accept(File pathname) {

		if (!pathname.isFile()) {
			return false;
		}

		if (pathname.getAbsolutePath().endsWith(".txt")) {
			return true;
		}

		return false;
	}
  }
  
  public static String removeCharAt(String s, int pos) {
      return s.substring(0, pos) + s.substring(pos + 1);
   }
}