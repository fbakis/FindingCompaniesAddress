import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapred.machines_jsp;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.springframework.beans.TypeMismatchException;
import java.io.InputStreamReader;
import java.net.URI;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;




public class RegnumAdd {
	//Map1 is a function to get register number as a key, company name as a value.
	//input for that function is CompanyHouse data. Every step map function takes one line.
	//output for that function is register number and company name as key-value pair.
	//Line number(LongWritable) CompanyHouse(Text)--->Key(Text) Value(Text)
	public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();//to convert each line to string.
			String [] words= line.split(",");//to split each word for each line by ",".
			String compname=words[0];
			String digit="0123456789";
				try{
					for(int i=1; i<words.length; i++){
						
						if(words[i].length()==10){
							if(digit.contains(words[i].substring(3,4))){
								context.write(new Text(words[i]), new Text(compname));//return key-value pair as output
								break;
							}
						}
						compname=compname+" "+words[i];
					}
				}
				catch (NumberFormatException e) {
					// TODO: handle exception
				}
			
		}

	}
	//Key(Text) Value(Text)---->Key(Text) Value(Text)
	public static class Reduce1 extends Reducer<Text, Text, Text, Text> {
		//Reduce1 is a method which combine result of Map1.
		//input is output from Map1. Each step Reduce1 function takes one key-value pair. 
	
		public void reduce(Text key, Iterable<Text> values, Context context)//values is a list of values with same key. 
				throws IOException, InterruptedException {

			for( Text value : values){
				context.write(key, value);//return key-value pairs.

			}
		}
	}
	//(Mapper)Address of each Website(Text)  Content of each Website(Text)--->Register Number(Text) URL(Text)
	public static class Map2 extends Mapper<Text, Text, Text, Text> {
		//Map2 is a method which takes Common Crawl(Address and Content) data as a input.
		HashMap<String, String> compHouse=new HashMap<String, String>();//to store Register number(key) and company name(value) pairs respectively.
		HashMap<String, String> compHouse2=new HashMap<String, String>();//to store company name(key) register number(value) respectively.

		public void configure(JobConf job) throws IOException{

			AmazonS3 s3Client = new AmazonS3Client(new PropertiesCredentials(//to connect AmazonS3.
					RegnumAdd.class.getResourceAsStream(
							"AwsCredentials.properties")));

			S3Object s3object = s3Client.getObject(new GetObjectRequest(//to get output file of Map1-Reduce1.
					"company-regnum", "output1/part-r-00000"));

			BufferedReader b=new BufferedReader(new InputStreamReader(s3object.getObjectContent()));//to read output from file.

			try {
				while (b.readLine() != null)
				{

					String line=b.readLine(); //to read each line.
					String splitarray[] = line.split("\""); //output format of first map-reduce is SequenceFileOutputFormat.
					//So we cannot split with " " , split with "
					String compName=splitarray[splitarray.length-2].replaceAll("LIMITED|LTD|PLC", ""); // compname is in second from the end because of SequenceFileFormat.
					//Delete all punctuations and LIMITED or LTD.
					String regnum=splitarray[1];
					compHouse.put(regnum, compName);//to add register number and company name into CompHouse(HashMap), when register number is given, to get company name.
					compHouse2.put(compName, regnum);//to add register number and company name into CompHouse2(HashMap), when company name is given, to get register number.
				}

			}

			catch(NullPointerException e) {

			}	 
			catch(IndexOutOfBoundsException e) {

			}	

		}
		//URL of each website(Text) Content of each website(Text)--->Register number(Text) and URL(Text) as pair.
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

			String content = value.toString();//to convert content to string.
			String addr = key.toString();//to convert address to string.
			String cleancompanyname=""; 
			boolean complete=false;
			String [] nameword; //each cell include words of one companyname.
			ArrayList<String> possregnums=new ArrayList<String>();
			String regex ="(([A-Z]){2}([0-9]){5}([A-Z0-9]))|([0-9]{4,7})"; 
			Pattern pattern = Pattern.compile(regex); 
			Matcher matcher = pattern.matcher(content); 
			// int m=0;
			while (matcher.find()) { 
				possregnums.add(matcher.group());
				//   m=m+1;
			}

			if(addr.substring(0, 4).equals("http")){
				addr=addr.substring(7);
			}

			//To get substring of the address(domain address) of each website until "/".
			String realaddress="";
			for (int i=1; i<addr.length()+1; i++){
				if(addr.substring(i-1,i).equals("/")){
					realaddress=addr.substring(0, i-1);
					break;
				}
			}
			if(realaddress==""){
				realaddress=addr;
			}
			//System.out.println(realaddress);
			String companyname;//each company name in CompanyHouse data.
			int y=0;//increment.
			ArrayList<String> nameword2=new ArrayList<String>();//List of words of company names after eliminate unnecessary information(example: one character in name)
			String [] words = content.split(" ");//to split each word in content and add them into array.
			int increment=0;
			int index=0;
			String addrafterslash=addr.substring(realaddress.length(), addr.length());
			//System.out.println(addrafterslash);
			for(String possregnum : possregnums){

				if(compHouse.containsKey(possregnum)){//to check whether word is equal to register number or not.
					companyname=compHouse.get(possregnum);//to get company name belongs to that register number.
					cleancompanyname=companyname.replaceAll("[^a-z0-9 ]", "");
					nameword=cleancompanyname.split(" ");//to get each word of that company name.
					//below codes is to eliminate words with one character.
					for(int k=0; k<nameword.length; k++){
						if(nameword[k].length()==1){
							y=y+1;
						}
						else{

							nameword2.add(k-y, nameword[k]);//set words after eliminating.
						}
					}

					//if address before "/" includes first word or second word of name, write it.
					//to get real website of company.
					try {	
						if(realaddress.contains(nameword2.get(0))){
							context.write(new Text(possregnum), key);
							complete=true;
							break;
						}

						else if(realaddress.contains(nameword2.get(1))){
							context.write(new Text(possregnum), key);
							complete=true;
							break;
						}
					}
					catch (IndexOutOfBoundsException e) {
						// TODO: handle exception
					}
					//if address after "/" does not includes first word or second word of name or register number, write it.
					//to eliminate websites which inform some details about companies

					if(content.contains(companyname)){
						try {
							if((!(addrafterslash.contains(nameword2.get(0))))&& (!(addrafterslash.contains(possregnum)))){
								context.write(new Text(possregnum), key);
								complete=true;
								break;
							}

							else if((!(addrafterslash.contains(nameword2.get(1))))&& (!(addrafterslash.contains(possregnum)))){
								context.write(new Text(possregnum), key);
								complete=true;
								break;

							}
						}
						catch (IndexOutOfBoundsException e) {
							// TODO: handle exception
						}
}
				}
			}
			
			if(!complete){
				context.write(key, new Text("it is not company site"));
			}
		}
	}
	

	public static class Reduce2 extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {

			for( Text value : values){
				context.write(key, value);
			}
		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job1 = new Job(conf, "job1");
		Job job2 = new Job(conf, "job2");
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.setMapperClass(Map1.class);
		job1.setReducerClass(Reduce1.class);
		FileInputFormat.setInputPaths(job1, new Path(args[0]));
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		job1.setJarByClass(RegnumAdd.class);
		job1.waitForCompletion(true);
		
		job2.setInputFormatClass(SequenceFileInputFormat.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setMapperClass(Map2.class);
		job2.setReducerClass(Reduce2.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		FileInputFormat.setInputPaths(job2, new Path("s3://aws-publicdatasets/common-crawl/parse-output/segment/1341690169105/textData-00112"));
		
		String segmentListFile = "s3n://aws-publicdatasets/common-crawl/parse-output/valid_segments.txt";
		FileSystem fs = FileSystem.get(new URI(segmentListFile), conf);
		BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(segmentListFile))));
		String segmentId;
		while ((segmentId = reader.readLine()) != null) {
			String inputPath = "s3n://aws-publicdatasets/common-crawl/parse-output/segment/"+segmentId+"/textData-*";
			FileInputFormat.addInputPath(job2, new Path(inputPath));
		}

		job2.setJarByClass(RegnumAdd.class);
		job2.waitForCompletion(true);
	}
}
