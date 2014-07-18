package KarmaAdaptor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.xmlbeans.impl.piccolo.xml.FastNamespaceSupport;

public class FileDetection {
	private String file_uri;
	public int result;
	
	public static void main(String[] args)
	{
		FileDetection detector = new FileDetection("/Users/Gabriel/test.trk");
		if(detector.misplacement())
		{
			ArrayList<String> diagnose_log = detector.corruption();
			if(detector.result==1)
			{
				
			}
			else {
				for(String line: diagnose_log)
				{
					System.out.println(line);
				}
			}
			
		}
		else {
			System.out.print("File not exist.");
		}
	}
	
	public FileDetection(String file_uri)
	{
		this.file_uri=file_uri;
	}
	
	public boolean misplacement(){
		Boolean result=null;
		
		File file=new File(this.file_uri);
		if(file.exists())
		{
			result=true;
			
		}
		else 
		{
			result=false;
		
		}
		return result;
	}

	public ArrayList<String> corruption()
	{
		int result=1;
		ArrayList<String> diagnose_log=new ArrayList<String>();
		File file=new File(this.file_uri);
		BufferedReader reader=null;
		try {
			reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
			int line_count=0;
			int data_count=0;
			int footer1_flag=0;
			int footer2_flag=0;
			int footer3_flag=0;
			try {
				String line =reader.readLine();
				while(line!=null)
				{					
					//Header check
					if(line_count<2)
					{
						line_count++;
					}
					else {
						//Check if emply line
						if(line.trim().length()==0);
						//Footer line 1
						else if(line.contains("IBGNT"))
						{
							footer1_flag=1;
							line=line.replaceAll("\\s+", " ");
							String[] tokens =line.trim().split(" ");
							if(tokens.length!=6)
							{
								result=0;
								diagnose_log.add(line+": Missing parameters.");
							}
							else {
								for(int i=0;i<3;i++)
								{
									if(!tokens[i].matches("[0-9]+"))
									{
										result= 0;
										diagnose_log.add(line+": Incorrect format."+tokens[i]);
										break;
									}
								}
								if(!tokens[3].equalsIgnoreCase("IBGNT")||!tokens[4].equalsIgnoreCase("ITEND")
										||!tokens[5].equalsIgnoreCase("JHR"))
								{
									result=0;
									diagnose_log.add(line+": Incorrect format.");
								}
							}
						}
						
						//Footer line 2
						else if(line.contains("TIME"))
						{
							footer2_flag=1;
							line=line.replaceAll("\\s+", " ");
							String[] tokens=line.trim().split(" ");
							String model_run_gmt=tokens[0].replace("HR","")+" "+tokens[1]+" "+tokens[2]+" "+tokens[3];
							DateFormat df = new SimpleDateFormat("HHmm dd MMM yyyy");
							try {
								Date d=df.parse(model_run_gmt);
							} catch (Exception e) {
								// TODO: handle exception
								result=0;
								diagnose_log.add(line+": Incorrect format.");
							}		
						}
						
						//Footer line 3
						else if(line.contains("DATUM"))
						{
							footer3_flag=1;
						}
						//Data Section 100 points - 8 columns
						else
						{
							//No NAP
							if (!line.contains("NAP")) {
								line=line.replaceAll("\\s+", " ");
								line=line.trim();
								String[] tokens=line.split(" ");
								if(tokens.length!=8)
								{
									result=0;
									diagnose_log.add(line+":"+"Missing parameter.");
								}
								else {
									data_count++;
								}
							}
							// NAP line
							else {
								line=line.replaceAll("\\s+", " ");
								line=line.trim();
								String[] tokens=line.split(" ");
								if(tokens.length!=10 || !tokens[0].contains("NAP")
										|| !tokens[tokens.length-1].contains("NAP"))
								{
									result=0;
									diagnose_log.add(line+":"+"Incorrect format.");
								}
								else {
									data_count++;
								}
							}
						}
					}
					line=reader.readLine();
				}
				
				if(footer1_flag!=1 || footer2_flag!=1||footer3_flag!=1)
				{
					result=0;
					diagnose_log.add("Incomplete footer.");
				}
				
				if(data_count!=100)
				{
					result=0;
					diagnose_log.add("Not sufficient data points:"+data_count+";100 points expected.");
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if(result==1)
		{
			diagnose_log.add("Complete file: No corruption detected.");
		}
		this.result=result;
		return diagnose_log;
	}
}

