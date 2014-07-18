package Util;

import java.text.SimpleDateFormat;
import java.util.Date;

public class timeParser {
	private static String timeZone=ConfigManager.getProperty("timeZone");
	
	public timeParser()
	{
		
	}

	public static String dateParser(Date time)
	{
		String temp =new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time);
		
		String[] tokens= temp.split(" ");
		
		String result=tokens[0]+"T"+tokens[1]+timeZone;
		
		return result;
	}
	
	public static String dateParser(String time) {
		String result = null;
		String[] tokens = null;
		tokens = time.split(" ");
		tokens[0] = tokens[0].replace("/", "-");

		result = tokens[0] + "T" + tokens[1] + timeZone;

		return result;
	}
	
	public static String epochParser(Long epochtime)
	{
		Date date=new Date(epochtime*1000);
		String result=dateParser(date);
		return result;
	}
}
