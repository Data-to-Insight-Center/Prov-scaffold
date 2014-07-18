package getURL;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import javax.sound.midi.MidiDevice.Info;
import javax.sound.sampled.Line;

import org.apache.log4j.Logger;

public class getURL {
	static final Logger logger = Logger.getLogger("getURL.getURL");
	
	public static Map<String, String> getStrorageURL(String _storage_log)
	{
		Map<String, String> result=new HashMap<String, String>();
		try{
			File storage_log = new File(_storage_log);
			FileInputStream FIS=new FileInputStream(storage_log);
			BufferedReader reader=new BufferedReader(new InputStreamReader(FIS));
			
			String line=reader.readLine();
			while(line!=null)
			{
				if(line.contains("bsn_dir"))
				{
					String[] tokens=line.split("=");
					result.put("bsn_dir",tokens[1].trim());
					logger.info("bsn_dir:"+tokens[1].trim());
				}
					
				if(line.contains("trk_dir"))
				{
					String[] tokens=line.split("=");
					result.put("trk_dir", tokens[1].trim());
					logger.info("trk_dir"+tokens[1].trim());
				}
				
				if(line.contains("output_dir"))
				{
					String[] tokens =line.split("=");
					result.put("output_dir", tokens[1].trim());
					logger.info("output_dir"+tokens[1].trim());
				}
				
				if(line.contains("merge_dir"))
				{
					String[] tokens =line.split("=");
					result.put("merge_dir", tokens[1].trim());
					logger.info("merge_dir"+tokens[1].trim());
				}
				
				if(line.contains("log_dir"))
				{
					String[] tokens =line.split("=");
					String logPath = tokens[1].trim();
					result.put("master_log", logPath+"/master.log");
					logger.info("master_log:"+logPath+"/master.log");
					result.put("worker_log",logPath+"/worker_logs");
					logger.info("worker_log:"+logPath+"/worker_logs");
					result.put("merge_log", logPath+"/merge.log");
					logger.info("merge_log"+logPath+"/merge.log");
				}
				line=reader.readLine();
			}
		}catch(Exception e)
		{
			logger.error(e.toString());
		}
		
		return result;
	}
}
