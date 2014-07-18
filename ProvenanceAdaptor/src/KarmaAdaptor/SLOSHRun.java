package KarmaAdaptor;

import getURL.getURL;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.LoggingPermission;

import javax.security.auth.callback.ConfirmationCallback;

import org.jdom2.Element;

import KarmaClient.client;
import LogProcessor.LogParser;
import Util.ConfigManager;

import org.apache.log4j.*;
public class SLOSHRun {
	static final Logger logger = Logger.getLogger("KarmaAdaptor.SLOSHRun");
	public static String config_path=""; 
	public static String storage_path="";
	public static Map<String, String> storage_urls=new HashMap<String, String>();
	
	public static void main(String[] args)
	{
		if(args.length==2)
		{	
			config_path=args[0];
			System.out.println("Config.properties Path:"+config_path);
			storage_path=args[1];
			File config=new File(config_path);
			File storage= new File(storage_path);
			if(!config.exists())
			{
				System.out.println("Exception: config.properties not exists.");
				return;
			}
			if(!storage.exists())
			{
				System.out.println("Exception: storage.log not exists.");
				return;
			}
		}
		else
		{
			System.out.println("Usage: KARMADaemon <config.properties path> <Storage.log path>");
			return;
		}
		
		String log4JPropertyFile = ConfigManager.getProperty("Log4j_Properties");
		PropertyConfigurator.configure(log4JPropertyFile);
		storage_urls=getURL.getStrorageURL(storage_path);
		
		logger.info("SLOSH provenance framework is invoked.");
		logger.info("Looping through log central location...");
		logger.info("Loading middleware log file...");
		
		String master_log=storage_urls.get("master_log");
		logger.info("Master node log file:"+master_log);
		
		LogParser log_parser=new LogParser(master_log);
		
		log_parser.masterParse();
		log_parser.middlewareParser();
	
		String parsedLog="tmp_master_log.txt";
		
		SLOSHAdaptor karma_adaptor=new SLOSHAdaptor(parsedLog);
		
		logger.info("\n"+"SLOSH KARMA Adaptor is starting...\n");
		String notification_folder="";
		if(ConfigManager.getProperty("provenance_level").equalsIgnoreCase("Middleware"))
		{
			logger.info("\n"+"SLOSH KARMA Adaptor provenance level: Middleware");
			notification_folder=karma_adaptor.workflowInvoked();
			karma_adaptor.DistributionPhase();
		}
		else {
			logger.info("\n"+"SLOSH KARMA Adaptor provenance level: Middleware + Application");
			notification_folder=karma_adaptor.workflowInvoked();
			ArrayList<Element> outputenv_list=karma_adaptor.DistributionPhase();
			karma_adaptor.MergePhase(outputenv_list);
		}
		
		logger.info("\n"+"SLOSH KARMA Adaptor finished.\n");
		
		logger.info("\n"+"Deleting temp files...");
		File master_file = new File("tmp_master_log.txt");
		 
		/*if(master_file.delete()){
			logger.info(master_file.getName() + " is deleted!");
		}else{
			logger.error("Temp Master log Delete operation is failed.");
		}*/
		
		File middleware_file = new File("tmp_middleware_log.txt");
		 
		/*if(middleware_file.delete()){
			logger.info(middleware_file.getName() + " is deleted!");
		}else{
			logger.error("Temp Middleware log Delete operation is failed.");
		}*/
		
		logger.info("\n"+"Karma client is sending provenance notifications...");
		client karma_client=new client(notification_folder);
		karma_client.sendNotifications();
		logger.info("\n"+"Karma client finished working.");
	}
	
}
