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
import LogProcessor.SHRiMP_LogParser;
import LogProcessor.bwa_LogParser;
import Util.ConfigManager;

import org.apache.log4j.*;
public class SHRiMP_Run {
	static final Logger logger = Logger.getLogger("KarmaAdaptor.BWARun");
	public static String config_path=""; 
	
	public static void main(String[] args)
	{
		/*if(args.length==1)
		{	
			config_path=args[0];
			System.out.println("Config.properties Path:"+config_path);
			File config=new File(config_path);
		}
		else
		{
			System.out.println("Usage: KARMADaemon <config.properties path>");
			return;
		}*/
		
		config_path="config/config.properties";
		
		String log4JPropertyFile = ConfigManager.getProperty("Log4j_Properties");
		PropertyConfigurator.configure(log4JPropertyFile);
		
		logger.info("SHRiMP provenance framework is invoked.");
		logger.info("Looping through log central location...");
		logger.info("Loading middleware log file...");
		
		/*SHRiMP_LogParser log_parser=new SHRiMP_LogParser("/Users/Gabriel/BioWorkflow/logs/mylog.log");
		
		log_parser.workerParser();
		log_parser.middlewareParser();*/
	
		String parsedLog="tmp_master_log.txt";
		
		SHRiMP_Adaptor karma_adaptor=new SHRiMP_Adaptor(parsedLog);
		
		logger.info("\n"+"SHRiMP KARMA Adaptor is starting...\n");
		String notification_folder="";
		/*if(ConfigManager.getProperty("provenance_level").equalsIgnoreCase("Middleware"))
		{
			logger.info("\n"+"SLOSH KARMA Adaptor provenance level: Middleware");
			notification_folder=karma_adaptor.workflowInvoked();
			karma_adaptor.MiddlewarePhase();
		}
		else {
			logger.info("\n"+"SLOSH KARMA Adaptor provenance level: Middleware + Application");
			notification_folder=karma_adaptor.workflowInvoked();
			ArrayList<Element> outputenv_list=karma_adaptor.DistributionPhase();
			karma_adaptor.MergePhase(outputenv_list);
		}*/
		int mid_flag=1;
		
		logger.info("\n"+"SHRiMP KARMA Adaptor provenance level: Middleware + Application");
		notification_folder=karma_adaptor.workflowInvoked(mid_flag);	
		ArrayList<Element> outputenv_list=karma_adaptor.DistributionPhase(mid_flag);
		karma_adaptor.MergePhase(outputenv_list);
		
		logger.info("\n"+"SHRiMP KARMA Adaptor finished.\n");
		
		logger.info("\n"+"Deleting temp files...");
		File master_file = new File("tmp_master_log.txt");
		 
		/*if(master_file.delete()){
			logger.info(master_file.getName() + " is deleted!");
		}else{
			logger.error("Temp Master log Delete operation is failed.");
		}
		
		File middleware_file = new File("tmp_middleware_log.txt");
		 
		if(middleware_file.delete()){
			logger.info(middleware_file.getName() + " is deleted!");
		}else{
			logger.error("Temp Middleware log Delete operation is failed.");
		}
		
		logger.info("\n"+"Karma client is sending provenance notifications...");*/
		//client karma_client=new client(notification_folder);
		//karma_client.sendNotifications();
		logger.info("\n"+"Karma client finished working.");
	}
	
}
