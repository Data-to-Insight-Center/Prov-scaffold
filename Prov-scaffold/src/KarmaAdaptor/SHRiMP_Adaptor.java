package KarmaAdaptor;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

import javax.rmi.CORBA.Util;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.Namespace;
import org.jdom2.input.SAXBuilder;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;

import org.apache.log4j.*;

import Util.ConfigManager;
import Util.timeParser;

public class SHRiMP_Adaptor {

	public Namespace ns = Namespace.getNamespace("ns",
			"http://www.dataandsearch.org/karma/2010/08/");
	public Namespace soapenv = Namespace.getNamespace("soapenv",
			"http://schemas.xmlsoap.org/soap/envelope/");

	private String parsedLogName = null;

	private String notification_dir = ConfigManager
			.getProperty("notification_dir");
	

	private String _workflowID;
	private String _workflowNodeID = "i136.india.futuregrid.org";
	private String _userDN = ConfigManager.getProperty("userDN");
	private String _usertype = "PERSON";
	private String _email = ConfigManager.getProperty("email");
	private String _workflowinvocationTime = null;
	private String _fileownerDN = "UND";

	private String file_anno = ConfigManager.getProperty("file_annotations");

	private int timestep = 1;
	private Map<String, Element> worker_map = new HashMap<String, Element>();
	private Element input_object = null;
	// private String timeZone=ConfigManager.getProperty("timeZone");

	static final Logger logger = Logger.getLogger("KarmaAdaptor.SLOSHAdaptor");

	public SHRiMP_Adaptor(String parsedLog) {
		parsedLogName = parsedLog;
	}

	public String workflowInvoked(int mid_flag) {
		logger.info("Extracting workflow invocation provenance information...");

		FileInputStream parsed_Log = null;
		FileInputStream parsed_mid_Log = null;
		try {
			parsed_Log = new FileInputStream(new File(parsedLogName));
			parsed_mid_Log = new FileInputStream(new File(
					"tmp_middleware_log.txt"));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				parsed_Log));
		BufferedReader reader2 = new BufferedReader(new InputStreamReader(
				parsed_mid_Log));

		String line;
		String[] tokens;

		try {
			line = reader.readLine();
			tokens = line.split(" ");
			String time = tokens[5] + " " + tokens[6];
			_workflowinvocationTime = "2014-04-23T14:10:00.05-04:00";
			_workflowID = "SHRiMP_Workflow";
			notification_dir = notification_dir + _workflowID + "/";

			logger.info("Workflow ID:" + _workflowID);
			File _notification_dir = new File(notification_dir);
			if (!_notification_dir.exists()) {
				_notification_dir.mkdirs();
			} else {
				String[] _deleteFiles = _notification_dir.list();
				for (int i = 0; i < _deleteFiles.length; i++) {
					File _deleteFile = new File(_notification_dir,
							_deleteFiles[i]);
					_deleteFile.delete();
				}
			}

			line = reader2.readLine();
			tokens = line.split(" ");
		} catch (Exception e) {
			logger.error(e.toString());// TODO: handle exception
		} finally {
			try {
				reader.close();
			} catch (IOException ex) {
				// TODO Auto-generated catch block
				logger.error(ex.toString());
			}
		}

		Element userInformation = KarmaElement.userInformation(_userDN,
				_usertype, _email);
		Element workflowInformation = KarmaElement.workflowInformation(
				_workflowID, _workflowNodeID, timestep);
		KarmaNotification.workflowInvoked(userInformation, "USER",
				workflowInformation, "WORKFLOW", _workflowinvocationTime,
				notification_dir + "workflowInvoked.xml");
		logger.info("\n SHRiMP WorkflowInvoked Notification Created and Saved!"
				+ "\n");
		timestep++;

		Element masterInformation = KarmaElement.serviceInformation(
				_workflowID, _workflowNodeID, timestep, "Master Process");
		KarmaNotification.serviceInvoked(workflowInformation, "WORKFLOW",
				masterInformation, "SERVICE", _workflowinvocationTime,
				notification_dir + "masterInvoked.xml");

		timestep++;

		FileInputStream middleware_log = null;
		try {
			middleware_log = new FileInputStream(new File(
					"tmp_middleware_log.txt"));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			logger.error(e1.toString());
		}
		BufferedReader middleware_reader = new BufferedReader(
				new InputStreamReader(middleware_log));

		try {
			line = middleware_reader.readLine();
			while (line != null && line.startsWith("worker")) {
				line = line.trim();
				tokens = line.split("\t");
				ArrayList<Element> worker_annotations = SLOSHAnnotations
						.workerAnnotations(tokens[1], tokens[2], tokens[3]);

				String worker_name = tokens[0];
				String worker_invocationTime = timeParser.dateParser(tokens[tokens.length-1]);

				Element workerInformation = KarmaElement.serviceInformation(
						_workflowID, worker_name, timestep, worker_name,
						worker_annotations);
				if(mid_flag==1)
				{
					KarmaNotification.serviceInvoked(workflowInformation,
						"WORKFLOW", workerInformation, "SERVICE",
						worker_invocationTime, notification_dir + worker_name
								+ "Invoked.xml");
					timestep++;
				}

				worker_map.put(worker_name, workerInformation);
				line = middleware_reader.readLine();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.error(e.toString());
		} finally {
			try {
				middleware_reader.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				logger.error(e.toString());
			}
		}

		Element submitInformation = KarmaElement.serviceInformation(
				_workflowID, _workflowNodeID, timestep, "Submit Tasks");
		KarmaNotification.serviceInvoked(masterInformation, "SERVICE",
				submitInformation, "SERVICE", _workflowinvocationTime,
				notification_dir + "submitInvoked.xml");
		logger.info("\n SHRiMP Submit Tasks ServiceInvoked Notification Created and Saved!"
				+ "\n");
		timestep++;

		return notification_dir;
	}

	
	public ArrayList<Element> DistributionPhase(int mid_flag)
	{
		logger.info("Extracting SHRiMP Distribution phase provenance...");
		Element submitInformation = KarmaElement.serviceInformation(
				_workflowID, _workflowNodeID, timestep - 1, "Submit Tasks");

		FileInputStream parsed_Log = null;
		try {
			parsed_Log = new FileInputStream(new File(parsedLogName));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			logger.error(e1.toString());
		}
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				parsed_Log));

		ArrayList<Element> outputenv_list = new ArrayList<Element>();
		String line = null;

		int count = 1;
		try {
			line = reader.readLine();
			String[] tokens = line.split(" ");
			System.out.print(tokens[0]);
			String input_url = "2266.input.csfasta";
			Element input_file = KarmaElement.File(input_url, _fileownerDN, "2014-04-02T12:00:00.05-04:00", 0, input_url);

			input_object = KarmaElement.dataObject(input_file, null);
			
			Element splitInformation = KarmaElement.serviceInformation(
					_workflowID, _workflowNodeID, timestep,
					"splitreads.py");
		
			String consumeTimestamp = "2014-04-23T14:10:00.05-04:00";
			String produceTimestamp = "";
			
			KarmaNotification.dataRelation("PRODUCE",
					submitInformation, "SERVICE", input_object,
					_workflowinvocationTime, "File Produced",
					notification_dir + "inputcsfastaProduced.xml");
			
			KarmaNotification.dataRelation("CONSUME", splitInformation,
					"SERVICE", input_object, consumeTimestamp,
					"File Consumed", notification_dir + "inputcsfastaConsumed.xml");
			
			Element input1_file = KarmaElement.File("2266.input.1", _fileownerDN,"2014-04-02T12:00:00.05-04:00",0,"2266.input.1");

			Element input1_object = KarmaElement.dataObject(input1_file,null);
			
			KarmaNotification.dataRelation("PRODUCE", submitInformation,
					"SERVICE", input1_object, _workflowinvocationTime,
					"File Produced", notification_dir + "input1Produced.xml");
			
			Element rmapperInformation = KarmaElement.serviceInformation(
					_workflowID, _workflowNodeID, timestep,
					"./rmapper-cs");
			
			while (line != null) {
					System.out.print(line);
					tokens = line.split(" ");	

					String _serviceNodeID = tokens[4];

					Element currentWorker = worker_map.get(_serviceNodeID);
				
					String invocation_time=timeParser.dateParser(tokens[5]
								+ " " + tokens[6]);
					produceTimestamp = timeParser.dateParser(tokens[7]
							+ " " + tokens[8]);
		
					KarmaNotification.serviceInvoked(currentWorker, "SERVICE",
							rmapperInformation, "SERVICE", invocation_time,
							notification_dir + "rmapperInvoked" + count + ".xml");

					Element csfasta_file = KarmaElement.File(tokens[1], _fileownerDN,invocation_time,0, tokens[1]);

					Element csfasta_object = KarmaElement.dataObject(csfasta_file,null);
					
					Element output_file = KarmaElement.File(tokens[3], _fileownerDN,produceTimestamp,0,tokens[3]);
					
					Element output_object =KarmaElement.dataObject(output_file, null);
					
					KarmaNotification.dataRelation("PRODUCE",
							splitInformation, "SERVICE", csfasta_object,
							invocation_time, "File Produced",
							notification_dir + "csfasta"+count+"Produced.xml");
			
					KarmaNotification.dataRelation("CONSUME",
							rmapperInformation, "SERVICE", csfasta_object,
							invocation_time, "File Consumed",
							notification_dir + "csfasta"+count+"Consumed.xml");

					KarmaNotification.dataRelation("CONSUME",
							rmapperInformation, "SERVICE", input1_object,
							_workflowinvocationTime, "File Consumed",
							notification_dir + "input1Cosumed"+count+".xml");

					KarmaNotification.dataRelation("PRODUCE", rmapperInformation,
							"SERVICE", output_object, produceTimestamp,
							"File Produced", notification_dir + "output"+count+"Produced.xml");

					outputenv_list.add(output_object);
					
					count++;
					timestep++;
					line = reader.readLine();
			}
					logger.info("\nSLOSH Distribution Phase Notification Created and Saved!\n");
		} catch (Exception e) {
			logger.error(e.toString());// TODO: handle exception
		} finally {
			try {
				reader.close();
			} catch (IOException ex) {
				// TODO Auto-generated catch block
				logger.error(ex.toString());
			}
		}

		return outputenv_list;
	}
	
	public void MergePhase(ArrayList<Element> outputenv_list) {
		logger.info("Extracting BWA Merge Phase provenance...");

		Element mergeInformation = KarmaElement.serviceInformation(_workflowID,
				_workflowNodeID, timestep, "./combine.sh");

		
		int count = 1;
		for (Element outputenv : outputenv_list) {
			KarmaNotification.dataRelation("CONSUME", mergeInformation,
					"SERVICE", outputenv, "2014-04-23T15:45:00.05-04:00", "File Consumed",
					notification_dir + "merge_outputConsumed" + count + ".xml");
			count++;
		}

		Element mergeOutput_file = KarmaElement.File("2266.output", _fileownerDN, "2014-04-23T15:50:05.15-04:00",0,"2266.output");
		Element mergeOutput_object = KarmaElement.dataObject(mergeOutput_file,
				null);
		
		KarmaNotification.dataRelation("PRODUCE", mergeInformation,
					"SERVICE", mergeOutput_object, "2014-04-23T15:50:05.15-04:00", "File Produced",
					notification_dir + "merge_outputProduced.xml");
		
		timestep++;

		logger.info("\nSHRiMP Concatenation Phase Notification Created and Saved!\n");
	}
}