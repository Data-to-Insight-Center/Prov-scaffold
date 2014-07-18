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

public class BWA_Adaptor {

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
	private Element input1_object = null;
	private Element input2_object = null;
	private Element input0_object = null;
	// private String timeZone=ConfigManager.getProperty("timeZone");

	static final Logger logger = Logger.getLogger("KarmaAdaptor.SLOSHAdaptor");

	public BWA_Adaptor(String parsedLog) {
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
			_workflowinvocationTime = "2014-03-02T12:53:00.05-04:00";
			_workflowID = "BWA_Workflow";
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
		logger.info("\n BWA WorkflowInvoked Notification Created and Saved!"
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
		logger.info("\n BWA Submit Tasks ServiceInvoked Notification Created and Saved!"
				+ "\n");
		timestep++;

		return notification_dir;
	}

	
	public Map<String,Element> DistributionPhase(int mid_flag)
	{
		logger.info("Extracting BWA Distribution phase provenance...");
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

		Map<String, Element> outputenv_list = new HashMap<String, Element>();
		String line = null;

		int count = 1;
		try {
			line = reader.readLine();
			String[] tokens = line.split(" ");
			System.out.print(tokens[0]);
			String input1_url = tokens[0];
			Element input1_file = KarmaElement.File(input1_url, _fileownerDN, "2014-03-02T12:00:00.05-04:00", 0, input1_url);

			input1_object = KarmaElement.dataObject(input1_file, null);
			
			Element input2_file = KarmaElement.File("input.2", _fileownerDN, "2014-03-02T12:00:00.05-04:00", 0, "input.2");

			input2_object = KarmaElement.dataObject(input2_file, null);
			
			Element input0_file = KarmaElement.File("input.0", _fileownerDN, "2014-03-02T12:00:00.05-04:00", 0, "input.0");

			input0_object = KarmaElement.dataObject(input0_file, null);
			
			Element params_file = KarmaElement.File("job.params", _fileownerDN, "2014-03-02T12:00:00.05-04:00", 0, "job.params");

			Element params_object = KarmaElement.dataObject(params_file, null);
			
			Element bwa1Information = KarmaElement.serviceInformation(
					_workflowID, _workflowNodeID, timestep,
					"1st distribution BWA Application");
		
			String consumeTimestamp = timeParser.dateParser(tokens[4]
					+ " " + tokens[5]);
			String produceTimestamp = "";
			
			while (line != null && !line.startsWith("2nd distribution Information")) {

				tokens = line.split(" ");	

				String _serviceNodeID = tokens[3];

				Element currentWorker = worker_map.get(_serviceNodeID);
				
				//Task Success
				if (!tokens[tokens.length - 1].contains("node")) {
					String invocation_time=timeParser.dateParser(tokens[4]
							+ " " + tokens[5]);
					produceTimestamp = timeParser.dateParser(tokens[6]
							+ " " + tokens[7]);
					if(mid_flag==1)
					{
						KarmaNotification.serviceInvoked(currentWorker, "SERVICE",
							bwa1Information, "SERVICE", invocation_time,
							notification_dir + "bwa1Invoked" + count + ".xml");
					}

					count++;
					line = reader.readLine();
				} 
			}
			
			Element output_file = KarmaElement.File("aln.sai", _fileownerDN,produceTimestamp,0,"aln.sai");

			Element output_object = KarmaElement.dataObject(output_file,null);

			Element output2_file = KarmaElement.File("2aln.sai", _fileownerDN,produceTimestamp,0,"2aln.sai");

			Element output2_object = KarmaElement.dataObject(output2_file,null);
			
			KarmaNotification.dataRelation("PRODUCE", submitInformation,
					"SERVICE", input1_object, _workflowinvocationTime,
					"File Produced", notification_dir + "input1Produced.xml");
			
			KarmaNotification.dataRelation("PRODUCE",
					submitInformation, "SERVICE", input0_object,
					_workflowinvocationTime, "File Produced",
					notification_dir + "input0Produced.xml");
			
			KarmaNotification.dataRelation("PRODUCE",
					submitInformation, "SERVICE", input2_object,
					_workflowinvocationTime, "File Produced",
					notification_dir + "input2Produced.xml");

			KarmaNotification.dataRelation("PRODUCE",
					submitInformation, "SERVICE", params_object,
					_workflowinvocationTime, "File Produced",
					notification_dir + "paramsProduced.xml");
			
			KarmaNotification.dataRelation("CONSUME", bwa1Information,
					"SERVICE", input0_object, consumeTimestamp,
					"File Consumed", notification_dir + "input0Consumed.xml");
			KarmaNotification.dataRelation("CONSUME", bwa1Information,
					"SERVICE", input1_object, consumeTimestamp,
					"File Consumed", notification_dir + "input1Consumed.xml");
			KarmaNotification.dataRelation("CONSUME", bwa1Information,
					"SERVICE", input2_object, consumeTimestamp,
					"File Consumed", notification_dir + "input2Consumed.xml");
			KarmaNotification.dataRelation("CONSUME", bwa1Information,
					"SERVICE", params_object, consumeTimestamp,
					"File Consumed", notification_dir + "paramsConsumed.xml");
			
			KarmaNotification.dataRelation("PRODUCE", bwa1Information,
					"SERVICE", output_object, produceTimestamp,
					"File Produced", notification_dir + "alnProduced.xml");
			KarmaNotification.dataRelation("PRODUCE", bwa1Information,
					"SERVICE", output2_object, produceTimestamp,
					"File Produced", notification_dir + "aln2Produced.xml");

			outputenv_list.put("aln.sai",output_object);
			outputenv_list.put("2aln.sai",output2_object);
			
			timestep++;

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
	
	public ArrayList<Element> Distribution2Phase(Map<String, Element> outputenv_list,int mid_flag) {
		logger.info("Extracting 2nd Distribution Phase provenance...");
		FileInputStream parsed_Log = null;
		try {
			parsed_Log = new FileInputStream(new File(parsedLogName));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			logger.error(e.toString());
		}
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				parsed_Log));

		String line = null;
		String[] tokens = null;
		int count=0;
		ArrayList<Element> mergeenv_list = new ArrayList<Element>();

		try {
			line = reader.readLine();
			while (!line.startsWith("2nd distribution Information") && line != null) {
				line = reader.readLine();
			}
			line = reader.readLine();
			Element bwa2Information = KarmaElement.serviceInformation(
					_workflowID, _workflowNodeID, timestep,
					"2nd distribution BWA Application");
			tokens=line.split(" ");
			
			String consumeTimestamp=timeParser.dateParser(tokens[7]
					+ " " + tokens[8]);;
			String produceTimestamp = "";
			
		
			
			while (line != null && line.contains("input")) {
				tokens = line.split(" ");	
				System.out.println(line);
				String _serviceNodeID = tokens[6];

				Element currentWorker = worker_map.get(_serviceNodeID);
				
				//Task Success
				if (!tokens[tokens.length - 1].contains("node")) {

					String output_url = tokens[5];

					String invocation_time = timeParser.dateParser(tokens[7]
							+ " " + tokens[8]);
					produceTimestamp = timeParser.dateParser(tokens[9]
							+ " " + tokens[10]);
					
					Element output_file = KarmaElement.File(output_url, _fileownerDN,produceTimestamp,0,output_url);

					Element output_object = KarmaElement.dataObject(output_file,null);

					if(mid_flag==1)
					{
						KarmaNotification.serviceInvoked(currentWorker, "SERVICE",
							bwa2Information, "SERVICE", invocation_time,
							notification_dir + "bwa2Invoked" + count + ".xml");
					}
					
					KarmaNotification.dataRelation("PRODUCE", bwa2Information,
							"SERVICE", output_object, produceTimestamp,
							"File Produced", notification_dir + "outputProduced"
									+ count + ".xml");
					
					mergeenv_list.add(output_object);

					count++;
					line = reader.readLine();
				} 
			}
			
			Element aln_object = outputenv_list.get("aln.sai");
			Element aln2_object = outputenv_list.get("2aln.sai");
			
			KarmaNotification.dataRelation("CONSUME", bwa2Information,
					"SERVICE", input0_object, consumeTimestamp,
					"File Consumed", notification_dir + "input0ndConsumed.xml");
			KarmaNotification.dataRelation("CONSUME", bwa2Information,
					"SERVICE", input1_object, consumeTimestamp,
					"File Consumed", notification_dir + "input1ndConsumed.xml");
			KarmaNotification.dataRelation("CONSUME", bwa2Information,
					"SERVICE", input2_object, consumeTimestamp,
					"File Consumed", notification_dir + "input2ndConsumed.xml");
			KarmaNotification.dataRelation("CONSUME", bwa2Information,
					"SERVICE", aln_object, consumeTimestamp,
					"File Consumed", notification_dir + "alnndConsumed.xml");
			KarmaNotification.dataRelation("CONSUME", bwa2Information,
					"SERVICE", aln2_object, consumeTimestamp,
					"File Consumed", notification_dir + "aln2ndConsumed.xml");
			
			timestep++;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.error(e.toString());
		}
		logger.info("\nBWA 2nd Distribution Phase Notification Created and Saved!\n");
		return mergeenv_list;
	}
	
	public void MergePhase(ArrayList<Element> outputenv_list) {
		logger.info("Extracting BWA Merge Phase provenance...");

		Element mergeInformation = KarmaElement.serviceInformation(_workflowID,
				_workflowNodeID, timestep, "concat.pl");

		
		int count = 1;
		for (Element outputenv : outputenv_list) {
			KarmaNotification.dataRelation("CONSUME", mergeInformation,
					"SERVICE", outputenv, "2014-03-02T14:30:00.05-04:00", "File Consumed",
					notification_dir + "merge_outputConsumed" + count + ".xml");
			count++;
		}

		Element mergeOutput_file = KarmaElement.File("output", _fileownerDN, "2014-03-02T14:35:05.15-04:00",0,"output");
		Element mergeOutput_object = KarmaElement.dataObject(mergeOutput_file,
				null);
		
		KarmaNotification.dataRelation("PRODUCE", mergeInformation,
					"SERVICE", mergeOutput_object, "2014-03-02T14:35:05.15-04:00", "File Produced",
					notification_dir + "merge_outputProduced.xml");
		
		timestep++;

		logger.info("\nBWA Concatenation Phase Notification Created and Saved!\n");
	}
}