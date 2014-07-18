package KarmaAdaptor;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Queue;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.Namespace;
import org.jdom2.input.SAXBuilder;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;

import Util.ConfigManager;

public class SLOSHAnnotations {
	
	private InputStream input_file=null;
	private Namespace ns = Namespace.getNamespace("ns","http://www.dataandsearch.org/karma/2010/08/");
	private Namespace soapenv = Namespace.getNamespace("soapenv","http://schemas.xmlsoap.org/soap/envelope/");
	private String file_url;

	public SLOSHAnnotations(String file_url)
	{
		this.file_url=file_url.replace("file://","");
		URL url=null;
		try {
			url = new URL(file_url);
			try {
				input_file=url.openStream();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public SLOSHAnnotations()
	{
		
	}
	
	public ArrayList<Element> trkAnnotations()
	{
		BufferedReader reader = new BufferedReader(new InputStreamReader(input_file));
		
		String IBGNT=null;
		String ITEND=null;
		String JHR=null;
		String TIME=null;
		String SEA_LAKE_DATUM=null;

		FileDetection detector = new FileDetection(file_url);
               
                ArrayList<String> diagnose_log = detector.corruption();
                if(detector.result==1)
                {
			String line=null;
			try {
				line = reader.readLine();
				while(line!=null)
				{
				line=line.trim();
				String[] tokens=null;
				
				if(line.contains("IBGNT ITEND JHR"))
				{
					tokens=line.split(" ");
					IBGNT=tokens[0];
					ITEND=tokens[1];
					JHR=tokens[2];
				}
				if(line.contains("TIME"))
				{
					tokens=line.split(" ");
					TIME=tokens[0]+" "+tokens[1]+" "+tokens[2]+" "+tokens[3];
				}
				if(line.contains("SEA AND LAKE DATUM"))
				{
					tokens=line.split("\\s+");
					SEA_LAKE_DATUM=tokens[0]+" "+tokens[1];
				}
				line=reader.readLine();
				}
			} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			}
			/*
			System.out.println(IBGNT);
			System.out.println(ITEND);
			System.out.println(JHR);
			System.out.println(TIME);
			System.out.println(SEA_LAKE_DATUM);
			*/
			ArrayList<Element> trk_result=new ArrayList<Element>();
		
			Element _IBGNT=KarmaElement.annotations("IBGNT",IBGNT);
			Element _ITEND=KarmaElement.annotations("ITEND",ITEND);
			Element _JHR=KarmaElement.annotations("JHR",JHR);
			Element _TIME=KarmaElement.annotations("Model Run GMT",TIME);
			Element _DATUM=KarmaElement.annotations("Sea And Lake Datum",SEA_LAKE_DATUM);
		
			trk_result.add(_IBGNT);
			trk_result.add(_ITEND);
			trk_result.add(_JHR);
			trk_result.add(_TIME);
			trk_result.add(_DATUM);
		
			return trk_result;
		}
		else
		{
			ArrayList<Element> trk_result=new ArrayList<Element>();
			
			int count=1;
			for(String line: diagnose_log)
                        {
                               Element diagnose=KarmaElement.annotations("Diagnose"+count,line);
			       trk_result.add(diagnose);
			       count++;
                        }
			return trk_result;
		}	
	}
	
	public ArrayList<Element> bsnAnnotations()
	{
		BufferedReader reader = new BufferedReader(new InputStreamReader(input_file));
		
		String BASIN_NAME=null;
		String center_location=null;
		String branch_location=null;
		
		String line=null;
		try {
			line = reader.readLine();
			while(line!=null)
			{
				line=line.trim();
				String[] tokens=null;
				
				if(line.contains("BASIN NAME"))
				{
					tokens=line.split(" ");
					BASIN_NAME=tokens[0]+" "+tokens[1];
				}
				if(line.contains("LAT/LONG OF CENTER PT (TLAT,TLON)"))
				{
					tokens=line.split("\\s+");
					center_location=tokens[0]+" "+tokens[1];
				}
				if(line.contains("LAT/LONG OF BRANCH PT (ALTO,ALNO)"))
				{
					tokens=line.split("\\s+");
					branch_location=tokens[0]+" "+tokens[1];
				}
				line=reader.readLine();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		/*
		System.out.println(BASIN_NAME);
		System.out.println(center_location);
		System.out.println(branch_location);
		*/
		
		ArrayList<Element> bsn_result=new ArrayList<Element>();
		
		Element _name=KarmaElement.annotations("BASIN NAME",BASIN_NAME);
		Element _center=KarmaElement.annotations("LAT/LONG OF CENTER PT (TLAT,TLON)",center_location);
		Element _branch=KarmaElement.annotations("LAT/LONG OF BRANCH PT (ALTO,ALNO)",branch_location);
		
		bsn_result.add(_name);
		bsn_result.add(_center);
		bsn_result.add(_branch);

		return bsn_result;
	}
	
	public ArrayList<Element> inputenvAnnotations(String _simulationInterval, String _executionTime)
	{
		ArrayList<Element> inputenv_result=new ArrayList<Element>();
		
		Element simulation=KarmaElement.annotations("Simulation Interval", _simulationInterval);
		Element execution=KarmaElement.annotations("Execution Time", _executionTime);
		
		inputenv_result.add(simulation);
		inputenv_result.add(execution);
		
		return inputenv_result;
	}
	
	public ArrayList<Element> inputrexAnnotations(String _simulationInterval, String _executionTime)
	{
		ArrayList<Element> inputrex_result=new ArrayList<Element>();
		
		Element simulation=KarmaElement.annotations("Simulation Interval", _simulationInterval);
		Element execution=KarmaElement.annotations("Execution Time", _executionTime);
		
		inputrex_result.add(simulation);
		inputrex_result.add(execution);
		
		return inputrex_result;
	}
	
	public static ArrayList<Element> outputenvAnnotations(String _CAT, String _direction, String _speed, String _datums)
	{
		ArrayList<Element> outputenv_result=new ArrayList<Element>();
		
		Element CAT=KarmaElement.annotations("Storm Category", _CAT);
		Element direction=KarmaElement.annotations("Storm Direction", _direction);
		Element speed=KarmaElement.annotations("Forward Speed", _speed);
		Element datums=KarmaElement.annotations("Datums", _datums);
		
		outputenv_result.add(CAT);
		outputenv_result.add(direction);
		outputenv_result.add(speed);
		outputenv_result.add(datums);
		
		return outputenv_result;
	}
	
	public static ArrayList<Element> workerAnnotations(String _version, String _system, String _host,String _endTime )
	{
		ArrayList<Element> worker_result=new ArrayList<Element>();
		
		Element version=KarmaElement.annotations("Worker Version", _version);
		Element system=KarmaElement.annotations("System Information", _system);
		Element host=KarmaElement.annotations("Host Information", _host);
		Element endTime=KarmaElement.annotations("End Time", _endTime);
		
		worker_result.add(version);
		worker_result.add(system);
		worker_result.add(host);
		worker_result.add(endTime);
		
		return worker_result;
	}
	
	public static ArrayList<Element> failureAnnotations(String _failure)
	{
		ArrayList<Element> failure_result=new ArrayList<Element>();
		
		Element failure=KarmaElement.annotations("failure_message", _failure);
		
		failure_result.add(failure);
		
		return failure_result;
	}
	/*
	public static void main(String[] args)
	{
		new dataAnnotations("http://bitternut.cs.indiana.edu:12991/dta/hmi3dta").bsnAnnotations();
	}*/
}
