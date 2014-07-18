Prov-scaffold
=============

Middleware-centric Multi-layer Provenance Framework

Prov-scaffold is a useful tool to capture provenance from systems with multiple software stacks and distributed knowledge. Prov-scaffold requires no application instrumentation and minimal system intrusion with provenance collection. It uses a novel correlation inference algorithm to correlate provenance events from multiple system layers.

Following is a quick start guide for Prov-scaffold. 

User Guide
=============

Software Dependencies
--------------------------------

1) Java Development Kit(JDK) V5 or later
Available at: http://java.sun.com 

2) KARMA Messaging Client Version 3.2.3 (for RabbitMQ karma Service Core configuration)
Available at: http://sourceforge.net/projects/karmatool/files/v3.2.3/karma-v3.2.3-messaging-client-core.tar.gz/download

3) Apache Ant V1.6 or later
Available at: http://ant.apache.org/

4) Cooperating Computing Tools (cctools)
Available at: http://www3.nd.edu/~ccl/software/download.shtml

5) Python 2.7.6
Available at: http://www.python.org/download/

Installing and Configuring Prov-scaffold
----------------------------------------------------

1) Edit the build.xml file:

	vi Prov-scaffold/build.xml
	
Please edit value ProvenanceRepo.dir to your local provenance repository path;

2) Build Prov-scaffold:

	ant Prov-scaffold/build


3) Edit Prov-scaffold Configuration File:

	vi Prov-scaffold/config/config.property
	
config.property is used to configure Prov-scaffold system. It specifies controlling parameters such as provenance granularity and layers. General information can also be specified such as time zone, etc..

	//Adaptor_control
	file_annotations={On,Off}
	provenance_level={Application, Middleware, Application+Middleware}
	
	//User_info
	userDN={Your User Name}
	email={Your Email Address}
	timeZone={Your Time Zone}
4) Edit provenance repository configuration file

	vi Prov-scaffold/config/ProvenanceRepo.propertie
	
This configures the connection between Prov-scaffold with your local provenance repository client. 

5) Edit Shell-script

	vi ./bin/Prov-scaffold-Run.sh
	
This shell script is used to invoke Prov-scaffold to collect and process  provenance information from target systems. Please edit the following lines to set up runtime environment.
	JAVA_HOME Your local JAVA home path
	Prov-scaffold_HOME Your local Prov-scaffold path
	Provenance_Repo_HOME Your local provenance repository client Path

Notice: This Shell-script requires input parameters as url of target system logs and configuration file of provenance repository, which are used to collect system logs and connect to a specified provenance repository.
	Prov-scaffold-Run.sh provenance_repo_properties_file System_log

Test case: SLOSH Scientific Workflow
-------------------------------------

The Sea, Lake and Overland Surges from Hurricanes (SLOSH) model is a computerized numerical model developed by the National Weather Service (NWS) to estimate storm surge heights resulting from historical, hypothetical, or predicted hurricanes by taking into account the atmospheric pressure, size, forward speed, and track data. These parameters are used to create a model of the wind field, which drives the storm surge.
Work Queue is a framework for building large master-worker applications that span many computers including clusters, clouds, and grids. Based on the Work Queue middleware, we developed SLOSH on Work Queue framework to execute SLOSH scientific workflows in a highly distributed manner, which can handle massive scientific data input and multiple workflows.
Prov-scaffold is used here to capture provenance information from both SLOSH and WorkQueue software stacks and process provenance events by correlation inference function. It can be used by researchers who need to create a global view of their experiments

Installing and Configuring SLOSH

1) Edit config/slosh.ini file:

	vi Applications/SLOSH/config/slosh.ini
	
config/slosh.ini is used to configure the parameters about SLOSH  such as input data and output data path, log path, etc, which will be passed to Prov-scaffold for capturing. More details can be found at docs/Prov-scaffold-UserGuide.pdf.

2) Edit slosh.makeflow file

	vi Applications/SLOSH/slosh.makeflow
	
We use slosh.makeflow as a wrapper script to automatically run SLOSH workflow and Prov-scaffold. More details can be found at docs/Prov-scaffold-UserGuide.pdf.

3) Edit ProvenanceRepo.properties:

	vi Prov-scaffold/config/ProvenanceRepo.propertie
	
For this test case, we use Karma Provenance Toolset. Configuration parameters are listed below:

	\\Karma Provenance Repository Configuration
	messaging.username={Your messaging bus username}
	messaging.password={Your messaging bus password}
	messaging.hostname={Your host name}
	messaging.hostport={Your host port #}
	messaging.virtualhost={If you use a virtual host}
	messaging.exchangename={Your messaging bus exchange name}
	messaging.queuename={Your messaging bus queue name}
	messaging.routingkey={Your messaging bus routing key}

4) Run SLOSH Workflow with Prov-scaffold 

	makflow slosh.makeflow


5) Viewing the Resulting Provenance Graph
To view a provenance graph enhanced with annotations from the SLOSH data, you can use the Karma Provenance Retrieval and Visualization Plug-ins (available at: http://pti.iu.edu/d2i/provenance_karma). Instructions for installing the latest version of the Karma plug-ins for the Cytoscape visualization tool are also available at: http://pti.iu.edu/d2i/provenance_karma.




