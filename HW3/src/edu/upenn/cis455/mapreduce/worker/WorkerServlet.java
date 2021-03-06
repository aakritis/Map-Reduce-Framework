package edu.upenn.cis455.mapreduce.worker;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.servlet.*;
import javax.servlet.http.*;

import edu.upenn.cis455.mapreduce.Job;
import edu.upenn.cis455.mapreduce.job.MapReduceContext;


/**
 * Worker Servlet 
 * @author Aakriti Singla
 *
 */
public class WorkerServlet extends HttpServlet {

	static final long serialVersionUID = 455555002;
	public WorkerStatusParams workerParams;
	public String masterServer;
	public String rootStorageDir;
	HashMap<String, String> maprunMapParams = new HashMap<String, String>();
	HashMap<String, String> maprunReduceParams = new HashMap<String, String>();
	public HashMap<String, String> mapworkersIPPort = new HashMap<String, String>();
	int workerIndex = 1;
	// for pushdata request
	int workerCount = 0;
	int numberWorkers = 0;
	BlockingQueue<String> syncFileLineQueue;
	MapReduceContext contextObj;

	public boolean isMapReduceThreadAlive = true;

	/**
	 * Inner Class
	 * WorkerStatusClient to invoke /workerstatus request 
	 * @author Aakriti Singla
	 *
	 */
	public class WorkerStatusClient extends Thread {
		boolean isWorkerStatusRequest = true;
		// public String masterServer;
		// public WorkerStatusParams workerParams;


		/**
		 * Parameterized Constructor to initialize /workerstatus request
		 * @param isWorkerStatusRequest
		 */
		public WorkerStatusClient(boolean isWorkerStatusRequest) {
			this.isWorkerStatusRequest = isWorkerStatusRequest;
			// this.masterServer = masterServer;
			// this.workerParams = workerParams;
		}

		/**
		 * Override run function
		 */
		@Override
		public void run() {
			do {
				try {
					String requestUrl = "http://" + masterServer + "/master/workerstatus" + "?port=" + workerParams.port + "&job=" + workerParams.job  + "&status=" + workerParams.status + "&keysRead=" + workerParams.keysRead + "&keysWritten=" + workerParams.keysWritten + "&timeRecieved=" + workerParams.timeRecieved; 
					URL masterUrl = new URL(requestUrl);
					HttpURLConnection connection = (HttpURLConnection) masterUrl.openConnection();
					connection.setRequestMethod("GET");
					connection.setDoOutput(true);
					connection.getResponseCode();
					connection.disconnect();
					try {
						Thread.sleep(10000);
					} 
					catch (InterruptedException e) {
						// TODO Auto-generated catch block
						System.err.println("[ERROR] In WorkerStatusClient -Interupt +" + e);
					}
				}
				catch(MalformedURLException ex) {
					System.err.println("[ERROR] In WorkerStatusClient +" + ex);
				}
				catch(IOException ex) {
					System.err.println("[ERROR] In WorkerStatusClient +" + ex);
				}

			} while (this.isWorkerStatusRequest);
		}
	}
	/**
	 * Inner Thread - Map Thread Class
	 * @author Aakriti Singla
	 *
	 */
	public class MapReduceThread extends Thread {
		// public boolean isAlive = true;
		/**
		 * run function for each MapThread
		 */
		@Override
		public void run() {
			while (isMapReduceThreadAlive) {
				String eachLine = "";
				try {
					synchronized (syncFileLineQueue) {
						while (syncFileLineQueue.size() == 0 && isMapReduceThreadAlive)
							syncFileLineQueue.wait();
						if (! isMapReduceThreadAlive)
							continue;
						// dequeue input lines from queue
						eachLine = syncFileLineQueue.remove();
					}
					try {
						// loading job 
						
						// call map for job class
						if (workerParams.status.equalsIgnoreCase("mapping")) {
							Job requestedJob = (Job) Class.forName(maprunMapParams.get("job")).newInstance();
							// as per input file format
							String[] jobMapParams = eachLine.split("\t");
							if(jobMapParams.length == 2) {
								requestedJob.map(jobMapParams[0], jobMapParams[1], contextObj);
								// increement the count of keys read
								workerParams.keysRead++;
							}
						}
						else {
							// call reducer for job class
							Job requestedJob = (Job) Class.forName(maprunReduceParams.get("job")).newInstance();
							String[] arrLine = eachLine.split(System.getProperty("line.separator"));
							String arrValues[] = new String[arrLine.length];
							for(int index = 0; index < arrLine.length; index++) {
								if(arrLine[index].split("\t").length == 2)
									arrValues[index]=(arrLine[index].split("\t")[1]);
							}
							if(arrLine.length==0)
								continue;
							requestedJob.reduce(arrLine[0].split("\t")[0], arrValues, contextObj);
							workerParams.keysRead ++;
						}
					}
					catch (ClassNotFoundException e) {
						System.err.println("[ERROR] Error while MapThread run + " + e);
					} catch (InstantiationException e) {
						System.err.println("[ERROR] Error while MapThread run + " + e);
					} catch (IllegalAccessException e) {
						System.err.println("[ERROR] Error while MapThread run + " + e);
					}
				}
				catch (InterruptedException ix) {
					System.err.println("[ERROR] Thread Interrupted +" + ix);
					isMapReduceThreadAlive = false;
				}
			}
		}
	}
	/**
	 * Inner Class PushDataThread
	 * @author Aakriti Singla
	 *
	 */
	public class PushDataThread extends Thread {
		String requesturl;
		String requestUrlParameters;

		public PushDataThread(String requesturl, String requestUrlParameters) {
			this.requesturl = requesturl;
			this.requestUrlParameters = requestUrlParameters;
		}

		@Override
		public void run() {
			int repeatFlag = 1;
			URL master;
			try {
				while(repeatFlag==1) {
					master = new URL(this.requesturl);

					HttpURLConnection connection = (HttpURLConnection) master.openConnection();

					connection.setDoOutput(true);
					connection.setRequestMethod("POST");
					connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded"); 
					connection.setRequestProperty("charset", "utf-8");
					connection.setRequestProperty("Content-Length", "" + Integer.toString(this.requestUrlParameters.getBytes().length));
					connection.setDoOutput(true);

					DataOutputStream writer = new DataOutputStream(connection.getOutputStream());
					writer.writeBytes(this.requestUrlParameters);
					writer.flush();
					writer.close();
					int responseStatusCode = (connection.getResponseCode());
					// System.out.println("[Debug] In PushData Threads + responseStatus + " + responseStatusCode);
					if(responseStatusCode != 200) {
						connection.disconnect();
						continue;
					}
					else
						repeatFlag = 0;

					// connection.setReadTimeout(2*2000);

					BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
					String eachLine;

					while ((eachLine = reader.readLine()) != null) {
						System.out.println("[INFO]" + eachLine);
					}

					reader.close();
					connection.disconnect();
				}
			} 
			catch (MalformedURLException e) {
				System.err.println("[ERROR] In PushData Client +" + e);
			} 
			catch (IOException e) {
				System.err.println("[ERROR] In PushData Client +" + e);
			}
		}
	}

	/**
	 * Init function to start Worker Servlet
	 */
	public void init (final ServletConfig config) throws ServletException {

		this.masterServer = config.getInitParameter("master");
		this.rootStorageDir = config.getInitParameter("storagedir");

		// store hash map for /runmap request
		this.maprunMapParams.put("rootstoragedir", this.rootStorageDir);

		this.workerParams = new WorkerStatusParams();
		this.workerParams.port = Integer.parseInt(config.getInitParameter("port"));
		this.workerParams.job = null;
		this.workerParams.keysRead = 0;
		this.workerParams.keysWritten = 0;
		this.workerParams.timeRecieved = System.currentTimeMillis();

		// calling WorkerStatusClient to invoke /workerstatus request every 10 secs
		/************* For /runmap Testing Purpose************************/
		Thread workerstatus = new WorkerStatusClient(true);
		workerstatus.start();
	}
	/**
	 * doGet for Worker Servlet
	 */
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws java.io.IOException {
		System.out.println("[Info] In doGet of Worker Servlet");
	}
	/**
	 * doPost for Worker Servlet
	 */
	public void doPost(HttpServletRequest request, HttpServletResponse response) throws java.io.IOException {
		System.out.println("[Info] In doPost of Worker Servlet");
		// various URL requests to handle 
		String urlRequest = request.getRequestURI();
		// System.out.println("[DEBUG] value of urlRequest + " + urlRequest);
		switch(urlRequest) {
		/************* For /runmap Testing Purpose************************/
		// case "/HW3/runmap" :
		case "/worker/runmap" :
			// call function for runmap request
			System.out.println("[DEBUG] value of urlRequest + " + urlRequest);
			this.runMap(request, response);
			break;
		/************* For /runmap Testing Purpose************************/
		// case "/HW3/runreduce" :	
		case "/worker/runreduce" :
			System.out.println("[DEBUG] In Run Reduce");
			// call function for runreduce request
			this.runReduce(request, response);
			break;
		/************* For /runmap Testing Purpose************************/
		// case "/HW3/pushdata" :
		case "/worker/pushdata" :
			System.out.println("[DEBUG] value of urlRequest + " + urlRequest);
			this.pushData(request, response);
			break;
		}
	}
	/**
	 * runReduce request
	 * @param request
	 * @param response
	 */
	public void runReduce (HttpServletRequest request, HttpServletResponse response) {
		// System.out.println("[INFO] In runreduce phase");
		// initialization
		this.workerParams.status = "reducing";
		//this.workerParams.keysRead = 0;
		this.workerParams.keysWritten = 0;
		
		this.syncFileLineQueue = new LinkedBlockingQueue<String>();

		// storing parameters 
		this.maprunReduceParams.put("job",request.getParameter("job"));
		this.maprunReduceParams.put("output",request.getParameter("output"));
		this.maprunReduceParams.put("numThreads",request.getParameter("numThreads"));

		this.workerParams.job = this.maprunReduceParams.get("job");

		// calling WorkerStatusClient to invoke /workerstatus request every 10 secs
		/************* For /runmap Testing Purpose************************/
		Thread workerstatus = new WorkerStatusClient(false);
		workerstatus.start();

		// create context object
		this.maprunReduceParams.put("rootstoragedir", this.rootStorageDir);
		System.out.println("[DEBUG] reached context obj");
		this.contextObj = new MapReduceContext (this.maprunReduceParams.get("rootstoragedir"), this.workerParams, this.maprunReduceParams.get("output"));
		System.out.println("[DEBUG] after context obj");
		this.reduce();
	}

	/**
	 * call reduce 
	 */
	public void reduce () {
		// sort file to reduce 
		System.out.println("[DEBUG] before sorting");
		this.contextObj.sortSpoolInFile();
		System.out.println("[DEBUG] after sorting");
		ArrayList<MapReduceThread> reduceThreads = new ArrayList<MapReduceThread>();
		isMapReduceThreadAlive = true;
		for(int index = 0; index < Integer.parseInt(this.maprunReduceParams.get("numThreads")); index++) {
			reduceThreads.add(new MapReduceThread());
			reduceThreads.get(index).start();
		}
		try {
			String fileToRead;
			String dirPath = "";
			if (this.rootStorageDir.endsWith("/"))
				dirPath = this.rootStorageDir + "spoolin";
			else 
				dirPath = this.rootStorageDir + "/spoolin"; 
			fileToRead = dirPath + "/toReduce.txt";
			BufferedReader reader = new BufferedReader(new FileReader(fileToRead));
			String eachLine; 
			String prvsKey="";
			String presentKey; 
			String value = "";
			boolean isfirst = true;
			while((eachLine = reader.readLine()) != null) {
				if (eachLine.length() > 0) {
					// System.out.println("[DEBUG] eachLine in reader +" + eachLine);
					presentKey = eachLine.split("\t")[0];
					if (isfirst) {
						prvsKey = presentKey;
						value += eachLine + "\n";
						isfirst = false;
					}
					else if(prvsKey.equals(presentKey)) {
						value += eachLine + "\n";
					}
					else {
						synchronized(this.syncFileLineQueue) {
							this.syncFileLineQueue.add(value);
							this.syncFileLineQueue.notify();
						}
						value = eachLine+"\n";
						prvsKey = presentKey;
					}
				}
			}
			synchronized(this.syncFileLineQueue) {
				this.syncFileLineQueue.add(value);
				this.syncFileLineQueue.notify();
			}

			reader.close();

			while (this.syncFileLineQueue.size() != 0) {}
			isMapReduceThreadAlive = false;

			// join all running threads
			for (MapReduceThread thread : reduceThreads) {
				if (thread.getState()==Thread.State.RUNNABLE) {
					try {
						thread.join();
					} 
					catch (InterruptedException e) {
						System.err.println(" [Output from log4j] Error while joining threads " + e);
					}
				}
			}
			// re-initialize worker params
			this.workerParams.status="idle";
			// this.workerParams.keysRead = 0;
			this.workerParams.job = "null";
			// calling WorkerStatusClient to invoke /workerstatus request every 10 secs
			/************* For /runmap Testing Purpose************************/
			Thread workerstatus = new WorkerStatusClient(false);
			workerstatus.start();
		}
		catch (FileNotFoundException e) {
			System.err.println("[ERROR] Error in reduce + " + e);
		} 
		catch (IOException e) {
			System.err.println("[ERROR] Error in reduce + " + e);
		}
	}

	/**
	 * pushdata request call
	 * @param request
	 * @param response
	 */
	public void pushData (HttpServletRequest request, HttpServletResponse response) {
		String content = request.getParameter("content");
		if(this.contextObj != null) {
			this.workerCount++;
			this.contextObj.writeforReduceFile(content);
		}
		if (this.workerCount == this.numberWorkers) {
			this.workerParams.status = "waiting";
			// calling WorkerStatusClient to invoke /workerstatus request every 10 secs
			/************* For /runmap Testing Purpose************************/
			Thread workerstatus = new WorkerStatusClient(false);
			workerstatus.start();
		}
	}

	/**
	 * runmap request call 
	 * @param request
	 * @param response
	 * @throws UnsupportedEncodingException
	 */
	public void runMap (HttpServletRequest request, HttpServletResponse response) throws UnsupportedEncodingException {
		/**
		 * Steps	1. Load class specified in job
		 * 			2. Instantiate the specified numThreads
		 * 			3. Each Thread reads tab-separated key-value pairs line wise from input file
		 * 			4. Create local directories spool-out and spool-in
		 * 			5. Worker should hash key to one of the workers and append in file in spool-out (SHA-1 hashing) 
		 */

		// initialization
		this.workerCount = 0;
		this.workerParams.keysRead = 0;
		this.workerParams.keysWritten = 0;

		this.syncFileLineQueue = new LinkedBlockingQueue<String>();

		// storing hash map for runmap
		this.maprunMapParams.put("job",request.getParameter("job"));
		String inputdir = URLDecoder.decode(request.getParameter("input"), "UTF-8");
		this.maprunMapParams.put("input",inputdir);

		this.maprunMapParams.put("numThreads",request.getParameter("numThreads"));
		this.maprunMapParams.put("numWorkers",request.getParameter("numWorkers"));
		this.numberWorkers = Integer.parseInt(this.maprunMapParams.get("numWorkers"));
		for(int index = 1; index <= this.numberWorkers ; index++)
			// storing hash maps for workers and their ip:port 
			this.mapworkersIPPort.put("worker"+index, request.getParameter("worker"+index));

		// setting workerstatus params
		this.workerParams.job = this.maprunMapParams.get("job");
		this.workerParams.status = "mapping";
		// run /workerstatus request 1 time
		/************* For /runmap Testing Purpose************************/
		Thread workerstatus = new WorkerStatusClient(false);
		workerstatus.start();

		this.setCurrentWorkerID();

		// call constructor for Context class
		this.contextObj = new MapReduceContext(Integer.parseInt(this.maprunMapParams.get("numWorkers")), this.maprunMapParams.get("rootstoragedir"), this.workerIndex, this.workerParams);

		this.map();
	}
	/**
	 * to determine current worker id 
	 */
	public void setCurrentWorkerID() {
		for (String worker : this.mapworkersIPPort.keySet())
			if(this.mapworkersIPPort.get(worker).contains(Integer.toString(this.workerParams.port)))
				this.workerIndex = Integer.parseInt(worker.split("worker")[1]);
	}
	/**
	 * mapping
	 */
	public void map() {
		// set absolute dir path
		String root = this.maprunMapParams.get("rootstoragedir");
		String subInputDir = this.maprunMapParams.get("input");
		String absInputDir = "";
		if (root.endsWith("/") && subInputDir.startsWith("/"))
			absInputDir = root.substring(0,root.length()-1) + subInputDir;
		else if (!(root.endsWith("/")) && !(subInputDir.startsWith("/")))
			absInputDir = root + "/" + subInputDir;
		else 
			absInputDir = root + subInputDir;

		ArrayList<MapReduceThread> mapThreads = new ArrayList<MapReduceThread>();
		for(int index = 0; index < Integer.parseInt(this.maprunMapParams.get("numThreads")); index++) {
			mapThreads.add(new MapReduceThread());
			mapThreads.get(index).start();
		}

		File fInputDir = new File(absInputDir);

		for(File file : fInputDir.listFiles()) {
			try {
				if(file.getName().equals(".DS_Store")||file.getName().equals(".svn")) {
					continue;
				}
				// reading lines from file 
				BufferedReader brObj = new BufferedReader(new FileReader(file));
				String eachLine = "";
				while ((eachLine = brObj.readLine()) != null) {
					synchronized (this.syncFileLineQueue) {
						// enqueue lines of input file 
						this.syncFileLineQueue.add(eachLine);
						this.syncFileLineQueue.notify();
					}
				}
				brObj.close();
			}
			catch(IOException ex) {
				System.err.println("[ERROR] Error in map + " + ex);
			}	
		}

		while (this.syncFileLineQueue.size() != 0) {}
		isMapReduceThreadAlive = false;
		// to changing state of threads 
		//// System.out.println(" [Output from log4j] Out of infinite Daemon Thread loop ");
		/*
		synchronized (this.syncFileLineQueue) {
			this.syncFileLineQueue.notifyAll();
		}
		 */
		// join all running threads
		for (MapReduceThread thread : mapThreads) {
			if (thread.getState()==Thread.State.RUNNABLE) {
				try {
					thread.join();
				} 
				catch (InterruptedException e) {
					System.err.println(" [Output from log4j] Error while joining threads " + e);
				}
			}
		}

		// send /pushdata request
		this.sendPushData();

		// stay in map loop till workerstatus is waiting
		while (! this.workerParams.status.equalsIgnoreCase("waiting")){};
	}
	/**
	 * Function to create /pushdata request
	 */
	public void sendPushData() {
		HashMap<String, String> mapworkerPushData = this.contextObj.mapPushData();
		for(String worker: mapworkerPushData.keySet()) {
			/************* For /runmap Testing Purpose************************/
			String requestUrl = "http://"+this.mapworkersIPPort.get(worker)+"/worker/pushdata";
			// String requestUrl = "http://localhost:8080/HW3/pushdata";
			// System.out.println("[DEBUG] value of urlRequest for push data + " + requestUrl);
			String requestUrlParameters = "";
			requestUrlParameters = "content=" + mapworkerPushData.get(worker) + "&from=" + this.workerIndex;

			PushDataThread pushThread = new PushDataThread(requestUrl, requestUrlParameters);
			pushThread.start();
		}
	}
}

