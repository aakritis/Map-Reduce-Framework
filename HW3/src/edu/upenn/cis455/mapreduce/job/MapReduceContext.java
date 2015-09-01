package edu.upenn.cis455.mapreduce.job;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.worker.WorkerStatusParams;
/**
 * MapReduceContext Class for Hashing/Writing data for Map/Reduce
 * @author Aakriti Singla
 *
 */
public class MapReduceContext implements Context {

	String rootStorageDir;
	WorkerStatusParams workers;
	String outputDir;
	int numWorkers;
	HashMap<Integer, BigInteger> mapRanges;

	/**
	 * Parameterized Constructor for Map
	 * @param numWorkers
	 * @param rootStorageDir
	 * @param workerID
	 * @param workers
	 */
	public MapReduceContext (int numWorkers, String rootStorageDir, int workerID, WorkerStatusParams workers) {
		this.numWorkers = numWorkers;
		// method stub for generating ranges 
		this.mapRanges();
		this.rootStorageDir = rootStorageDir;
		// method stub for creating spool-out dir
		this.createSpoolOutDirectories(workerID);
		this.createSpoolInDirectories();
		this.workers = workers;
	}
	/**
	 * Parameterized constructor for Reduce
	 * @param rootStorageDir
	 * @param workers
	 * @param outputDir
	 */
	public MapReduceContext (String rootStorageDir, WorkerStatusParams workers, String outputDir) {
		this.rootStorageDir = rootStorageDir;
		this.workers = workers;
		this.outputDir = outputDir;
	}
	/**
	 * function to get ranges for each worker
	 * @param numWorkers
	 */
	public void mapRanges () {
		this.mapRanges = new HashMap<Integer, BigInteger>();

		String minValue = "0000000000000000000000000000000000000000";
		String maxValue = "ffffffffffffffffffffffffffffffffffffffff";

		BigInteger maxIntVal = new BigInteger(maxValue, 16);
		BigInteger minIntVal = new BigInteger(minValue, 16);
		BigInteger numWorkerInt = new BigInteger(Integer.toString(this.numWorkers));

		// get range for given values
		BigInteger rangeVal = maxIntVal.divide(numWorkerInt);

		for (int index = 1; index <= this.numWorkers; index++) {
			minIntVal = minIntVal.add(rangeVal);
			this.mapRanges.put(index, minIntVal);
		}
	}
	/**
	 * function to create spool out dir 
	 * @param workerID
	 */
	public void createSpoolOutDirectories (int workerID) {
		File fSpoolOut;
		String dirPath = "";
		if (this.rootStorageDir.endsWith("/"))
			dirPath = this.rootStorageDir + "spoolout";
		else 
			dirPath = this.rootStorageDir + "/spoolout";

		fSpoolOut = new File (dirPath);

		// check if spoolout exists 
		if (fSpoolOut.exists()) {
			// delete nested dir / files 
			this.deleteSubDirs(fSpoolOut);
			// delete root dir 
			fSpoolOut.delete();
		}
		boolean isCreated = fSpoolOut.mkdir();
		if (isCreated) {
			for (int index = 1 ; index <= this.numWorkers ; index++) {
				File fSpoolOutFile = new File (dirPath + "/worker" + index + ".txt");
				try {
					fSpoolOutFile.createNewFile();
				} 
				catch (IOException e) {
					// TODO Auto-generated catch block
					System.err.println("[ERROR] In create spool out file index + " + index + " + " + e);
				}
			}
		}

	}
	/**
	 * Function to create SpoolIn directories
	 * @param workerID
	 */
	public void createSpoolInDirectories () {
		File fSpoolIn;
		String dirPath = "";
		if (this.rootStorageDir.endsWith("/"))
			dirPath = this.rootStorageDir + "spoolin";
		else 
			dirPath = this.rootStorageDir + "/spoolin";

		fSpoolIn = new File (dirPath);

		// check if spoolout exists 
		if (fSpoolIn.exists()) {
			// delete nested dir / files 
			this.deleteSubDirs(fSpoolIn);
			// delete root dir 
			fSpoolIn.delete();
		}
		boolean isCreated = fSpoolIn.mkdir();
		if (isCreated) {
			File fSpoolInFile = new File(dirPath + "/forReduce.txt");
			try {
				fSpoolInFile.createNewFile();
			} 
			catch (IOException e) {
				// TODO Auto-generated catch block
				System.err.println("[ERROR] In create spool in file + " + e);
			}
		}

	}

	/**
	 * write to spoolin file 
	 * @param content
	 */
	public void writeforReduceFile(String content) {
		String fileToWrite;
		String dirPath = "";
		// System.out.println("[DEBUG] In Reduce File + content + " + content);
		if (this.rootStorageDir.endsWith("/"))
			dirPath = this.rootStorageDir + "spoolin";
		else 
			dirPath = this.rootStorageDir + "/spoolin"; 
		fileToWrite = dirPath + "/forReduce.txt";

		FileWriter fwriter;
		try {
			fwriter = new FileWriter(fileToWrite,true);
			fwriter.write(content+"\n");
			fwriter.close();
		} 
		catch (IOException e) {
			e.printStackTrace();
		} 

	}
	/**
	 * sort file in spoolin
	 */
	public void sortSpoolInFile() {

		String fileToRead;
		String dirPath = "";
		if (this.rootStorageDir.endsWith("/"))
			dirPath = this.rootStorageDir + "spoolin";
		else 
			dirPath = this.rootStorageDir + "/spoolin"; 
		fileToRead = dirPath + "/forReduce.txt";
		try {
			Process process = Runtime.getRuntime().exec("sort "+fileToRead);
			BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
			File fOut = new File(dirPath + "/toReduce.txt");
			FileWriter fwriter = new FileWriter(fOut.getAbsoluteFile());
			BufferedWriter bwriter = new BufferedWriter(fwriter);
			String line;
			while((line = reader.readLine()) != null)
				bwriter.write(line+"\n");
			bwriter.close();
			reader.close();
		} 
		catch (IOException e) {
			System.err.println("[ERROR] Error while sorting file + " + e);
		}
	}

	/**
	 * to delete sub directories and files 
	 * @param subdir
	 */
	public void deleteSubDirs (File rootdir) {
		for (File subf : rootdir.listFiles()) 
			if (subf.isDirectory())
				deleteSubDirs(subf);
			else 
				subf.delete();
	}

	/**
	 * Get Hash for the given key 
	 * @param key
	 * @return
	 * @throws NoSuchAlgorithmException
	 */
	private String fetchHash(String key) throws NoSuchAlgorithmException {
		MessageDigest message;
		message = MessageDigest.getInstance("SHA-1");
		message.update(key.getBytes());
		byte byteData[] = message.digest();

		StringBuffer writer = new StringBuffer();
		for (int j = 0; j < byteData.length; j++) {
			writer.append(Integer.toString((byteData[j] & 0xff) + 0x100, 16).substring(1));
		}
		// System.out.println("[DEBUG] In fetchHash writer +" + writer);
		return writer.toString();
	}
	/**
	 * to get mapping worker id
	 * @param key
	 * @return
	 */
	private int getWorkerId(String key) {
		try {
			String hash = this.fetchHash(key);
			BigInteger hashVal = new BigInteger(hash, 16);
			for(int index = 1; index <= this.numWorkers; index++) {
				if((hashVal.compareTo(this.mapRanges.get(index))) < 0 ) {
					return index;
				}
			}
		} 
		catch (NoSuchAlgorithmException e) {
			System.err.println("[ERROR] In get Worker ID + " + e);
		}
		return 0;
	}
	/**
	 * function to map push data 
	 * @return
	 */
	public HashMap<String, String> mapPushData() {
		HashMap<String, String> mapworkerPushData = new HashMap<String, String>();
		for(int i = 1; i <= this.numWorkers; i++) {
			String dirPath = "";
			if (this.rootStorageDir.endsWith("/"))
				dirPath = this.rootStorageDir + "spoolout";
			else 
				dirPath = this.rootStorageDir + "/spoolout";

			String fileName = dirPath + "/worker" + i + ".txt";
			String fileContent = "";
			try {
				BufferedReader br = new BufferedReader(new FileReader(fileName));
				String line;
				while((line = br.readLine())!=null) {
					if(line.equals("")||line.equals("\n"))
						continue;
					fileContent += line+"\n";
				}
				br.close();
				mapworkerPushData.put("worker"+i, fileContent);
				// System.out.println("[DEBUG] Checking file content in mapPushData + " + i + " + " + fileContent );
			} 
			catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return mapworkerPushData;
	}



	/**
	 * to write data in spool files 
	 */
	@Override
	public void write(String key, String value) {
		// TODO Auto-generated method stub
		if (key == null || value == null)
			return;
		if(key.equals("null") || (value.equals("null")))
			return;

		// for mapping data 
		String fileToWrite = "";
		if (this.workers.status.equalsIgnoreCase("mapping")) {
			int workerID = this.getWorkerId(key);
			String dirPath = "";
			if (this.rootStorageDir.endsWith("/"))
				dirPath = this.rootStorageDir + "spoolout";
			else 
				dirPath = this.rootStorageDir + "/spoolout"; 
			fileToWrite = dirPath + "/worker" + workerID + ".txt";

		}
		else {
			// when reducing
			String dirPath = "";
			if (this.rootStorageDir.endsWith("/") && this.outputDir.startsWith("/"))
				dirPath = this.rootStorageDir.substring(0,this.rootStorageDir.length()-1) + this.outputDir;
			else if (!(this.rootStorageDir.endsWith("/")) && !(this.outputDir.startsWith("/")))
				dirPath = this.rootStorageDir + "/" + this.outputDir;
			else 
				dirPath = this.rootStorageDir + this.outputDir;
			fileToWrite = dirPath + "/resultReduce.txt";
		}

		FileWriter fileWriter;
		try {
			fileWriter = new FileWriter(fileToWrite, true);
			fileWriter.write(key+"\t"+value+"\n");
			fileWriter.close();
			this.workers.keysWritten ++;
		}
		catch (Exception ex) {
			System.err.println("[ERROR] Error while writing to file +" + ex);
		}
	}
}
