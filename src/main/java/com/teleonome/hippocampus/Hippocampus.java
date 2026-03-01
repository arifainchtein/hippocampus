package com.teleonome.hippocampus;


import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.teleonome.framework.TeleonomeConstants;
import com.teleonome.framework.denome.DenomeUtils;
import com.teleonome.framework.denome.Identity;
import com.teleonome.framework.exception.InvalidDenomeException;
import com.teleonome.framework.persistence.PostgresqlPersistenceManager;
import com.teleonome.framework.utils.Utils;

public class Hippocampus {

	private final Map<String,TreeMap> shortTermMemory;
	private MqttClient client;
	private final String broker = "tcp://localhost:1883"; 
	//Total data points allowed across the whole Organ
	private  int globalLimit = 50000; 
	//Threshold to start warning (90% of limit)
	private int warningThreshold = 45000; 
	//Atomic counter to track total points in RAM
	private final java.util.concurrent.atomic.AtomicInteger totalPoints = new java.util.concurrent.atomic.AtomicInteger(0);
	Logger logger;
	long messageArrivedMillis=0;
	private JSONObject denomeJSONObject;
	private PostgresqlPersistenceManager aDBManager;
	private String teleonomeName;
	String loadDataDuration="";
	int preLoadHours=24;
	boolean preLoadData=true;
	
	public Hippocampus() {
		String fileName =  "/home/pi/Teleonome/lib/Log4J.properties";
		System.out.println("reading log4j file at " + fileName);
		PropertyConfigurator.configure(fileName);
		logger = Logger.getLogger(getClass());
		this.shortTermMemory = new ConcurrentHashMap();
		aDBManager = PostgresqlPersistenceManager.instance();
		PingThread aPingThread = new PingThread();
		aPingThread.start();
		
	}

	public static void main(String[] args) {
		try {
			Hippocampus organ = new Hippocampus();
			organ.start();
		} catch (MqttException e) {
			e.printStackTrace();
		}
	}

	private void loadData() {
		File denomeFile = new File(Utils.getLocalDirectory() + "Teleonome.denome");
		Identity identity;
		boolean validJSONFormat=false;
		long startProcessTime=System.currentTimeMillis();
		try{
			String denomeFileInString = FileUtils.readFileToString(denomeFile, Charset.defaultCharset());
			logger.info("line 82 checking the Teleonome.denome first, length=" + denomeFileInString.length() );
			denomeJSONObject = new JSONObject(denomeFileInString);
			validJSONFormat=true;
			preLoadHours=24;
			 
			identity = new Identity(teleonomeName, TeleonomeConstants.NUCLEI_INTERNAL, TeleonomeConstants.DENECHAIN_INTERNAL_HIPPOCAMPUS, TeleonomeConstants.DENE_HIPPOCAMPUS_CONFIGURATION, TeleonomeConstants.DENE_HIPPOCAMPUS_GLOBAL_LIMITS);
			globalLimit =  (Integer) DenomeUtils.getDeneWordByIdentity(denomeJSONObject, identity, TeleonomeConstants.DENEWORD_VALUE_ATTRIBUTE);
			identity = new Identity(teleonomeName, TeleonomeConstants.NUCLEI_INTERNAL, TeleonomeConstants.DENECHAIN_INTERNAL_HIPPOCAMPUS, TeleonomeConstants.DENE_HIPPOCAMPUS_CONFIGURATION, TeleonomeConstants.DENE_HIPPOCAMPUS_WARNING_THRESHOLD);
			warningThreshold =  (Integer) DenomeUtils.getDeneWordByIdentity(denomeJSONObject, identity, TeleonomeConstants.DENEWORD_VALUE_ATTRIBUTE);
			
			identity = new Identity(teleonomeName, TeleonomeConstants.NUCLEI_INTERNAL, TeleonomeConstants.DENECHAIN_INTERNAL_HIPPOCAMPUS, TeleonomeConstants.DENE_HIPPOCAMPUS_CONFIGURATION, TeleonomeConstants.DENE_HIPPOCAMPUS_PRELOAD_HOURS);
			preLoadHours =  (Integer) DenomeUtils.getDeneWordByIdentity(denomeJSONObject, identity, TeleonomeConstants.DENEWORD_VALUE_ATTRIBUTE);
			identity = new Identity(teleonomeName, TeleonomeConstants.NUCLEI_INTERNAL, TeleonomeConstants.DENECHAIN_INTERNAL_HIPPOCAMPUS, TeleonomeConstants.DENE_HIPPOCAMPUS_CONFIGURATION, TeleonomeConstants.DENE_HIPPOCAMPUS_PRELOAD_DATA);
			preLoadData =  (Boolean) DenomeUtils.getDeneWordByIdentity(denomeJSONObject, identity, TeleonomeConstants.DENEWORD_VALUE_ATTRIBUTE);
				
			//
			// get the data
			//
			ZoneId zone = ZoneId.of("Australia/Melbourne");
			ZonedDateTime now = ZonedDateTime.now(zone);
			ZonedDateTime overallStart = now.minusHours(preLoadHours);

			// Use the exact calculated start as our initial cursor
			ZonedDateTime cursor = overallStart;

			while (cursor.isBefore(now)) {
				// Calculate the very end of 'this' specific day (23:59:59)
				ZonedDateTime endOfThisDay = cursor.toLocalDate().atTime(23, 59, 59).atZone(zone);

				// 1. Determine the START for this database call
				// For the first loop, this is exactly 2:00 PM (or whatever minusHours results in)
				long startTimeSeconds = cursor.toEpochSecond();

				// 2. Determine the END for this database call
				long endTimeSeconds;
				// If 'now' is within this same day, end at 'now'. 
				// Otherwise, end at the end of the calendar day.
				if (now.isBefore(endOfThisDay)) {
					endTimeSeconds = now.toEpochSecond();
				} else {
					endTimeSeconds = endOfThisDay.toEpochSecond();
				}

				// --- Logic to resolve Table Name and Query DB goes here ---
				logger.debug("Processing: " + cursor.toLocalDate());
				logger.debug("Start: " + startTimeSeconds + " | End: " + endTimeSeconds);

				identity = new Identity(teleonomeName, TeleonomeConstants.NUCLEI_INTERNAL, TeleonomeConstants.DENECHAIN_INTERNAL_HIPPOCAMPUS, TeleonomeConstants.DENE_HIPPOCAMPUS_DATA_DENE);
				JSONObject dataDene =   DenomeUtils.getDeneByIdentity(denomeJSONObject, identity);
				JSONArray dataDeneWords = dataDene.getJSONArray("DeneWords");
				JSONObject storageDataDene;
				String valueDenePointer, storeDataDeneWordName,storeDataDeneWordKey ;
				Identity valueDenePointerIdentity;
				JSONArray storageDataDeneWords;
				Object storageDeneWordValue;
				long dataValueSecondsTime;
				Identity dataValueDeneChainIdentity;
				JSONObject dataValueDeneChain, dataItemDatabaseDataPoint;
				String telepathonName, deneName;
				JSONArray dataItemDatabaseData;
				String storeDataDeneWordType;
				
				for(int i=0;i<dataDeneWords.length();i++) {
					//
					// valueDenePointer contains something like "@ChinampaMonitor:Telepathons:Chinampa:Purpose"
					valueDenePointer = dataDeneWords.getJSONObject(i).getString(TeleonomeConstants.DENEWORD_VALUE_ATTRIBUTE);
					valueDenePointerIdentity = new Identity(valueDenePointer);
					telepathonName=  valueDenePointerIdentity.deneChainName;
					deneName=valueDenePointerIdentity.deneName;
					logger.debug("line 140 valueDenePointer=" + valueDenePointer);
					dataValueDeneChainIdentity = new Identity(valueDenePointerIdentity.getTeleonomeName(), valueDenePointerIdentity.nucleusName, valueDenePointerIdentity.deneChainName);
					logger.debug("line 144 dataValueDeneChainIdentity=" + dataValueDeneChainIdentity.toString());
					dataValueDeneChain =  (JSONObject) DenomeUtils.getDeneChainByIdentity(denomeJSONObject, dataValueDeneChainIdentity);
					logger.debug("line 147 dataValueDeneChain=" + dataValueDeneChain.toString());

					storageDataDene=  (JSONObject) DenomeUtils.getDeneByIdentity(denomeJSONObject, valueDenePointerIdentity);
					storageDataDeneWords = storageDataDene.getJSONArray("DeneWords");
					for(int j=0;j<storageDataDeneWords.length();j++) {
						storeDataDeneWordName  = storageDataDeneWords.getJSONObject(j).getString(TeleonomeConstants.DENEWORD_NAME_ATTRIBUTE);
						storeDataDeneWordType  = storageDataDeneWords.getJSONObject(j).getString(TeleonomeConstants.DENEWORD_VALUETYPE_ATTRIBUTE);
						if(!storeDataDeneWordType.equals("String") && !storeDataDeneWordType.equals("long") ) {
							storeDataDeneWordKey = valueDenePointer + ":" + storeDataDeneWordName;
							logger.debug("line 152 storeDataDeneWordKey=" + storeDataDeneWordKey);
							dataItemDatabaseData= aDBManager.getTelepathonDeneWordStart(  telepathonName,  deneName,  storeDataDeneWordName,   startTimeSeconds,   endTimeSeconds);
							logger.debug("line 154 dataItemDatabaseData=" + dataItemDatabaseData.length());

							for(int k=0;k<dataItemDatabaseData.length();k++) {
								dataItemDatabaseDataPoint = dataItemDatabaseData.getJSONObject(k);
								dataValueSecondsTime = dataItemDatabaseDataPoint.getLong("timeSeconds");
								storageDeneWordValue=dataItemDatabaseDataPoint.get("Value");
								checkMemoryHealth();
								TreeMap<Long, Object> history = (TreeMap<Long, Object>) shortTermMemory.computeIfAbsent(storeDataDeneWordKey, l -> {
									return new TreeMap<Long, Object>();
								});
								logger.debug("line 163 storageDeneWordValue=" + storageDeneWordValue);
								// 2. Add new point and increment counter
								if(storageDeneWordValue!=null) {
									history.put(dataValueSecondsTime, storageDeneWordValue);
									totalPoints.incrementAndGet();
									// 3. Normal Time-based Pruning (24h)
									long dayAgo = dataValueSecondsTime - 86400L;
									// Count how many we are about to remove for the global counter
									int removedCount = history.headMap(dayAgo).size();
									history.headMap(dayAgo).clear();
									totalPoints.addAndGet(-removedCount);
								}
							}
						}
					}
					// 3. Move the cursor to the START of the NEXT day (00:00:00)
					// This ensures the NEXT iteration starts exactly at midnight
					cursor = cursor.toLocalDate().plusDays(1).atStartOfDay(zone);
				}

			}
		}catch(Exception e) {
			logger.warn(Utils.getStringException(e));
		}finally{
		}
		int duration = (int) ((System.currentTimeMillis()-startProcessTime)/1000);
		loadDataDuration = Utils.getElapsedSecondsToHoursMinutesSecondsString(duration);
		logger.debug("Finished loading data, it took " + loadDataDuration);
		JSONObject hippocampusStatusDene= generateHippocampusStatusDene();
		try {
			logger.debug("about to save status file");
			FileUtils.writeStringToFile(new File("/home/pi/Teleonome/HippocampusStatus.json"), hippocampusStatusDene.toString(4));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.warn(Utils.getStringException(e));
		}
	}
	public void start() throws MqttException {
		client = new MqttClient(broker, "Hippocampus_Organ", new MemoryPersistence());
		MqttConnectOptions options = new MqttConnectOptions();
		options.setCleanSession(true);
		options.setAutomaticReconnect(true);

		client.setCallback(new MqttCallback() {
			@Override
			public void connectionLost(Throwable cause) {
				System.out.println("Heart connection lost.");
			}
			@Override
			public void messageArrived(String topic, MqttMessage message) {
				String payload = new String(message.getPayload());
				logger.debug("Message Arrived, topic=" + topic);
				messageArrivedMillis = System.currentTimeMillis();
				if (topic.equals("Status")) {
					absorbPulse(payload);
				} else if (topic.equals("Hippocampus_Request")) {
					processRequest(payload);
				}
			}
			@Override
			public void deliveryComplete(IMqttDeliveryToken token) {}

		});

		client.connect(options);
		client.subscribe("Status", 1);
		client.subscribe("Hippocampus_Request", 1);
		logger.info("Hippocampus Active.");
		
		loadData();
	}
	private void absorbPulse(String pulseJson) {
		try {
			JSONObject denomeJSONObject = new JSONObject(pulseJson);
			JSONObject denomeObject = denomeJSONObject.getJSONObject("Denome");
			teleonomeName = denomeObject.getString("Name");
			Identity identity = new Identity(teleonomeName, TeleonomeConstants.NUCLEI_INTERNAL, TeleonomeConstants.DENECHAIN_INTERNAL_HIPPOCAMPUS, TeleonomeConstants.DENE_HIPPOCAMPUS_CONFIGURATION, TeleonomeConstants.DENE_HIPPOCAMPUS_GLOBAL_LIMITS);
			globalLimit =  (Integer) DenomeUtils.getDeneWordByIdentity(denomeJSONObject, identity, TeleonomeConstants.DENEWORD_VALUE_ATTRIBUTE);
			identity = new Identity(teleonomeName, TeleonomeConstants.NUCLEI_INTERNAL, TeleonomeConstants.DENECHAIN_INTERNAL_HIPPOCAMPUS, TeleonomeConstants.DENE_HIPPOCAMPUS_CONFIGURATION, TeleonomeConstants.DENE_HIPPOCAMPUS_WARNING_THRESHOLD);
			warningThreshold =  (Integer) DenomeUtils.getDeneWordByIdentity(denomeJSONObject, identity, TeleonomeConstants.DENEWORD_VALUE_ATTRIBUTE);
			logger.debug("line 232 globalLimit=" + globalLimit + " warningThreshold= " + warningThreshold);
			//
			// get the data to store, which are the denewords in the "Data" dene of
			// 
			//
			identity = new Identity(teleonomeName, TeleonomeConstants.NUCLEI_INTERNAL, TeleonomeConstants.DENECHAIN_INTERNAL_HIPPOCAMPUS, TeleonomeConstants.DENE_HIPPOCAMPUS_DATA_DENE);
			JSONObject dataDene =   DenomeUtils.getDeneByIdentity(denomeJSONObject, identity);
			JSONArray dataDeneWords = dataDene.getJSONArray("DeneWords");
			JSONObject storageDataDene;
			String valueDenePointer, storeDataDeneWordName,storeDataDeneWordKey ;
			Identity valueDenePointerIdentity;
			JSONArray storageDataDeneWords;
			Object storageDeneWordValue;
			long dataValueSecondsTime;
			Identity dataValueDeneChainIdentity;
			JSONObject dataValueDeneChain;
			String storeDataDeneWordType;
			for(int i=0;i<dataDeneWords.length();i++) {
				//
				// valueDenePointer contains something like "@ChinampaMonitor:Telepathons:Chinampa:Purpose"
				valueDenePointer = dataDeneWords.getJSONObject(i).getString(TeleonomeConstants.DENEWORD_VALUE_ATTRIBUTE);
				valueDenePointerIdentity = new Identity(valueDenePointer);
				logger.debug("line 254 valueDenePointer=" + valueDenePointer);
				dataValueDeneChainIdentity = new Identity(valueDenePointerIdentity.getTeleonomeName(), valueDenePointerIdentity.nucleusName, valueDenePointerIdentity.deneChainName);

				logger.debug("line 257 dataValueDeneChainIdentity=" + dataValueDeneChainIdentity.toString());
				dataValueDeneChain =  (JSONObject) DenomeUtils.getDeneChainByIdentity(denomeJSONObject, dataValueDeneChainIdentity);

				if(dataValueDeneChain!=null && dataValueDeneChain.has("Seconds Time")) {
					logger.debug("line 261 dataValueDeneChain=" + dataValueDeneChain.toString());
					dataValueSecondsTime = dataValueDeneChain.getLong("Seconds Time");
					logger.debug("line 263 dataValueSecondsTime=" + dataValueSecondsTime);
					storageDataDene=  (JSONObject) DenomeUtils.getDeneByIdentity(denomeJSONObject, valueDenePointerIdentity);
					storageDataDeneWords = storageDataDene.getJSONArray("DeneWords");
					for(int j=0;j<storageDataDeneWords.length();j++) {
						storeDataDeneWordName  = storageDataDeneWords.getJSONObject(j).getString(TeleonomeConstants.DENEWORD_NAME_ATTRIBUTE);
						
						storeDataDeneWordType  = storageDataDeneWords.getJSONObject(j).getString(TeleonomeConstants.DENEWORD_VALUETYPE_ATTRIBUTE);
						if(!storeDataDeneWordType.equals("String") && !storeDataDeneWordType.equals("long") ) {
							storeDataDeneWordKey = valueDenePointer + ":" + storeDataDeneWordName;
							logger.debug("line 269 storeDataDeneWordKey=" + storeDataDeneWordKey);

							checkMemoryHealth();
							TreeMap<Long, Object> history = (TreeMap<Long, Object>) shortTermMemory.computeIfAbsent(storeDataDeneWordKey, k -> {
								return new TreeMap<Long, Object>();
							});
							identity = new Identity(storeDataDeneWordKey);
							storageDeneWordValue =    DenomeUtils.getDeneWordByIdentity(denomeJSONObject, identity, TeleonomeConstants.DENEWORD_VALUE_ATTRIBUTE);
							logger.debug("line 277 storageDeneWordValue=" + storageDeneWordValue);
							// 2. Add new point and increment counter
							if(storageDeneWordValue!=null) {
								history.put(dataValueSecondsTime, storageDeneWordValue);
								totalPoints.incrementAndGet();
								// 3. Normal Time-based Pruning (24h)
								long dayAgo = dataValueSecondsTime - 86400L;
								// Count how many we are about to remove for the global counter
								int removedCount = history.headMap(dayAgo).size();
								history.headMap(dayAgo).clear();
								totalPoints.addAndGet(-removedCount);
							}
						}
						

					}
				}else {
					logger.warn("The denechain with identity " + dataValueDeneChainIdentity + " does not have Seconds Time, dataValueDeneChain=" + dataValueDeneChain);
				}

			}
		} catch (Exception e) {
			logger.warn(Utils.getStringException(e));
		}
	}

	private JSONObject generateHippocampusStatusDene() {
		JSONObject hippocampusStatusDene = new JSONObject();
		try {
			int current = totalPoints.get();
			double percentUsed = ((double) current / globalLimit) * 100;
			int available = globalLimit - current;
			hippocampusStatusDene.put("Name", TeleonomeConstants.DENE_HIPPOCAMPUS_MEMORY_STATUS_DENE);
			JSONArray hippocampusDeneWords = new JSONArray();
			LocalDateTime currentTime = LocalDateTime.now();
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern(TeleonomeConstants.MNEMOSYNE_TIME_FORMAT);
			String formatedCurrentTime = currentTime.format(formatter);
			hippocampusStatusDene.put(TeleonomeConstants.DATATYPE_TIMESTAMP, formatedCurrentTime);
			hippocampusStatusDene.put(TeleonomeConstants.DATATYPE_TIMESTAMP_MILLISECONDS, System.currentTimeMillis());
			hippocampusStatusDene.put("Last Message Time", messageArrivedMillis);
			hippocampusStatusDene.put("DeneWords", hippocampusDeneWords);
			JSONObject hippocampusStatusDeneDeneWord;
			hippocampusStatusDeneDeneWord = Utils.createDeneWordJSONObject("PointsUsed", current ,null,"int",true);
			hippocampusDeneWords.put(hippocampusStatusDeneDeneWord);
			hippocampusStatusDeneDeneWord = Utils.createDeneWordJSONObject("PointsAvailable", available ,null,"int",true);
			hippocampusDeneWords.put(hippocampusStatusDeneDeneWord);
			hippocampusStatusDeneDeneWord = Utils.createDeneWordJSONObject("PercentageUsed",  Math.round(percentUsed * 100.0) / 100.0 ,null,"double",true);
			hippocampusDeneWords.put(hippocampusStatusDeneDeneWord);
			hippocampusStatusDeneDeneWord = Utils.createDeneWordJSONObject("Status", (percentUsed > (100*(warningThreshold/globalLimit))) ? "Critical" : "Ok" ,null,"String",true);
			hippocampusDeneWords.put(hippocampusStatusDeneDeneWord);
			
			hippocampusStatusDeneDeneWord = Utils.createDeneWordJSONObject(TeleonomeConstants.DENE_HIPPOCAMPUS_PRELOAD_DATA, preLoadData ,null,"boolean",true);
			hippocampusDeneWords.put(hippocampusStatusDeneDeneWord);
			hippocampusStatusDeneDeneWord = Utils.createDeneWordJSONObject(TeleonomeConstants.DENE_HIPPOCAMPUS_PRELOAD_HOURS, preLoadHours ,null,"int",true);
			hippocampusDeneWords.put(hippocampusStatusDeneDeneWord);
			
			if(preLoadData) {
				hippocampusStatusDeneDeneWord = Utils.createDeneWordJSONObject("Load Process Duration", loadDataDuration ,null,"String",true);
				hippocampusDeneWords.put(hippocampusStatusDeneDeneWord);
			}
			
		} catch (Exception e) {
			System.err.println("Could not broadcast health: " + e.getMessage());
		}
		return hippocampusStatusDene;
	}

	private void broadcastHealth() {
		try {
			JSONObject hippocampusStatusDene= generateHippocampusStatusDene();
			MqttMessage message = new MqttMessage(hippocampusStatusDene.toString().getBytes());
			 message.setQos(TeleonomeConstants.HEART_QUALITY_OF_SERVICE);
			 message.setRetained(false);
			client.publish("Hippocampus_Status", message);

		} catch (Exception e) {
			System.err.println("Could not broadcast health: " + e.getMessage());
		}
	}

	class PingThread extends Thread{

		public PingThread(){
			setDaemon(true);
		}
		public void run(){
			while(true) {
				JSONObject hippocampusStatusDene= generateHippocampusStatusDene();
				try {
					logger.debug("about to save status file");
					FileUtils.writeStringToFile(new File("/home/pi/Teleonome/HippocampusStatus.json"), hippocampusStatusDene.toString(4));
					logger.debug("about to broadcasthealth");
					broadcastHealth();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					logger.warn(Utils.getStringException(e));
				}
				try {
					Thread.sleep(1000*30);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		}

	}

	private void checkMemoryHealth() {
		int currentSize = totalPoints.get();
		if (currentSize > globalLimit) {
			System.out.println("CRITICAL: Memory Limit Exceeded (" + currentSize + "). Emergency Pruning...");
			emergencyPrune();
		} else if (currentSize > warningThreshold) {
			System.out.println("WARNING: Hippocampus is 90% full. Current points: " + currentSize);
		}
	}

	private void emergencyPrune() {
		// Iterate through every sensor (identity)
		for (String identity : shortTermMemory.keySet()) {
			TreeMap<Long, Object> history = (TreeMap<Long, Object>) shortTermMemory.get(identity);
			if (history != null && !history.isEmpty()) {
				// Remove the oldest single value (the first key in the TreeMap)
				history.pollFirstEntry(); 
				totalPoints.decrementAndGet();
			}
		}
	}
	private void processRequest(String requestJson) {
		JSONArray data = new JSONArray();
		try {
			JSONObject response = new JSONObject();
			JSONObject req = new JSONObject(requestJson);
			logger.debug("line 441, request received=" +req.toString() );
			String id = req.getString("Identity");
			Identity identity = new Identity(id);
			String telepathonName=identity.deneChainName;
			String deneWordName = identity.deneWordName;
			
			String range = req.optString("Range", "24h");
			ZoneId melbourneZone = ZoneId.of("Australia/Melbourne");
			DateTimeFormatter pgFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
					.withZone(melbourneZone);
			ZonedDateTime zdt;
			TreeMap history = (TreeMap) shortTermMemory.get(id);
			if (history != null && !history.isEmpty()) {
				long now = System.currentTimeMillis()/1000;
				long startTs = range.equals("lastHour") ? (now - 3600L) : (now - 86400L);
				logger.debug("line 453, startTs=" +startTs );
				NavigableMap slice = history.tailMap(startTs, true);
			
				logger.debug("line 455, slice=" +slice.size() );
				JSONObject j;
				long timeSeconds;
				String timeString;
				for (Object entryObj : slice.entrySet()) {
					Map.Entry entry = (Map.Entry) entryObj;
					timeSeconds = (long)entry.getKey();
					zdt = Instant.ofEpochSecond(timeSeconds).atZone(melbourneZone);
					timeString = zdt.format(pgFormatter);

					j = new JSONObject();
					j.put("timeSeconds", timeSeconds);
					j.put("timeString", timeString);
					j.put("Value", entry.getValue());
					data.put(j);
					logger.debug("line 471, j=" +j.toString() );
				}
				response.put("Identity", id);
				response.put("Data", data);
				response.put("telepathonName", telepathonName);
				response.put("deneWordName", deneWordName);
			}
			client.publish(TeleonomeConstants.HEART_TOPIC_HIPPOCAMPUS_RESPONSE, new MqttMessage(response.toString().getBytes()));
		//	client.publish(TeleonomeConstants.HEART_TOPIC_HIPPOCAMPUS_RESPONSE, new MqttMessage(data.toString().getBytes()));
			logger.debug("response sent" );
		} catch (Exception e) {
			logger.warn(Utils.getStringException(e));
		}
	}


}