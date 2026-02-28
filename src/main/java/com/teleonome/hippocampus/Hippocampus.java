package com.teleonome.hippocampus;


import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
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
import com.teleonome.framework.utils.Utils;

public class Hippocampus {

	private final Map<String,TreeMap> shortTermMemory;
	private MqttClient client;
	private final String broker = "tcp://localhost:1883"; 
	//Total data points allowed across the whole Organ
	private  int globalLimit = 50000; 
	//Threshold to start warning (90% of limit)
	private int warningThreshold = 45000; 
	private boolean prunningActive=false;
	//Atomic counter to track total points in RAM
	private final java.util.concurrent.atomic.AtomicInteger totalPoints = new java.util.concurrent.atomic.AtomicInteger(0);
	Logger logger;
	long messageArrivedMillis=0;
	
	public Hippocampus() {
		String fileName =  "/home/pi/Teleonome/lib/Log4J.properties";
		System.out.println("reading log4j file at " + fileName);
		PropertyConfigurator.configure(fileName);
		logger = Logger.getLogger(getClass());
		this.shortTermMemory = new ConcurrentHashMap();
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
	}
	private void absorbPulse(String pulseJson) {
		try {
			JSONObject denomeJSONObject = new JSONObject(pulseJson);
			JSONObject denomeObject = denomeJSONObject.getJSONObject("Denome");
			String teleonomeName = denomeObject.getString("Name");
			Identity identity = new Identity(teleonomeName, TeleonomeConstants.NUCLEI_INTERNAL, TeleonomeConstants.DENECHAIN_INTERNAL_HIPPOCAMPUS, TeleonomeConstants.DENE_HIPPOCAMPUS_CONFIGURATION, TeleonomeConstants.DENE_HIPPOCAMPUS_GLOBAL_LIMITS);
			globalLimit =  (Integer) DenomeUtils.getDeneWordByIdentity(denomeJSONObject, identity, TeleonomeConstants.DENEWORD_VALUE_ATTRIBUTE);

			 identity = new Identity(teleonomeName, TeleonomeConstants.NUCLEI_INTERNAL, TeleonomeConstants.DENECHAIN_INTERNAL_HIPPOCAMPUS, TeleonomeConstants.DENE_HIPPOCAMPUS_CONFIGURATION, TeleonomeConstants.DENE_HIPPOCAMPUS_WARNING_THRESHOLD);
			warningThreshold =  (Integer) DenomeUtils.getDeneWordByIdentity(denomeJSONObject, identity, TeleonomeConstants.DENEWORD_VALUE_ATTRIBUTE);
			
			logger.debug("line 100 globalLimit=" + globalLimit + " warningThreshold= " + warningThreshold);
			//
			// get the data to store, which are the denewords in the "Data" dene of
			// 
			//
			 identity = new Identity(teleonomeName, TeleonomeConstants.NUCLEI_INTERNAL, TeleonomeConstants.DENECHAIN_INTERNAL_HIPPOCAMPUS, TeleonomeConstants.DENE_HIPPOCAMPUS_DATA_DENE);
			JSONObject dataDene =   DenomeUtils.getDeneByIdentity(denomeJSONObject, identity);
			JSONArray dataDeneWords = dataDene.getJSONArray("DeneWords");
			JSONObject dataDeneWord, storageDataDene;
			String valueDenePointer, storeDataDeneWordName,storeDataDeneWordKey ;
			Identity storeDataDeneWordIdentity, valueDenePointerIdentity;
			JSONArray storageDataDeneWords;
			JSONObject storageDeneWord;
			Object storageDeneWordValue;
			long dataValueSecondsTime;
			Identity dataValueSecondsTimeIdentity, dataValueDeneChainIdentity;
			JSONObject dataValueDeneChain;
			
			for(int i=0;i<dataDeneWords.length();i++) {
				//
				// valueDenePointer contains something like "@ChinampaMonitor:Telepathons:Chinampa:Purpose"
				valueDenePointer = dataDeneWords.getJSONObject(i).getString(TeleonomeConstants.DENEWORD_VALUE_ATTRIBUTE);
				valueDenePointerIdentity = new Identity(valueDenePointer);
				logger.debug("line 123 valueDenePointer=" + valueDenePointer);
				dataValueDeneChainIdentity = new Identity(valueDenePointerIdentity.getTeleonomeName(), valueDenePointerIdentity.nucleusName, valueDenePointerIdentity.deneChainName);
				dataValueDeneChain =  (JSONObject) DenomeUtils.getDeneChainByIdentity(denomeJSONObject, dataValueDeneChainIdentity);
				dataValueSecondsTime = dataValueDeneChain.getLong("Seconds Time");
				logger.debug("line 127 dataValueSecondsTime=" + dataValueSecondsTime);
				storageDataDene=  (JSONObject) DenomeUtils.getDeneByIdentity(denomeJSONObject, identity);
				storageDataDeneWords = storageDataDene.getJSONArray("DeneWords");
				for(int j=0;j<storageDataDeneWords.length();j++) {
					storeDataDeneWordName  = storageDataDeneWords.getJSONObject(j).getString(TeleonomeConstants.DENEWORD_NAME_ATTRIBUTE);
					storeDataDeneWordKey = valueDenePointer + ":" + storeDataDeneWordName;
					logger.debug("line 133 storeDataDeneWordKey=" + storeDataDeneWordKey);
					
					checkMemoryHealth();
					TreeMap<Long, Object> history = (TreeMap<Long, Object>) shortTermMemory.computeIfAbsent(storeDataDeneWordKey, k -> {
						return new TreeMap<Long, Object>();
					});
					storeDataDeneWordIdentity = new Identity(storeDataDeneWordKey);
					storageDeneWordValue =    DenomeUtils.getDeneWordByIdentity(denomeJSONObject, identity, TeleonomeConstants.DENEWORD_VALUE_ATTRIBUTE);
					logger.debug("line 141 storageDeneWordValue=" + storageDeneWordValue);
					// 2. Add new point and increment counter
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
			hippocampusStatusDeneDeneWord = Utils.createDeneWordJSONObject("Status", percentUsed > 100*(warningThreshold/globalLimit) ? "Critical" : "Ok" ,null,"String",true);
			hippocampusDeneWords.put(hippocampusStatusDeneDeneWord);
	        
	    } catch (Exception e) {
	        System.err.println("Could not broadcast health: " + e.getMessage());
	    }
	    return hippocampusStatusDene;
	}
	
	private void broadcastHealth() {
	    try {
	    	 JSONObject hippocampusStatusDene= generateHippocampusStatusDene();
	        MqttMessage message = new MqttMessage(hippocampusStatusDene.toString().getBytes());
	        message.setQos(0); // Low priority, no need to retry
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
					FileUtils.writeStringToFile(new File("/home/pi/Teleonome/HippocampusStatus.json"), hippocampusStatusDene.toString());
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
		prunningActive=false;
		if (currentSize > globalLimit) {
			System.out.println("CRITICAL: Memory Limit Exceeded (" + currentSize + "). Emergency Pruning...");
			emergencyPrune();
			prunningActive=true;
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
		try {
			JSONObject req = new JSONObject(requestJson);
			String id = req.getString("Identity");
			String range = req.optString("Range", "24h");

			TreeMap history = (TreeMap) shortTermMemory.get(id);
			if (history != null && !history.isEmpty()) {
				long now = System.currentTimeMillis();
				long startTs = range.equals("lastHour") ? (now - 3600000L) : (now - 86400000L);

				NavigableMap slice = history.tailMap(startTs, true);
				JSONArray data = new JSONArray();

				for (Object entryObj : slice.entrySet()) {
					Map.Entry entry = (Map.Entry) entryObj;
					data.put(new JSONObject()
							.put("t", entry.getKey())
							.put("v", entry.getValue()));
				}

				JSONObject response = new JSONObject();
				response.put("Identity", id);
				response.put("Data", data);

				client.publish("Hippocampus_Response", new MqttMessage(response.toString().getBytes()));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private Object resolveValueByIdentity(JSONObject pulseBody, String identity) {
		try {
			String[] parts = identity.substring(1).split(":");
			JSONObject denome = pulseBody.getJSONObject("Denome");
			JSONArray sectors = denome.getJSONArray("Sectors");

			for (int i = 0; i < sectors.length(); i++) {
				JSONObject sector = sectors.getJSONObject(i);
				if (sector.getString("Name").equals(parts[1])) {
					JSONArray chains = sector.getJSONArray("DeneChains");
					for (int j = 0; j < chains.length(); j++) {
						JSONObject chain = chains.getJSONObject(j);
						if (chain.getString("Name").equals(parts[2])) {
							JSONArray denes = chain.getJSONArray("Denes");
							for (int k = 0; k < denes.length(); k++) {
								JSONObject dene = denes.getJSONObject(k);
								if (dene.getString("Name").equals(parts[3])) {
									JSONArray words = dene.getJSONArray("DeneWords");
									for (int l = 0; l < words.length(); l++) {
										JSONObject word = words.getJSONObject(l);
										if (word.getString("Name").equals(parts[4])) {
											return word.get("Value");
										}
									}
								}
							}
						}
					}
				}
			}
		} catch (Exception e) {}
		return null;
	}
}