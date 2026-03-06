package com.teleonome.hippocampus;


import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.charset.Charset;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
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
	private  int globalLimit = 300000; 
	//Threshold to start warning (90% of limit)
	private int warningThreshold = 270000; 
	//Atomic counter to track total points in RAM
	private final java.util.concurrent.atomic.AtomicInteger totalPoints = new java.util.concurrent.atomic.AtomicInteger(0);
	Logger logger;
	long messageArrivedMillis=0;
	private JSONObject denomeJSONObject;
	private PostgresqlPersistenceManager aDBManager;
	private String teleonomeName;
	
	int preLoadHours=48;
	boolean preLoadData=true;
	String processName ;
	int hippocampusPid;
	int totalSacrificedDuringPreload = 0;
	
    private int totalSacrificed = 0; // Define it here
    private String loadDataDuration = "";
    
	public Hippocampus() {
		String fileName =  "/home/pi/Teleonome/lib/Log4J.properties";
		System.out.println("reading log4j file at " + fileName);
		PropertyConfigurator.configure(fileName);
		logger = Logger.getLogger(getClass());
		
		 processName = ManagementFactory.getRuntimeMXBean().getName();
		 hippocampusPid = Integer.parseInt(processName.split("@")[0]);
		logger.warn("line 69, hippocampusPid=" + hippocampusPid);
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
	    boolean validJSONFormat = false;
	    long startProcessTime = System.currentTimeMillis();
	    
	    // Reset trackers for this load session
	    totalSacrificed = 0;
	    int pointsAdded = 0;

	    try {
	        String denomeFileInString = FileUtils.readFileToString(denomeFile, Charset.defaultCharset());
	        logger.info("line 82 checking the Teleonome.denome first, length=" + denomeFileInString.length());
	        denomeJSONObject = new JSONObject(denomeFileInString);
	        validJSONFormat = true;
	        
	        // Load Configuration from Denome
	        identity = new Identity(teleonomeName, TeleonomeConstants.NUCLEI_INTERNAL, TeleonomeConstants.DENECHAIN_INTERNAL_HIPPOCAMPUS, TeleonomeConstants.DENE_HIPPOCAMPUS_CONFIGURATION, TeleonomeConstants.DENE_HIPPOCAMPUS_GLOBAL_LIMITS);
	        globalLimit = (Integer) DenomeUtils.getDeneWordByIdentity(denomeJSONObject, identity, TeleonomeConstants.DENEWORD_VALUE_ATTRIBUTE);
	        
	        identity = new Identity(teleonomeName, TeleonomeConstants.NUCLEI_INTERNAL, TeleonomeConstants.DENECHAIN_INTERNAL_HIPPOCAMPUS, TeleonomeConstants.DENE_HIPPOCAMPUS_CONFIGURATION, TeleonomeConstants.DENE_HIPPOCAMPUS_PRELOAD_HOURS);
	        preLoadHours = (Integer) DenomeUtils.getDeneWordByIdentity(denomeJSONObject, identity, TeleonomeConstants.DENEWORD_VALUE_ATTRIBUTE);
	        
	        // Setup Time Windows
	        ZoneId zone = ZoneId.of("Australia/Melbourne");
	        ZonedDateTime now = ZonedDateTime.now(zone);
	        ZonedDateTime cursor = now.minusHours(preLoadHours);
	        
	        // --- DATA INGESTION PHASE ---
	        while (cursor.isBefore(now)) {
	            ZonedDateTime endOfThisDay = cursor.toLocalDate().atTime(23, 59, 59).atZone(zone);
	            long startTimeSeconds = cursor.toEpochSecond();
	            long endTimeSeconds = now.isBefore(endOfThisDay) ? now.toEpochSecond() : endOfThisDay.toEpochSecond();

	            logger.debug("Processing Day: " + cursor.toLocalDate() + " | Range: " + startTimeSeconds + " to " + endTimeSeconds);

	            identity = new Identity(teleonomeName, TeleonomeConstants.NUCLEI_INTERNAL, TeleonomeConstants.DENECHAIN_INTERNAL_HIPPOCAMPUS, TeleonomeConstants.DENE_HIPPOCAMPUS_DATA_DENE);
	            JSONObject dataDene = DenomeUtils.getDeneByIdentity(denomeJSONObject, identity);
	            JSONArray dataDeneWords = dataDene.getJSONArray("DeneWords");

	            for (int i = 0; i < dataDeneWords.length(); i++) {
	                String valueDenePointer = dataDeneWords.getJSONObject(i).getString(TeleonomeConstants.DENEWORD_VALUE_ATTRIBUTE);
	                Identity valueDenePointerIdentity = new Identity(valueDenePointer);
	                
	                JSONObject storageDataDene = (JSONObject) DenomeUtils.getDeneByIdentity(denomeJSONObject, valueDenePointerIdentity);
	                JSONArray storageDataDeneWords = storageDataDene.getJSONArray("DeneWords");

	                for (int j = 0; j < storageDataDeneWords.length(); j++) {
	                    JSONObject wordObj = storageDataDeneWords.getJSONObject(j);
	                    String storeDataDeneWordName = wordObj.getString(TeleonomeConstants.DENEWORD_NAME_ATTRIBUTE);
	                    String storeDataDeneWordType = wordObj.getString(TeleonomeConstants.DENEWORD_VALUETYPE_ATTRIBUTE);
	                    
	                    if (!storeDataDeneWordType.equals("String") && !storeDataDeneWordType.equals("long")) {
	                        String storeDataDeneWordKey = valueDenePointer + ":" + storeDataDeneWordName;

	                        // Fetch from DB
	                        JSONArray dataItemDatabaseData = aDBManager.getTelepathonDeneWordStart(
	                                valueDenePointerIdentity.deneChainName, 
	                                valueDenePointerIdentity.deneName, 
	                                storeDataDeneWordName, 
	                                startTimeSeconds, 
	                                endTimeSeconds);

	                        for (int k = 0; k < dataItemDatabaseData.length(); k++) {
	                            JSONObject point = dataItemDatabaseData.getJSONObject(k);
	                            long dataValueSecondsTime = point.getLong("timeSeconds");
	                            Object storageDeneWordValue = point.get("Value");

	                            if (storageDeneWordValue != null) {
	                                // 1. Memory Safety: Prune if we hit the global limit
	                                while (totalPoints.get() >= globalLimit) {
	                                    totalSacrificed += emergencyPrune(); 
	                                }

	                                TreeMap<Long, Object> history = (TreeMap<Long, Object>) shortTermMemory.computeIfAbsent(storeDataDeneWordKey, l -> new TreeMap<>());
	                                
	                                // 2. Add data point
	                                history.put(dataValueSecondsTime, storageDeneWordValue);
	                                totalPoints.incrementAndGet();
	                                pointsAdded++;
	                                
	                                // NOTE: Time-based pruning removed from here to prevent "data vanishing"
	                            }
	                        }
	                    }
	                }
	            }
	            // Advance cursor to start of next day
	            cursor = cursor.toLocalDate().plusDays(1).atStartOfDay(zone);
	        }

	        // --- POST-LOAD CLEANUP PHASE ---
	        // Now that everything is loaded, trim to the 24h rolling window
	        performPostLoadCleanup(System.currentTimeMillis() / 1000);

	    } catch (Exception e) {
	        logger.warn(Utils.getStringException(e));
	    }

	    // --- REPORTING AND PERSISTENCE ---
	    int duration = (int) ((System.currentTimeMillis() - startProcessTime) / 1000);
	    loadDataDuration = Utils.getElapsedSecondsToHoursMinutesSecondsString(duration);
	    
	    logger.info("--- Preload Report ---");
	    logger.info("Total Points Added: " + pointsAdded);
	    logger.info("Points Sacrificed: " + totalSacrificed);
	    logger.info("Final Memory Count: " + totalPoints.get());
	    logger.info("Load Duration: " + loadDataDuration);
	    
	    // Save status and internal memory map for verification
	    JSONObject hippocampusStatusDene = generateHippocampusStatusDene();
	    try {
	        FileUtils.writeStringToFile(new File("/home/pi/Teleonome/HippocampusStatus.json"), hippocampusStatusDene.toString(4));
	        
	        StringBuilder debugReport = new StringBuilder();
	        for (String key : shortTermMemory.keySet()) {
	            int size = shortTermMemory.get(key).size();
	            debugReport.append("key=").append(key).append(" size=").append(size).append(System.lineSeparator());
	            logger.debug("Final Storage Check: key=" + key + " size=" + size);
	        }
	        FileUtils.writeStringToFile(new File("/home/pi/Teleonome/Preload.txt"), debugReport.toString());
	        
	    } catch (IOException e) {
	        logger.warn(Utils.getStringException(e));
	    }
	}
	
	private void performPostLoadCleanup(long nowSeconds) {
	    long twentyFourHoursAgo = nowSeconds - 86400L;
	    int totalPruned = 0;

	    for (String key : shortTermMemory.keySet()) {
	        TreeMap<Long, Object> history = (TreeMap<Long, Object>) shortTermMemory.get(key);
	        if (history != null) {
	            // Find all points older than 24 hours
	            int countBefore = history.size();
	            history.headMap(twentyFourHoursAgo).clear();
	            int countAfter = history.size();
	            
	            int prunedFromThisSensor = countBefore - countAfter;
	            totalPruned += prunedFromThisSensor;
	        }
	    }
	    totalPoints.addAndGet(-totalPruned);
	    logger.info("Post-load cleanup finished. Pruned " + totalPruned + " expired points.");
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
							logger.debug("line 292 storeDataDeneWordKey=" + storeDataDeneWordKey);

							checkMemoryHealth();
							TreeMap<Long, Object> history = (TreeMap<Long, Object>) shortTermMemory.computeIfAbsent(storeDataDeneWordKey, k -> {
								return new TreeMap<Long, Object>();
							});
							identity = new Identity(storeDataDeneWordKey);
							storageDeneWordValue =    DenomeUtils.getDeneWordByIdentity(denomeJSONObject, identity, TeleonomeConstants.DENEWORD_VALUE_ATTRIBUTE);
							logger.debug("line 300 storageDeneWordValue=" + storageDeneWordValue);
							// 2. Add new point and increment counter
							if(storageDeneWordValue!=null) {
								logger.debug("line 303 dataValueSecondsTime=" + dataValueSecondsTime);
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
			hippocampusStatusDene.put("hippocampusPid",hippocampusPid);
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
			
		
			long currentPoints = totalPoints.get();
	        // Calculate recommendation: Current points + what we had to throw away
	        long recommendedLimit = currentPoints + totalSacrificed;
	        
	        // Memory calculation: (Recommended Points * 128 bytes per point) / 1024 / 1024 
	        // We add a 50% buffer for the JVM stack, internal objects, and JSON strings.
	        int recommendedMX = (int) (((recommendedLimit * 128) / 1048576) * 1.5);
	        // Ensure a minimum floor of 128MB
	        if(recommendedMX < 128) recommendedMX = 128;

			// Existing status words (Total Points, Duration, etc.)
			hippocampusStatusDeneDeneWord = Utils.createDeneWordJSONObject(TeleonomeConstants.DENE_HIPPOCAMPUS_TOTAL_POINTS,currentPoints ,null,"int",true);
			hippocampusDeneWords.put(hippocampusStatusDeneDeneWord);
			
			
			hippocampusStatusDeneDeneWord = Utils.createDeneWordJSONObject(TeleonomeConstants.DENE_HIPPOCAMPUS_SACRIFIED_POINTS, totalSacrificed ,null,"int",true);
			hippocampusDeneWords.put(hippocampusStatusDeneDeneWord);
			
			hippocampusStatusDeneDeneWord = Utils.createDeneWordJSONObject(TeleonomeConstants.DENE_HIPPOCAMPUS_PRE_LOAD_DURATION, loadDataDuration ,null,"int",true);
			hippocampusDeneWords.put(hippocampusStatusDeneDeneWord);
		
			hippocampusStatusDeneDeneWord = Utils.createDeneWordJSONObject(TeleonomeConstants.DENE_HIPPOCAMPUS_RECOMMENDED_XMX, recommendedMX ,null,"int",true);
			hippocampusDeneWords.put(hippocampusStatusDeneDeneWord);
			
			hippocampusStatusDeneDeneWord = Utils.createDeneWordJSONObject(TeleonomeConstants.DENE_HIPPOCAMPUS_RECOMMENDED_PRELOAD__RECOMMENDED_LIMIT, recommendedLimit ,null,"int",true);
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

	private int emergencyPrune() {
	    int removedCount = 0;
	    // Iterate through every sensor (identity)
	    for (String identity : shortTermMemory.keySet()) {
	        TreeMap<Long, Object> history = (TreeMap<Long, Object>) shortTermMemory.get(identity);
	        if (history != null && !history.isEmpty()) {
	            // Remove the oldest single value (the first key in the TreeMap)
	            history.pollFirstEntry(); 
	            totalPoints.decrementAndGet();
	            removedCount++;
	        }
	    }
	    return removedCount;
	}
	
	private void processRequest(String requestJson) {
	    JSONArray data = new JSONArray();
	    try {
	        JSONObject response = new JSONObject();
	        JSONObject req = new JSONObject(requestJson);
	        logger.debug("line 441, request received=" + req.toString());
	        
	        String id = req.getString("Identity").trim();
	        String requestId = req.optString("RequestId", "default");
	        Identity identity = new Identity(id);
	        
	        int rangeHours = req.getInt("Range");
	        logger.debug("line 453, id=" + id );
	        logger.debug("line 454,  Range Hours=" + rangeHours);
	        ZoneId melbourneZone = ZoneId.of("Australia/Melbourne");
	        // Using ISO-like format for Postgres consistency
	        DateTimeFormatter pgFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
	                .withZone(melbourneZone);
	        Iterator it = shortTermMemory.keySet().iterator();
	        String s;
	        while(it.hasNext()) {
	        	s = (String) it.next();
	        	
	        	logger.debug("line 486, key=" +s + " size=" + shortTermMemory.get(s).size());
	        }
	        // Retrieve the history for this identity
	        TreeMap<Long, Object> history = (TreeMap<Long, Object>) shortTermMemory.get(id);

	        if (history != null && !history.isEmpty()) {
	            logger.debug("line 465, history found. Points in memory: " + history.size());

	            // 1. Standardize to Milliseconds
	            long nowSeconds = System.currentTimeMillis()/1000;
	            long rangeInSeconds = rangeHours * 3600L;
	            long startTsSeconds = nowSeconds - rangeInSeconds;

	            logger.debug("Querying from startTs (sec): " + startTsSeconds);

	            // 2. Get the slice (TailMap)
	            NavigableMap<Long, Object> slice = history.tailMap(startTsSeconds, true);
	            logger.debug("line 465, slice size after time filter=" + slice.size());

	            for (Map.Entry<Long, Object> entry : slice.entrySet()) {
	                long timeSeconds = entry.getKey();
	                
	                // 3. Conversion using Millis
	                ZonedDateTime zdt = Instant.ofEpochSecond(timeSeconds).atZone(melbourneZone);
	                String timeString = zdt.format(pgFormatter);

	                JSONObject j = new JSONObject();
	                // Send back seconds to the browser if that's what the UI expects
	                j.put("timeSeconds", timeSeconds / 1000); 
	                j.put("timeString", timeString);
	                j.put("Value", entry.getValue());
	                data.put(j);
	            }

	            response.put("Identity", id);
	            response.put("Data", data);
	            response.put("telepathonName", identity.deneChainName);
	            response.put("deneWordName", identity.deneWordName);
	            response.put("RequestId", requestId);
	            
	            String destination = TeleonomeConstants.HEART_TOPIC_HIPPOCAMPUS_RESPONSE + "/" + requestId;
	            client.publish(destination, new MqttMessage(response.toString().getBytes()));
	            logger.debug("Response sent to: " + destination + " with " + data.length() + " points.");

	        } else {
	            logger.warn("Request failed: No data found in memory for Identity: " + id);
	            // Optionally send an empty response so the browser doesn't hang
	            response.put("Data", new JSONArray());
	            response.put("RequestId", requestId);
	            String destination = TeleonomeConstants.HEART_TOPIC_HIPPOCAMPUS_RESPONSE + "/" + requestId;
	            client.publish(destination, new MqttMessage(response.toString().getBytes()));
	        }

	    } catch (Exception e) {
	        logger.warn("Error in processRequest: " + Utils.getStringException(e));
	    }
	}


}