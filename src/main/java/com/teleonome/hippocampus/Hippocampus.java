package com.teleonome.hippocampus;


import java.io.File;
import java.io.FileInputStream;
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
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
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
	;
	int preLoadHours=168;
	int memoryWindowDays=7;
	boolean preLoadDataComplete=false;
	String processName ;
	int hippocampusPid;
	int totalSacrificedDuringPreload = 0;

    private int totalSacrificed = 0;
    private String loadDataDuration = "";

	private static final String PREFS_FILE = "/home/pi/Teleonome/lib/hippocampus.properties";

	private void loadPreferences() {
		Properties props = new Properties();
		try (FileInputStream fis = new FileInputStream(PREFS_FILE)) {
			props.load(fis);
			memoryWindowDays = Integer.parseInt(props.getProperty("memory.window.days", "1").trim());
			preLoadHours = memoryWindowDays * 24;
			logger.warn("Hippocampus preferences loaded: memory.window.days=" + memoryWindowDays
					+ " preLoadHours=" + preLoadHours);
		} catch (Exception e) {
			logger.warn("Could not load " + PREFS_FILE + ", using defaults (7 days): " + e.getMessage());
			memoryWindowDays = 7;
			preLoadHours = 168;
		}
	}

	public Hippocampus() {
		String fileName =  "/home/pi/Teleonome/lib/Log4J.properties";
		System.out.println("reading log4j file at " + fileName);
		PropertyConfigurator.configure(fileName);
		logger = Logger.getLogger(getClass());
		
		 processName = ManagementFactory.getRuntimeMXBean().getName();
		 hippocampusPid = Integer.parseInt(processName.split("@")[0]);
		logger.warn("line 69, hippocampusPid=" + hippocampusPid);
		loadPreferences();
		this.shortTermMemory = new ConcurrentHashMap();
		aDBManager = PostgresqlPersistenceManager.instance();
		try {
			start();
		} catch (MqttException e) {
			// TODO Auto-generated catch block
			logger.warn(Utils.getStringException(e));
		}
		
	}

	public static void main(String[] args) {
		Hippocampus organ = new Hippocampus();
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
	        String denomeFileInString=null;
	     //   logger.info("line 82 checking the Teleonome.denome first, length=" + denomeFileInString.length());
	        boolean keepGoing=true;
	        do {
	        	try {
	        		denomeFileInString = FileUtils.readFileToString(denomeFile, Charset.defaultCharset());
	        		validJSONFormat = true;
	        		
	        		  denomeJSONObject = new JSONObject(denomeFileInString);
	        		  keepGoing=false;
	        	}catch(Exception e ) {
	        		denomeFileInString = null;
	        		logger.warn(Utils.getStringException(e));
	        		Thread.sleep(5000);
	        	}
	        }while(keepGoing);

	        // The file root mirrors a live pulse's shape: the actual Denome object (with "Name")
	        // is nested under "Denome". teleonomeName is otherwise only ever set from a live pulse
	        // in absorbPulse(), so without this it stays null for the whole of preload.
	        teleonomeName = denomeJSONObject.getJSONObject("Denome").getString("Name");

	        // Load Configuration from Denome
	        identity = new Identity(teleonomeName, TeleonomeConstants.NUCLEI_INTERNAL, TeleonomeConstants.DENECHAIN_INTERNAL_HIPPOCAMPUS, TeleonomeConstants.DENE_HIPPOCAMPUS_CONFIGURATION, TeleonomeConstants.DENE_HIPPOCAMPUS_GLOBAL_LIMITS);
	        globalLimit = (Integer) DenomeUtils.getDeneWordByIdentity(denomeJSONObject, identity, TeleonomeConstants.DENEWORD_VALUE_ATTRIBUTE);

	        identity = new Identity(teleonomeName, TeleonomeConstants.NUCLEI_INTERNAL, TeleonomeConstants.DENECHAIN_INTERNAL_HIPPOCAMPUS, TeleonomeConstants.DENE_HIPPOCAMPUS_CONFIGURATION, TeleonomeConstants.DENE_HIPPOCAMPUS_PRELOAD_HOURS);
	        try {
	            Integer denomePreLoadHours = (Integer) DenomeUtils.getDeneWordByIdentity(denomeJSONObject, identity, TeleonomeConstants.DENEWORD_VALUE_ATTRIBUTE);
	            if (denomePreLoadHours != null && denomePreLoadHours >= memoryWindowDays * 24) {
	                preLoadHours = denomePreLoadHours;
	            } else {
	                logger.warn("Ignoring stale/short denome Preload Hours (" + denomePreLoadHours + "), using configured default of " + preLoadHours + " hours instead");
	            }
	        } catch (Exception denomePreloadEx) {
	            logger.warn("No usable Preload Hours DeneWord in denome, using configured default of " + preLoadHours + " hours: " + Utils.getStringException(denomePreloadEx));
	        }

	        // Setup Time Window
	        ZoneId zone = ZoneId.of("Australia/Melbourne");
	        ZonedDateTime now = ZonedDateTime.now(zone);
	        long startTimeSeconds = now.minusHours(preLoadHours).toEpochSecond();
	        long endTimeSeconds = now.toEpochSecond();

	        // --- DATA INGESTION PHASE ---
	        // Discover every telepathon that actually reported data to Postgres during the
	        // preload window, instead of only the ones currently listed in the live Denome's
	        // Hippocampus "Data" dene. This way a device's history is still backfilled even if
	        // it isn't currently connected/registered when Hippocampus starts.
	        java.util.List<String> telepathonNames = aDBManager.getDistinctTelepathonNames(startTimeSeconds, endTimeSeconds);
	        logger.info("Preload: found " + telepathonNames.size() + " distinct telepathons in DB over the last " + preLoadHours + " hours: " + telepathonNames);

	        for (String telepathonName : telepathonNames) {
	            try {
	                JSONArray telepathonRows = aDBManager.getTelepathonDataStart(telepathonName, startTimeSeconds, endTimeSeconds);
	                for (int r = 0; r < telepathonRows.length(); r++) {
	                    JSONObject row = telepathonRows.getJSONObject(r);
	                    long dataValueSecondsTime = row.getLong("timeSeconds");
	                    JSONObject telepathonChain = row.getJSONObject("data");
	                    JSONArray denes = telepathonChain.optJSONArray("Denes");
	                    if (denes == null) continue;

	                    for (int d = 0; d < denes.length(); d++) {
	                        JSONObject dene = denes.getJSONObject(d);
	                        String deneName = dene.getString(TeleonomeConstants.DENE_NAME_ATTRIBUTE);
	                        JSONArray storageDataDeneWords = dene.optJSONArray("DeneWords");
	                        if (storageDataDeneWords == null) continue;

	                        for (int j = 0; j < storageDataDeneWords.length(); j++) {
	                            JSONObject wordObj = storageDataDeneWords.getJSONObject(j);
	                            String storeDataDeneWordName = wordObj.getString(TeleonomeConstants.DENEWORD_NAME_ATTRIBUTE);
	                            String storeDataDeneWordType = wordObj.getString(TeleonomeConstants.DENEWORD_VALUETYPE_ATTRIBUTE);

	                            if (!storeDataDeneWordType.equals("String") && !storeDataDeneWordType.equals("long")) {
	                                Object storageDeneWordValue = wordObj.opt(TeleonomeConstants.DENEWORD_VALUE_ATTRIBUTE);
	                                if (storageDeneWordValue == null) continue;

	                                String storeDataDeneWordKey = "@" + teleonomeName + ":" + TeleonomeConstants.NUCLEI_TELEPATHONS
	                                        + ":" + telepathonName + ":" + deneName + ":" + storeDataDeneWordName;

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
	            } catch (Exception perTelepathonEx) {
	                // Isolate failures per telepathon: one misconfigured/malformed device must not
	                // abort preload for every other telepathon found in the database.
	                logger.warn("Preload failed for telepathon " + telepathonName + ": " + Utils.getStringException(perTelepathonEx));
	            }
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
	    long windowSeconds = 86400L * memoryWindowDays;
	    long windowAgo = nowSeconds - windowSeconds;
	    int totalPruned = 0;

	    for (String key : shortTermMemory.keySet()) {
	        TreeMap<Long, Object> history = (TreeMap<Long, Object>) shortTermMemory.get(key);
	        if (history != null) {
	            int countBefore = history.size();
	            history.headMap(windowAgo).clear();
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
		options.setCleanSession(false);
		options.setAutomaticReconnect(true);
		options.setKeepAliveInterval(120);

		client.setCallback(new MqttCallbackExtended() {
			@Override
			public void connectComplete(boolean reconnect, String serverURI) {
				if (reconnect) {
					logger.warn("Reconnected to Heart broker, re-subscribing topics");
				} else {
					logger.info("Connected to Heart broker");
				}
				try {
					client.subscribe("Status", 1);
					client.subscribe("Hippocampus_Request", 1);
					logger.info("Subscriptions restored");
				} catch (MqttException e) {
					logger.warn("Failed to re-subscribe after connect: " + Utils.getStringException(e));
				}
			}
			@Override
			public void connectionLost(Throwable cause) {
				logger.warn("Heart connection lost: " + cause.getMessage());
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
		// connectComplete callback handles initial subscriptions and all re-subscriptions after reconnect
		logger.info("Hippocampus Active.");
		
		loadData();
		preLoadDataComplete=true;
		String destination = TeleonomeConstants.HEART_TOPIC_HIPPOCAMPUS_RESPONSE;
        client.publish(destination, new MqttMessage("Preload Complete".getBytes()));
        logger.debug("Hippocampus Ready");
        
		PingThread aPingThread = new PingThread();
		aPingThread.start();
	}
	private void absorbPulse(String pulseJson) {
		try {
			System.gc();

			JSONObject denomeJSONObject = new JSONObject(pulseJson);
			JSONObject denomeObject = denomeJSONObject.getJSONObject("Denome");
			teleonomeName = denomeObject.getString("Name");
			Identity identity = new Identity(teleonomeName, TeleonomeConstants.NUCLEI_INTERNAL, TeleonomeConstants.DENECHAIN_INTERNAL_HIPPOCAMPUS, TeleonomeConstants.DENE_HIPPOCAMPUS_CONFIGURATION, TeleonomeConstants.DENE_HIPPOCAMPUS_GLOBAL_LIMITS);
			globalLimit =  (Integer) DenomeUtils.getDeneWordByIdentity(denomeJSONObject, identity, TeleonomeConstants.DENEWORD_VALUE_ATTRIBUTE);
			identity = new Identity(teleonomeName, TeleonomeConstants.NUCLEI_INTERNAL, TeleonomeConstants.DENECHAIN_INTERNAL_HIPPOCAMPUS, TeleonomeConstants.DENE_HIPPOCAMPUS_CONFIGURATION, TeleonomeConstants.DENE_HIPPOCAMPUS_WARNING_THRESHOLD);
			warningThreshold =  (Integer) DenomeUtils.getDeneWordByIdentity(denomeJSONObject, identity, TeleonomeConstants.DENEWORD_VALUE_ATTRIBUTE);
			logger.debug("line 232 globalLimit=" + globalLimit + " warningThreshold= " + warningThreshold);
			//
			// "Data" now lists device TYPES to absorb (e.g. "Chinampa", "Daffodil", "Langley"),
			// not individual device instances. This lets new instances of an already-listed type
			// (LangleyWest today, Langley_North/Langley_East tomorrow) be absorbed automatically,
			// without ever touching the denome again -- we discover which instances actually exist
			// by walking the live pulse's own Telepathons DeneChains every time, the same way
			// loadData() already discovers telepathons dynamically from Postgres for preload.
			identity = new Identity(teleonomeName, TeleonomeConstants.NUCLEI_INTERNAL, TeleonomeConstants.DENECHAIN_INTERNAL_HIPPOCAMPUS, TeleonomeConstants.DENE_HIPPOCAMPUS_DATA_DENE);
			JSONObject dataDene = DenomeUtils.getDeneByIdentity(denomeJSONObject, identity);
			JSONArray dataDeneWords = dataDene.getJSONArray("DeneWords");
			java.util.Set<String> allowedDeviceTypes = new java.util.HashSet<>();
			for (int i = 0; i < dataDeneWords.length(); i++) {
				allowedDeviceTypes.add(dataDeneWords.getJSONObject(i).getString(TeleonomeConstants.DENEWORD_VALUE_ATTRIBUTE));
			}

			JSONArray telepathonChains = DenomeUtils.getAllDeneChainsForNucleus(denomeJSONObject, TeleonomeConstants.NUCLEI_TELEPATHONS);
			for (int t = 0; telepathonChains != null && t < telepathonChains.length(); t++) {
				JSONObject telepathonChain = telepathonChains.getJSONObject(t);
				String telepathonName = telepathonChain.getString(TeleonomeConstants.DENE_NAME_ATTRIBUTE);
				try {
					JSONArray telepathonDenes = telepathonChain.optJSONArray("Denes");
					if (telepathonDenes == null) continue;

					String deviceType = null;
					JSONObject storageDataDene = null;
					for (int d = 0; d < telepathonDenes.length(); d++) {
						JSONObject dene = telepathonDenes.getJSONObject(d);
						String deneName = dene.getString(TeleonomeConstants.DENE_NAME_ATTRIBUTE);
						if (TeleonomeConstants.TELEPATHON_DENE_CONFIGURATION.equals(deneName)) {
							JSONArray configWords = dene.optJSONArray("DeneWords");
							for (int c = 0; configWords != null && c < configWords.length(); c++) {
								JSONObject cw = configWords.getJSONObject(c);
								if ("Device Type Id".equals(cw.getString(TeleonomeConstants.DENEWORD_NAME_ATTRIBUTE))) {
									deviceType = cw.optString(TeleonomeConstants.DENEWORD_VALUE_ATTRIBUTE, null);
								}
							}
						} else if (TeleonomeConstants.TELEPATHON_DENE_PURPOSE.equals(deneName)) {
							storageDataDene = dene;
						}
					}

					if (deviceType == null || !allowedDeviceTypes.contains(deviceType)) continue;
					if (storageDataDene == null) {
						logger.warn("No Purpose dene found for telepathon: " + telepathonName);
						continue;
					}

					// Build the same storage key shape as before ("@Teleonome:Telepathons:Name:Purpose:DeneWord")
					// so existing queries (Hippocampus_Request, TeleonomeDataGateway) keep working unchanged.
					String valueDenePointer = "@" + teleonomeName + ":" + TeleonomeConstants.NUCLEI_TELEPATHONS
							+ ":" + telepathonName + ":" + TeleonomeConstants.TELEPATHON_DENE_PURPOSE;
					JSONArray storageDataDeneWords = storageDataDene.getJSONArray("DeneWords");

					// Seconds Time may be a top-level DeneChain attribute (older pulses)
					// or a DeneWord of type "long" inside the Dene (current structure).
					long dataValueSecondsTime = System.currentTimeMillis() / 1000;
					if (telepathonChain.has("Seconds Time")) {
						dataValueSecondsTime = telepathonChain.getLong("Seconds Time");
					} else {
						for (int j = 0; j < storageDataDeneWords.length(); j++) {
							JSONObject dw = storageDataDeneWords.getJSONObject(j);
							if ("Seconds Time".equals(dw.getString(TeleonomeConstants.DENEWORD_NAME_ATTRIBUTE))) {
								dataValueSecondsTime = ((Number) dw.get(TeleonomeConstants.DENEWORD_VALUE_ATTRIBUTE)).longValue();
								break;
							}
						}
					}
					logger.debug("line 263 dataValueSecondsTime=" + dataValueSecondsTime);

					for (int j = 0; j < storageDataDeneWords.length(); j++) {
						JSONObject dwObj = storageDataDeneWords.getJSONObject(j);
						String storeDataDeneWordName = dwObj.getString(TeleonomeConstants.DENEWORD_NAME_ATTRIBUTE);
						String storeDataDeneWordType = dwObj.getString(TeleonomeConstants.DENEWORD_VALUETYPE_ATTRIBUTE);
						if (!storeDataDeneWordType.equals("String") && !storeDataDeneWordType.equals("long")) {
							String storeDataDeneWordKey = valueDenePointer + ":" + storeDataDeneWordName;
							logger.debug("line 292 storeDataDeneWordKey=" + storeDataDeneWordKey);

							checkMemoryHealth();
							TreeMap<Long, Object> history = (TreeMap<Long, Object>) shortTermMemory.computeIfAbsent(storeDataDeneWordKey, k -> new TreeMap<Long, Object>());
							Object storageDeneWordValue = dwObj.opt(TeleonomeConstants.DENEWORD_VALUE_ATTRIBUTE);
							logger.debug("line 300 storageDeneWordValue=" + storageDeneWordValue);
							if (storageDeneWordValue != null) {
								logger.debug("line 303 dataValueSecondsTime=" + dataValueSecondsTime);
								history.put(dataValueSecondsTime, storageDeneWordValue);
								totalPoints.incrementAndGet();
								long windowCutoff = dataValueSecondsTime - (86400L * memoryWindowDays);
								int removedCount = history.headMap(windowCutoff).size();
								history.headMap(windowCutoff).clear();
								totalPoints.addAndGet(-removedCount);
							}
						}
					}
				} catch (Exception perTelepathonEx) {
					// Isolate failures per telepathon: one misconfigured/missing device must not
					// stop every other device present in this pulse from being absorbed.
					logger.warn("absorbPulse failed for telepathon " + telepathonName + ": " + Utils.getStringException(perTelepathonEx));
				}
			}
		} catch (Exception e) {
			logger.warn(Utils.getStringException(e));
		}
	}

	private JSONObject generateHippocampusStatusDene() {
		JSONObject hippocampusStatusDene = new JSONObject();
		try {
			// Compute accurate point count and breakdown in a single pass over shortTermMemory
			// so that PointsUsed and the breakdown slices always sum to the same total.
			java.util.Map<String, Integer> chainCounts = new java.util.HashMap<>();
			// Earliest/latest epoch seconds currently held per device (deneChainName), across all its DeneWords
			java.util.Map<String, long[]> chainRanges = new java.util.HashMap<>();
			int current = 0;
			for (java.util.Map.Entry<String, TreeMap> memEntry : shortTermMemory.entrySet()) {
				String memKey = memEntry.getKey();
				TreeMap<Long, Object> memHistory = memEntry.getValue();
				int count = memHistory.size();
				current += count;
				String groupName = memKey;
				try {
					Identity memId = new Identity(memKey);
					if (memId.deneChainName != null && !memId.deneChainName.isEmpty()) {
						groupName = memId.deneChainName;
					}
				} catch (Exception ignored) {}
				chainCounts.merge(groupName, count, Integer::sum);
				if (!memHistory.isEmpty()) {
					long firstSeconds = memHistory.firstKey();
					long lastSeconds = memHistory.lastKey();
					chainRanges.merge(groupName, new long[]{firstSeconds, lastSeconds},
						(a, b) -> new long[]{Math.min(a[0], b[0]), Math.max(a[1], b[1])});
				}
			}

			double percentUsed = ((double) current / globalLimit) * 100;
			int available = globalLimit - current;
			hippocampusStatusDene.put("Name", TeleonomeConstants.DENE_HIPPOCAMPUS_MEMORY_STATUS_DENE);
			JSONArray hippocampusDeneWords = new JSONArray();
			LocalDateTime currentTime = LocalDateTime.now();
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern(TeleonomeConstants.MNEMOSYNE_TIME_FORMAT);
			String formatedCurrentTime = currentTime.format(formatter);
			hippocampusStatusDene.put(TeleonomeConstants.DATATYPE_TIMESTAMP, formatedCurrentTime);
			hippocampusStatusDene.put(TeleonomeConstants.DATATYPE_TIMESTAMP_MILLISECONDS, System.currentTimeMillis());
			hippocampusStatusDene.put("hippocampusPid", hippocampusPid);
			hippocampusStatusDene.put("Last Message Time", messageArrivedMillis);
			hippocampusStatusDene.put("DeneWords", hippocampusDeneWords);
			JSONObject hippocampusStatusDeneDeneWord;
			hippocampusStatusDeneDeneWord = Utils.createDeneWordJSONObject("PointsUsed", current, null, "int", true);
			hippocampusDeneWords.put(hippocampusStatusDeneDeneWord);
			hippocampusStatusDeneDeneWord = Utils.createDeneWordJSONObject("PointsAvailable", available, null, "int", true);
			hippocampusDeneWords.put(hippocampusStatusDeneDeneWord);
			hippocampusStatusDeneDeneWord = Utils.createDeneWordJSONObject("PercentageUsed", Math.round(percentUsed * 100.0) / 100.0, null, "double", true);
			hippocampusDeneWords.put(hippocampusStatusDeneDeneWord);
			hippocampusStatusDeneDeneWord = Utils.createDeneWordJSONObject("Status", (percentUsed > (100.0 * warningThreshold / globalLimit)) ? "Critical" : "Ok", null, "String", true);
			hippocampusDeneWords.put(hippocampusStatusDeneDeneWord);

			hippocampusStatusDeneDeneWord = Utils.createDeneWordJSONObject(TeleonomeConstants.DENE_HIPPOCAMPUS_PRELOAD_DATA, preLoadDataComplete, null, "boolean", true);
			hippocampusDeneWords.put(hippocampusStatusDeneDeneWord);
			hippocampusStatusDeneDeneWord = Utils.createDeneWordJSONObject(TeleonomeConstants.DENE_HIPPOCAMPUS_PRELOAD_HOURS, preLoadHours, null, "int", true);
			hippocampusDeneWords.put(hippocampusStatusDeneDeneWord);

			long currentPoints = (long) current;
			long recommendedLimit = currentPoints + totalSacrificed;
			int recommendedMX = (int) (((recommendedLimit * 128) / 1048576) * 1.5);
			if (recommendedMX < 128) recommendedMX = 128;

			hippocampusStatusDeneDeneWord = Utils.createDeneWordJSONObject(TeleonomeConstants.DENE_HIPPOCAMPUS_TOTAL_POINTS, currentPoints, null, "int", true);
			hippocampusDeneWords.put(hippocampusStatusDeneDeneWord);
			hippocampusStatusDeneDeneWord = Utils.createDeneWordJSONObject(TeleonomeConstants.DENE_HIPPOCAMPUS_SACRIFIED_POINTS, totalSacrificed, null, "int", true);
			hippocampusDeneWords.put(hippocampusStatusDeneDeneWord);
			hippocampusStatusDeneDeneWord = Utils.createDeneWordJSONObject(TeleonomeConstants.DENE_HIPPOCAMPUS_PRE_LOAD_DURATION, loadDataDuration, null, "int", true);
			hippocampusDeneWords.put(hippocampusStatusDeneDeneWord);
			hippocampusStatusDeneDeneWord = Utils.createDeneWordJSONObject(TeleonomeConstants.DENE_HIPPOCAMPUS_RECOMMENDED_XMX, recommendedMX, null, "int", true);
			hippocampusDeneWords.put(hippocampusStatusDeneDeneWord);
			hippocampusStatusDeneDeneWord = Utils.createDeneWordJSONObject(TeleonomeConstants.DENE_HIPPOCAMPUS_RECOMMENDED_PRELOAD__RECOMMENDED_LIMIT, recommendedLimit, null, "int", true);
			hippocampusDeneWords.put(hippocampusStatusDeneDeneWord);

			if (preLoadDataComplete) {
				hippocampusStatusDeneDeneWord = Utils.createDeneWordJSONObject("Load Process Duration", loadDataDuration, null, "String", true);
				hippocampusDeneWords.put(hippocampusStatusDeneDeneWord);
			}

			// Build MemoryBreakdown from the chainCounts already computed above
			java.util.List<java.util.Map.Entry<String, Integer>> sortedChains =
				new java.util.ArrayList<>(chainCounts.entrySet());
			sortedChains.sort((a, b) -> b.getValue() - a.getValue());
			JSONArray memoryBreakdown = new JSONArray();
			int otherPoints = 0;
			for (int bi = 0; bi < sortedChains.size(); bi++) {
				if (bi < 15) {
					JSONObject bEntry = new JSONObject();
					bEntry.put("name", sortedChains.get(bi).getKey());
					bEntry.put("points", sortedChains.get(bi).getValue());
					memoryBreakdown.put(bEntry);
				} else {
					otherPoints += sortedChains.get(bi).getValue();
				}
			}
			if (otherPoints > 0) {
				JSONObject otherEntry = new JSONObject();
				otherEntry.put("name", "Other");
				otherEntry.put("points", otherPoints);
				memoryBreakdown.put(otherEntry);
			}
			// Stored as JSON string so it travels through the Denome and ExoZero network
			hippocampusStatusDeneDeneWord = Utils.createDeneWordJSONObject("MemoryBreakdown", memoryBreakdown.toString(), null, "String", true);
			hippocampusDeneWords.put(hippocampusStatusDeneDeneWord);

			// Earliest/latest data currently available per device, covering every device (unlike
			// MemoryBreakdown's top-15 cap), so the webapp can show a date range per device tab.
			ZoneId melbourneZoneForRanges = ZoneId.of("Australia/Melbourne");
			DateTimeFormatter rangeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
					.withZone(melbourneZoneForRanges);
			JSONArray deviceDateRanges = new JSONArray();
			for (java.util.Map.Entry<String, long[]> rangeEntry : chainRanges.entrySet()) {
				long startSeconds = rangeEntry.getValue()[0];
				long endSeconds = rangeEntry.getValue()[1];
				JSONObject rangeJSON = new JSONObject();
				rangeJSON.put("name", rangeEntry.getKey());
				rangeJSON.put("start", Instant.ofEpochSecond(startSeconds).atZone(melbourneZoneForRanges).format(rangeFormatter));
				rangeJSON.put("end", Instant.ofEpochSecond(endSeconds).atZone(melbourneZoneForRanges).format(rangeFormatter));
				deviceDateRanges.put(rangeJSON);
			}
			// Stored as JSON string so it travels through the Denome and ExoZero network
			hippocampusStatusDeneDeneWord = Utils.createDeneWordJSONObject("DeviceDateRanges", deviceDateRanges.toString(), null, "String", true);
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
			 message.setQos(TeleonomeConstants.HEART_QUALITY_OF_SERVICE);
			 message.setRetained(false);
			client.publish(TeleonomeConstants.HEART_TOPIC_HIPPOCAMPUS_STATUS, message);

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
	        
	        long rangeMs = req.getLong("Range");
	        logger.debug("line 453, id=" + id );
	        logger.debug("line 454, Range Ms=" + rangeMs);
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

	        // Backfill from Postgres whenever the request reaches further back than what's
	        // currently in memory (e.g. a multi-day chart against a cache that only holds the
	        // live rolling window). Only the missing older slice is fetched -- whatever's
	        // already in memory is left alone -- and the fetched rows are merged into the
	        // in-memory TreeMap so the same range is served from memory next time, without
	        // ever needing to preload every device's full history up front.
	        {
	            long nowSecondsForBackfill = System.currentTimeMillis() / 1000;
	            long startTsForBackfill = nowSecondsForBackfill - (rangeMs / 1000L);
	            Long earliestInMemory = (history != null && !history.isEmpty()) ? history.firstKey() : null;

	            if (earliestInMemory == null || earliestInMemory > startTsForBackfill) {
	                long backfillEndSeconds = (earliestInMemory != null) ? earliestInMemory - 1 : nowSecondsForBackfill;
	                if (backfillEndSeconds >= startTsForBackfill) {
	                    try {
	                        logger.info("Backfilling from Postgres for " + id + ": " + startTsForBackfill + " to " + backfillEndSeconds);
	                        JSONArray backfillRows = aDBManager.getTelepathonDeneWordStart(
	                                identity.deneChainName, identity.deneName, identity.deneWordName,
	                                startTsForBackfill, backfillEndSeconds);

	                        if (history == null) {
	                            history = new TreeMap<>();
	                            shortTermMemory.put(id, history);
	                        }
	                        int backfilled = 0;
	                        for (int i = 0; i < backfillRows.length(); i++) {
	                            JSONObject row = backfillRows.getJSONObject(i);
	                            long ts = row.getLong("timeSeconds");
	                            if (!history.containsKey(ts)) {
	                                while (totalPoints.get() >= globalLimit) {
	                                    totalSacrificed += emergencyPrune();
	                                }
	                                history.put(ts, row.get("Value"));
	                                totalPoints.incrementAndGet();
	                                backfilled++;
	                            }
	                        }
	                        logger.info("Backfilled " + backfilled + " points from Postgres for " + id);
	                    } catch (Exception backfillEx) {
	                        logger.warn("Backfill from Postgres failed for " + id + ": " + Utils.getStringException(backfillEx));
	                    }
	                }
	            }
	        }

	        if (history != null && !history.isEmpty()) {
	            logger.debug("line 465, history found. Points in memory: " + history.size());

	            // 1. Standardize to Milliseconds
	            long nowSeconds = System.currentTimeMillis()/1000;
	            long rangeInSeconds = rangeMs / 1000L;
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
	                j.put("timeSeconds", timeSeconds);
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