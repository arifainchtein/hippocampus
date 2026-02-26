public class Hippocampus {

	private final Map shortTermMemory;
	private IMqttClient client;
	private final String broker = "tcp://Egg.local:1883"; 
	//Total data points allowed across the whole Organ
	private final int GLOBAL_LIMIT = 50000; 
	//Threshold to start warning (90% of limit)
	private final int WARNING_THRESHOLD = 45000; 

	//Atomic counter to track total points in RAM
	private final java.util.concurrent.atomic.AtomicInteger totalPoints = new java.util.concurrent.atomic.AtomicInteger(0);

	public Hippocampus() {
		this.shortTermMemory = new ConcurrentHashMap();
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
		System.out.println("Hippocampus Active.");
	}

	private void absorbPulse(String pulseJson) {
		try {
			JSONObject pulse = new JSONObject(pulseJson);
			long ts = pulse.getLong("Pulse Timestamp in Milliseconds");
			String id = "@ChinampaMonitor:Purpose:Sensor Data:Network Status:UploadSpeed";

			Object val = resolveValueByIdentity(pulse, id);
			if (val != null) {
				// 1. Monitor Size and Detect Limits
				checkMemoryHealth();

				TreeMap<Long, Object> history = shortTermMemory.computeIfAbsent(id, k -> {
					return new TreeMap<Long, Object>();
				});

				// 2. Add new point and increment counter
				history.put(ts, val);
				totalPoints.incrementAndGet();

				// 3. Normal Time-based Pruning (24h)
				long dayAgo = ts - 86400000L;
				// Count how many we are about to remove for the global counter
				int removedCount = history.headMap(dayAgo).size();
				history.headMap(dayAgo).clear();
				totalPoints.addAndGet(-removedCount);
			}
		} catch (Exception e) {}
	}

	private void broadcastHealth() {
	    try {
	        int current = totalPoints.get();
	        double percentUsed = ((double) current / GLOBAL_POINT_LIMIT) * 100;
	        int available = GLOBAL_POINT_LIMIT - current;

	        JSONObject health = new JSONObject();
	        health.put("Type", "MemoryHealth");
	        health.put("PointsUsed", current);
	        health.put("PointsAvailable", available);
	        health.put("PercentageUsed", Math.round(percentUsed * 100.0) / 100.0); // 2 decimal places
	        health.put("Status", percentUsed > 90 ? "CRITICAL" : "HEALTHY");

	        MqttMessage message = new MqttMessage(health.toString().getBytes());
	        message.setQos(0); // Low priority, no need to retry
	        client.publish("Hippocampus_Health", message);
	        
	    } catch (Exception e) {
	        System.err.println("Could not broadcast health: " + e.getMessage());
	    }
	}
	
	private void checkMemoryHealth() {
		int currentSize = totalPoints.get();

		if (currentSize > GLOBAL_LIMIT) {
			System.out.println("CRITICAL: Memory Limit Exceeded (" + currentSize + "). Emergency Pruning...");
			emergencyPrune();
		} else if (currentSize > WARNING_THRESHOLD) {
			System.out.println("WARNING: Hippocampus is 90% full. Current points: " + currentSize);
		}
	}

	private void emergencyPrune() {
		// Iterate through every sensor (identity)
		for (String identity : shortTermMemory.keySet()) {
			TreeMap<Long, Object> history = shortTermMemory.get(identity);
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