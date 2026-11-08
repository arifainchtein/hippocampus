package com.teleonome.hippocampus;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.teleonome.framework.TeleonomeConstants;
import com.teleonome.framework.persistence.PostgresqlPersistenceManager;
import com.teleonome.framework.utils.Utils;

/**
 * Backs the webapp's Hippocampus modal "Telepathons" tab (which replaced the old
 * Registry Status popup) - answers "what was the last message from every
 * registered telepathon, and is it current or stale" entirely from memory, instead
 * of the webapp searching up to 90 daily telepathon_YYYY_M_D tables per device.
 *
 * Two inputs, both MQTT:
 *   - TelepathonStatus (HEART_TOPIC_TELEPATHON_STATUS): AnnabelleReader publishes the
 *     device's full DeneChain every time a reading (or a Vital Signs / Device Identity
 *     / serial-Dene update) arrives - the same JSON it stores in Postgres. It also
 *     publishes when it rejected the reading as an old duplicate (repeater re-send,
 *     overflow backlog), so a chain only replaces what is held when its Seconds Time
 *     is the same or newer.
 *   - Cerebellum_Status (HEART_TOPIC_CEREBELLUM_STATUS): TelepathonRegistryTask's
 *     "Telepathon Registry" Dene, every ~5 min sweep - the official device list
 *     (Name, Serial Number, Device Type, Seconds Since Last Seen, Status), the
 *     temporary list (pending, not yet promoted) and the overall registry status.
 *
 * Unlike the live denome (AnnabelleWriter prunes a device's chain after an idle
 * hour/day), a device's last record is kept here indefinitely - that is the point.
 * One chain per device, so it is not counted against globalLimit.
 *
 * Survives restarts via a snapshot file (rewritten by PingThread when changed), and
 * any registered device still without a record (e.g. first run, or a device silent
 * for longer than the preload window) gets exactly one background
 * getLastTelepathonData() lookup - never on the request path.
 */
public class TelepathonRegistryMemory {

	public static final String REQUEST_TYPE = "TelepathonRegistry";
	public static final String SNAPSHOT_FILE = "/home/pi/Teleonome/HippocampusTelepathonRegistry.json";

	private static final String DENE_TELEPATHON_REGISTRY = "Telepathon Registry";
	private static final String DENEWORD_REGISTRY_DEVICES = "Telepathon Registry Devices";
	private static final String DENEWORD_REGISTRY_PENDING = "Telepathon Registry Pending Devices";
	private static final String DENEWORD_REGISTRY_STATUS = "Telepathon Registry Status";
	// Same default as the webapp's DEFAULT_CEREBELLUM_DAYS_BEFORE_STALE / CerebellumConfigClient.
	private static final double DEFAULT_DAYS_BEFORE_STALE = 2;

	private final Logger logger = Logger.getLogger(getClass());

	// telepathon name -> {"timeSeconds": long, "data": full DeneChain JSON}
	private final ConcurrentHashMap<String, JSONObject> lastRecords = new ConcurrentHashMap<>();

	private volatile JSONArray registryDevices = new JSONArray();
	private volatile JSONArray registryPending = new JSONArray();
	private volatile String registryStatus = "";
	// When the registry above was received - the Cerebellum's "Seconds Since Last Seen"
	// is relative to its sweep, so this is needed to turn it back into a last-seen time.
	private volatile long registryReceivedSeconds = 0;
	private volatile double daysBeforeStale = DEFAULT_DAYS_BEFORE_STALE;

	private volatile boolean dirty = false;

	// Background database lookups for registered devices with no record yet.
	private final ExecutorService seedExecutor = Executors.newSingleThreadExecutor(r -> {
		Thread t = new Thread(r, "TelepathonRegistrySeed");
		t.setDaemon(true);
		return t;
	});
	private final Set<String> seedAttempted = ConcurrentHashMap.newKeySet();
	private final PostgresqlPersistenceManager aDBManager;

	public TelepathonRegistryMemory(PostgresqlPersistenceManager aDBManager) {
		this.aDBManager = aDBManager;
	}

	// ── Inputs ────────────────────────────────────────────────────────────────

	/** A TelepathonStatus payload, or a telepathon chain from a Status pulse. */
	public void absorbTelepathonChain(JSONObject chain) {
		String name = chain.optString(TeleonomeConstants.DENE_NAME_ATTRIBUTE, "");
		if (name.isEmpty()) return;
		long secondsTime = extractSecondsTime(chain);
		if (secondsTime <= 0) {
			logger.debug("Telepathon chain '" + name + "' has no Seconds Time, not remembered as last record");
			return;
		}
		absorbTelepathonRecord(name, secondsTime, chain);
	}

	/** Keeps the record only if it is at least as new as what is already held. */
	public void absorbTelepathonRecord(String name, long timeSeconds, JSONObject data) {
		lastRecords.compute(name, (k, existing) -> {
			if (existing != null && existing.optLong("timeSeconds", 0) > timeSeconds) return existing;
			JSONObject record = new JSONObject();
			record.put("timeSeconds", timeSeconds);
			record.put("data", data);
			dirty = true;
			return record;
		});
	}

	/**
	 * A Purpose:Cerebellum DeneChain - either a Cerebellum_Status payload or the same
	 * chain read out of a pulse / Teleonome.denome. Only its "Telepathon Registry"
	 * Dene is used; a chain without it (e.g. before the first sweep) changes nothing.
	 */
	public void absorbCerebellumChain(JSONObject cerebellumChain) {
		JSONArray denes = cerebellumChain.optJSONArray("Denes");
		for (int i = 0; denes != null && i < denes.length(); i++) {
			JSONObject dene = denes.optJSONObject(i);
			if (dene == null || !DENE_TELEPATHON_REGISTRY.equals(dene.optString(TeleonomeConstants.DENE_NAME_ATTRIBUTE))) continue;
			JSONArray words = dene.optJSONArray("DeneWords");
			JSONArray devices = null, pending = null;
			String status = null;
			for (int j = 0; words != null && j < words.length(); j++) {
				JSONObject w = words.optJSONObject(j);
				if (w == null) continue;
				String wName = w.optString(TeleonomeConstants.DENEWORD_NAME_ATTRIBUTE);
				Object value = w.opt(TeleonomeConstants.DENEWORD_VALUE_ATTRIBUTE);
				try {
					if (DENEWORD_REGISTRY_DEVICES.equals(wName)) devices = toJSONArray(value);
					else if (DENEWORD_REGISTRY_PENDING.equals(wName)) pending = toJSONArray(value);
					else if (DENEWORD_REGISTRY_STATUS.equals(wName)) status = String.valueOf(value);
				} catch (Exception e) {
					logger.warn("Could not parse Cerebellum registry DeneWord '" + wName + "': " + Utils.getStringException(e));
				}
			}
			if (devices == null) return;
			long received = cerebellumChain.optLong("Seconds Time", System.currentTimeMillis() / 1000);
			// The same sweep reaches us twice (Cerebellum_Status and, via the Hypothalamus,
			// the next pulse) - an older copy must not roll the registry back.
			if (received < registryReceivedSeconds) return;
			registryDevices = devices;
			if (pending != null) registryPending = pending;
			if (status != null) registryStatus = status;
			registryReceivedSeconds = received;
			dirty = true;
			seedMissingRecords();
			return;
		}
	}

	public void setDaysBeforeStale(Object value) {
		try {
			double d = (value instanceof Number) ? ((Number) value).doubleValue() : Double.parseDouble(String.valueOf(value).trim());
			if (d > 0 && d != daysBeforeStale) {
				daysBeforeStale = d;
				dirty = true;
			}
		} catch (Exception ignored) {
			// keep the previous / default value
		}
	}

	// ── Request ───────────────────────────────────────────────────────────────

	/**
	 * One entry per official registry device, sorted by name:
	 *   Name, Serial Number, Device Type, Status (Cerebellum's OK/WARNING/CRITICAL),
	 *   Seconds Since Last Seen (recomputed to now), Current (boolean), LastRecord ({timeSeconds, data} or null)
	 * plus the temporary list as Cerebellum published it.
	 */
	public JSONObject buildResponse(String requestId) {
		long nowSeconds = System.currentTimeMillis() / 1000;
		long staleSeconds = (long) (daysBeforeStale * 86400);
		JSONArray devicesSnapshot = registryDevices;

		java.util.List<JSONObject> devices = new java.util.ArrayList<>();
		for (int i = 0; i < devicesSnapshot.length(); i++) {
			JSONObject reg = devicesSnapshot.optJSONObject(i);
			if (reg == null) continue;
			String name = reg.optString("Name", "");
			JSONObject record = lastRecords.get(name);

			// Last seen = newer of what the Cerebellum knew at its sweep and the last
			// record held here (which may have arrived after that sweep).
			long lastSeen = 0;
			if (reg.has("Seconds Since Last Seen") && registryReceivedSeconds > 0) {
				lastSeen = registryReceivedSeconds - reg.optLong("Seconds Since Last Seen", 0);
			}
			if (record != null) lastSeen = Math.max(lastSeen, record.optLong("timeSeconds", 0));

			JSONObject d = new JSONObject();
			d.put("Name", name);
			d.put("Serial Number", reg.opt("Serial Number"));
			d.put("Device Type", reg.opt("Device Type"));
			d.put("Status", reg.optString("Status", ""));
			if (lastSeen > 0) {
				long age = Math.max(0, nowSeconds - lastSeen);
				d.put("Seconds Since Last Seen", age);
				d.put("Current", age < staleSeconds);
			} else {
				d.put("Current", !"CRITICAL".equals(reg.optString("Status")));
			}
			d.put("LastRecord", record == null ? JSONObject.NULL : record);
			devices.add(d);
		}
		devices.sort((a, b) -> a.optString("Name").compareToIgnoreCase(b.optString("Name")));

		JSONObject response = new JSONObject();
		response.put("RequestId", requestId);
		response.put("Type", REQUEST_TYPE);
		response.put("Devices", new JSONArray(devices));
		response.put("Pending", registryPending);
		response.put("RegistryStatus", registryStatus);
		response.put("RegistryReceivedSeconds", registryReceivedSeconds);
		response.put("DaysBeforeStale", daysBeforeStale);
		response.put("GeneratedSeconds", nowSeconds);
		return response;
	}

	public int getRecordCount() {
		return lastRecords.size();
	}

	// ── Persistence ───────────────────────────────────────────────────────────

	public void saveSnapshotIfDirty() {
		if (!dirty) return;
		dirty = false;
		try {
			JSONObject snapshot = new JSONObject();
			snapshot.put("Records", new JSONObject(lastRecords));
			snapshot.put("RegistryDevices", registryDevices);
			snapshot.put("RegistryPending", registryPending);
			snapshot.put("RegistryStatus", registryStatus);
			snapshot.put("RegistryReceivedSeconds", registryReceivedSeconds);
			snapshot.put("DaysBeforeStale", daysBeforeStale);
			File target = new File(SNAPSHOT_FILE);
			File tmp = new File(SNAPSHOT_FILE + ".tmp");
			FileUtils.writeStringToFile(tmp, snapshot.toString(), Charset.defaultCharset());
			if (!tmp.renameTo(target)) {
				FileUtils.copyFile(tmp, target);
				tmp.delete();
			}
		} catch (Exception e) {
			dirty = true;
			logger.warn("Could not save " + SNAPSHOT_FILE + ": " + Utils.getStringException(e));
		}
	}

	public void loadSnapshot() {
		File file = new File(SNAPSHOT_FILE);
		if (!file.exists()) {
			logger.info("No " + SNAPSHOT_FILE + " yet, telepathon registry memory starts empty");
			return;
		}
		try {
			JSONObject snapshot = new JSONObject(FileUtils.readFileToString(file, Charset.defaultCharset()));
			JSONObject records = snapshot.optJSONObject("Records");
			if (records != null) {
				for (String name : records.keySet()) {
					JSONObject r = records.optJSONObject(name);
					if (r != null && r.has("data")) {
						absorbTelepathonRecord(name, r.optLong("timeSeconds", 0), r.getJSONObject("data"));
					}
				}
			}
			JSONArray devices = snapshot.optJSONArray("RegistryDevices");
			if (devices != null) registryDevices = devices;
			JSONArray pending = snapshot.optJSONArray("RegistryPending");
			if (pending != null) registryPending = pending;
			registryStatus = snapshot.optString("RegistryStatus", "");
			registryReceivedSeconds = snapshot.optLong("RegistryReceivedSeconds", 0);
			daysBeforeStale = snapshot.optDouble("DaysBeforeStale", DEFAULT_DAYS_BEFORE_STALE);
			dirty = false;
			logger.info("Loaded " + lastRecords.size() + " telepathon last records and " + registryDevices.length()
					+ " registry devices from " + SNAPSHOT_FILE);
		} catch (Exception e) {
			logger.warn("Could not load " + SNAPSHOT_FILE + ", starting empty: " + Utils.getStringException(e));
		}
	}

	// ── Seeding ───────────────────────────────────────────────────────────────

	/**
	 * Queues one background getLastTelepathonData() lookup for each registry device
	 * that has no record and hasn't been looked up yet this run. Called whenever the
	 * registry changes, so it covers both startup and newly promoted devices.
	 */
	public void seedMissingRecords() {
		JSONArray devicesSnapshot = registryDevices;
		for (int i = 0; i < devicesSnapshot.length(); i++) {
			JSONObject reg = devicesSnapshot.optJSONObject(i);
			if (reg == null) continue;
			final String name = reg.optString("Name", "");
			if (name.isEmpty() || lastRecords.containsKey(name) || !seedAttempted.add(name)) continue;
			seedExecutor.submit(() -> {
				try {
					if (lastRecords.containsKey(name)) return;
					long start = System.currentTimeMillis();
					JSONObject row = aDBManager.getLastTelepathonData(name);
					if (row != null && row.has("data")) {
						absorbTelepathonRecord(name, row.getLong("timeSeconds"), row.getJSONObject("data"));
						logger.info("Seeded last record for telepathon " + name + " from Postgres in "
								+ (System.currentTimeMillis() - start) + "ms");
					} else {
						logger.info("No stored data found in Postgres for registry telepathon " + name);
					}
				} catch (Exception e) {
					logger.warn("Seeding last record for " + name + " failed: " + Utils.getStringException(e));
				}
			});
		}
	}

	// ── Helpers ───────────────────────────────────────────────────────────────

	// Seconds Time may be a top-level DeneChain attribute (older pulses) or a
	// DeneWord inside the Purpose Dene (current structure) - same as absorbPulse().
	private static long extractSecondsTime(JSONObject chain) {
		if (chain.has(TeleonomeConstants.TELEPHATON_DENEWORD_SECONDS_TIME)) {
			return chain.optLong(TeleonomeConstants.TELEPHATON_DENEWORD_SECONDS_TIME, 0);
		}
		JSONArray denes = chain.optJSONArray("Denes");
		for (int d = 0; denes != null && d < denes.length(); d++) {
			JSONObject dene = denes.optJSONObject(d);
			if (dene == null || !TeleonomeConstants.TELEPATHON_DENE_PURPOSE.equals(dene.optString(TeleonomeConstants.DENE_NAME_ATTRIBUTE))) continue;
			JSONArray words = dene.optJSONArray("DeneWords");
			for (int j = 0; words != null && j < words.length(); j++) {
				JSONObject w = words.optJSONObject(j);
				if (w != null && TeleonomeConstants.TELEPHATON_DENEWORD_SECONDS_TIME.equals(w.optString(TeleonomeConstants.DENEWORD_NAME_ATTRIBUTE))) {
					Object v = w.opt(TeleonomeConstants.DENEWORD_VALUE_ATTRIBUTE);
					if (v instanceof Number) return ((Number) v).longValue();
					try { return Long.parseLong(String.valueOf(v).trim()); } catch (Exception e) { return 0; }
				}
			}
		}
		return 0;
	}

	private static JSONArray toJSONArray(Object value) {
		if (value instanceof JSONArray) return (JSONArray) value;
		if (value == null || JSONObject.NULL.equals(value)) return new JSONArray();
		return new JSONArray(String.valueOf(value));
	}
}
