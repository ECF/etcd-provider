/*******************************************************************************
 * Copyright (c) 2014 Composent, Inc. All rights reserved. This
 * program and the accompanying materials are made available under the terms of
 * the Eclipse Public License v1.0 which accompanies this distribution, and is
 * available at http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors: Scott Lewis - initial API and implementation
 ******************************************************************************/
package org.eclipse.ecf.provider.etcd;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.core.runtime.IStatus;
import org.eclipse.core.runtime.Status;
import org.eclipse.core.runtime.jobs.Job;
import org.eclipse.ecf.core.ContainerConnectException;
import org.eclipse.ecf.core.events.ContainerConnectedEvent;
import org.eclipse.ecf.core.events.ContainerConnectingEvent;
import org.eclipse.ecf.core.events.ContainerDisconnectedEvent;
import org.eclipse.ecf.core.events.ContainerDisconnectingEvent;
import org.eclipse.ecf.core.identity.ID;
import org.eclipse.ecf.core.security.IConnectContext;
import org.eclipse.ecf.discovery.AbstractDiscoveryContainerAdapter;
import org.eclipse.ecf.discovery.IServiceInfo;
import org.eclipse.ecf.discovery.ServiceContainerEvent;
import org.eclipse.ecf.discovery.ServiceInfo;
import org.eclipse.ecf.discovery.ServiceTypeContainerEvent;
import org.eclipse.ecf.discovery.identity.IServiceID;
import org.eclipse.ecf.discovery.identity.IServiceTypeID;
import org.eclipse.ecf.internal.provider.etcd.DebugOptions;
import org.eclipse.ecf.internal.provider.etcd.LogUtility;
import org.eclipse.ecf.internal.provider.etcd.protocol.EtcdDeleteRequest;
import org.eclipse.ecf.internal.provider.etcd.protocol.EtcdException;
import org.eclipse.ecf.internal.provider.etcd.protocol.EtcdGetRequest;
import org.eclipse.ecf.internal.provider.etcd.protocol.EtcdNode;
import org.eclipse.ecf.internal.provider.etcd.protocol.EtcdProtocol;
import org.eclipse.ecf.internal.provider.etcd.protocol.EtcdRequest;
import org.eclipse.ecf.internal.provider.etcd.protocol.EtcdResponse;
import org.eclipse.ecf.internal.provider.etcd.protocol.EtcdSetRequest;
import org.eclipse.ecf.internal.provider.etcd.protocol.EtcdSuccessResponse;
import org.eclipse.ecf.internal.provider.etcd.protocol.EtcdWatchRequest;
import org.eclipse.ecf.provider.etcd.identity.EtcdNamespace;
import org.eclipse.ecf.provider.etcd.identity.EtcdServiceID;
import org.json.JSONException;

public class EtcdDiscoveryContainer extends AbstractDiscoveryContainerAdapter {

	public class EtcdServiceInfoKey {
		private final String sessId;
		private final String serviceInfoId;
		private final String fullKey;

		public EtcdServiceInfoKey() {
			this.sessId = localSessionId;
			this.serviceInfoId = UUID.randomUUID().toString();
			this.fullKey = verifySlash(localSessionId) + this.serviceInfoId;
		}

		public EtcdServiceInfoKey(String sessionId, String serviceInfoId) {
			this.sessId = sessionId;
			this.serviceInfoId = serviceInfoId;
			this.fullKey = verifySlash(sessionId) + this.serviceInfoId;
		}

		public String getFullKey() {
			return this.fullKey;
		}

		public boolean matchSessionId(String sId) {
			if (sessId == null)
				return true;
			return sessId.equals(sId);
		}

		public boolean matchFullKey(String otherFullKey) {
			if (otherFullKey == null)
				return true;
			return this.fullKey.equals(otherFullKey);
		}

		public int hashCode() {
			return this.fullKey.hashCode();
		}

		public boolean equals(Object other) {
			if (other == null)
				return false;
			if (!(other instanceof EtcdServiceInfoKey))
				return false;
			EtcdServiceInfoKey ok = (EtcdServiceInfoKey) other;
			return this.fullKey.equals(ok.fullKey);
		}
	}

	// services
	private final Map<EtcdServiceInfoKey, EtcdServiceInfo> services = new HashMap<EtcdServiceInfoKey, EtcdServiceInfo>();
	private EtcdServiceID etcdTargetID;
	private String localSessionId;
	private String keyPrefix;

	private String dirUrl;
	private EtcdWatchJob watchJob;
	private boolean watchDone;
	private int watchIndex;

	private EtcdTTLJob ttlJob;

	private String getDirectoryUrl() {
		return this.dirUrl;
	}

	public EtcdDiscoveryContainer(EtcdDiscoveryContainerConfig config) {
		super(EtcdNamespace.NAME, config);
	}

	public EtcdDiscoveryContainer() throws MalformedURLException, URISyntaxException {
		super(EtcdNamespace.NAME, new EtcdDiscoveryContainerConfig(EtcdDiscoveryContainer.class.getName()));
	}

	public void registerService(IServiceInfo serviceInfo) {
		trace("registerService", "serviceInfo=" + serviceInfo); //$NON-NLS-1$ //$NON-NLS-2$
		long ttl = serviceInfo.getTTL();
		if (ttl == ServiceInfo.DEFAULT_TTL)
			ttl = ((EtcdDiscoveryContainerConfig) getConfig()).getTTL();
		EtcdServiceInfo si = (serviceInfo instanceof EtcdServiceInfo) ? (EtcdServiceInfo) serviceInfo
				: new EtcdServiceInfo(serviceInfo, ttl);
		String endpointid = serviceInfo.getServiceProperties().getPropertyString("endpoint.id"); //$NON-NLS-1$
		EtcdServiceInfoKey siKey = (endpointid == null) ? new EtcdServiceInfoKey()
				: new EtcdServiceInfoKey(this.localSessionId, endpointid);
		int etcdTTL = convertLongTTLToIntTTL(si.getTTL());
		String siString = null;
		try {
			siString = si.serializeToJsonString();
		} catch (JSONException e) {
			throw new IllegalArgumentException("Exception serializing serviceInfo=" + si, e); //$NON-NLS-1$
		}
		String fullKey = createFullKey(siKey);
		synchronized (services) {
			executeEtcdRequest("registerService", //$NON-NLS-1$
					new EtcdSetRequest(fullKey, siString, etcdTTL),
					"Error in EtcdServiceInfo set request serviceInfo=" + si); //$NON-NLS-1$
			services.put(siKey, si);
		}
		fireServiceTypeDiscovered(si.getServiceID().getServiceTypeID());
		fireServiceDiscovered(fullKey, si);
	}

	private String createFullKey(EtcdServiceInfoKey key) {
		return getDirectoryUrl() + key.getFullKey();
	}

	public void unregisterService(IServiceInfo serviceInfo) {
		trace("unregisterService", "serviceInfo=" + serviceInfo); //$NON-NLS-1$ //$NON-NLS-2$
		EtcdServiceInfoKey key = findEtcdServiceInfoKey(serviceInfo.getServiceID(), true);
		if (key == null) {
			logEtcdError("unregisterService", "Could not find serviceInfo=" + serviceInfo, null); //$NON-NLS-1$ //$NON-NLS-2$
			return;
		}
		String fullKey = createFullKey(key);
		EtcdServiceInfo si = null;
		synchronized (services) {
			executeEtcdRequest("unregisterService", new EtcdDeleteRequest(fullKey), //$NON-NLS-1$
					"Error in EtcdDeleteRequest with serviceInfo=" + serviceInfo); //$NON-NLS-1$
			si = services.remove(key);
		}
		if (si != null)
			fireServiceUndiscovered(fullKey, si);
	}

	private void executeEtcdRequest(String methodName, EtcdRequest request, String exceptionMessage) {
		try {
			EtcdResponse response = request.execute();
			if (response.isError())
				throw new EtcdException(exceptionMessage, response.getErrorResponse());
		} catch (EtcdException e) {
			logAndThrowEtcdError(methodName, "Error communicating with etcd server", e); //$NON-NLS-1$
		}
	}

	@Override
	public String getContainerName() {
		return EtcdDiscoveryContainerInstantiator.NAME;
	}

	private String removeKeyPrefix(String key) {
		return key.substring(this.keyPrefix.length());
	}

	public void connect(ID aTargetID, IConnectContext connectContext) throws ContainerConnectException {
		if (this.etcdTargetID != null)
			throw new ContainerConnectException("Already connected"); //$NON-NLS-1$
		EtcdDiscoveryContainerConfig config = (EtcdDiscoveryContainerConfig) getConfig();
		if (config == null)
			throw new ContainerConnectException("Container has been disposed"); //$NON-NLS-1$

		fireContainerEvent(new ContainerConnectingEvent(getID(), aTargetID, connectContext));

		// set targetID from config
		if (aTargetID == null) {
			etcdTargetID = config.getTargetID();
		} else {
			if (!(aTargetID instanceof EtcdServiceID))
				throw new ContainerConnectException("targetID must be of type EtcdServiceID"); //$NON-NLS-1$
			etcdTargetID = (EtcdServiceID) aTargetID;
		}
		// Set sessionId from config
		localSessionId = config.getSessionId();
		if (localSessionId == null)
			throw new ContainerConnectException("SessionId cannot be null"); //$NON-NLS-1$
		this.keyPrefix = verifySlash("/" + getID().getName()); //$NON-NLS-1$
		// Then set directory URL
		this.dirUrl = this.etcdTargetID.getLocation().toString() + this.keyPrefix;
		// Do a get request on directoryUrl to verify that the EtcdDiscoveryContainer
		// location
		// is present
		try {
			EtcdResponse topResponse = new EtcdGetRequest(getDirectoryUrl(), true).execute();
			if (topResponse.isError()) {
				trace("connect", "etcd directoryURL=" + getDirectoryUrl() + " does not exist, attempting to create"); //$NON-NLS-1$//$NON-NLS-2$ //$NON-NLS-3$
				// Try to create directory
				topResponse = new EtcdSetRequest(getDirectoryUrl()).execute();
				if (topResponse.isError()) {
					trace("doConnect ERROR", "etcd directoryURL could not be created, throwing"); //$NON-NLS-1$ //$NON-NLS-2$
					throw new ContainerConnectException("Could not create etcd directoryURL=" + getDirectoryUrl()); //$NON-NLS-1$
				}
			}
			EtcdSuccessResponse r = topResponse.getSuccessResponse();
			EtcdNode node = r.getNode();
			if (!node.isDirectory())
				throw new ContainerConnectException("etcd directoryUrl=" + getDirectoryUrl() + " is not a directory"); //$NON-NLS-1$ //$NON-NLS-2$
			this.watchIndex = node.getCreatedIndex();
			int sessionTTL = config.getSessionTTL();
			// Add directory contents
			handleAddDirectory(node);
			// create a directory with our unique sessionid
			EtcdResponse sessionExistsResponse = new EtcdSetRequest(getDirectoryUrl() + this.localSessionId, sessionTTL)
					.execute();
			if (sessionExistsResponse.isError())
				throw new ContainerConnectException("Could not create etcd session directory"); //$NON-NLS-1$
			this.watchIndex = sessionExistsResponse.getSuccessResponse().getNode().getCreatedIndex() + 1;
			ttlJob = new EtcdTTLJob(sessionTTL);
			ttlJob.schedule();
			// Now setup and start discovery job
			watchJob = new EtcdWatchJob();
			watchJob.schedule();
		} catch (EtcdException e) {
			throw new ContainerConnectException(
					"Exception communicating with etcd service at directoryURL=" + getDirectoryUrl(), e); //$NON-NLS-1$
		}
		// Fire container connected event
		fireContainerEvent(new ContainerConnectedEvent(this.getID(), aTargetID));
	}

	public ID getConnectedID() {
		return etcdTargetID;
	}

	public void disconnect() {
		if (etcdTargetID != null) {
			ID anID = getConnectedID();
			fireContainerEvent(new ContainerDisconnectingEvent(this.getID(), anID));
			synchronized (services) {
				// delete our sessionId from etcd service
				try {
					new EtcdDeleteRequest(getDirectoryUrl() + this.localSessionId, true).execute();
				} catch (EtcdException e) {
					logEtcdError("shutdownEtcdConnection", "Error with etcd shutdown", e); //$NON-NLS-1$ //$NON-NLS-2$
				}
				services.clear();
				etcdTargetID = null;
				localSessionId = null;
				keyPrefix = null;
				dirUrl = null;
				if (ttlJob != null) {
					ttlJob.cancel();
					try {
						ttlJob.join();
					} catch (InterruptedException e) {
					}
					ttlJob = null;
				}
				if (watchJob != null) {
					watchJob.cancel();
					try {
						watchJob.join();
					} catch (InterruptedException e) {
					}
					watchJob = null;
				}
			}
			fireContainerEvent(new ContainerDisconnectedEvent(this.getID(), anID));
		}
	}

	public class EtcdTTLJob extends Job {

		private static final int DELAY = 1000;
		private int ttl;

		public EtcdTTLJob(int ttl) {
			super("Etcd TTL Job"); //$NON-NLS-1$
			this.ttl = ttl;
		}

		private long getStartWaitTime() {
			long kttl = this.ttl * 1000;
			return kttl - (kttl / 6);
		}

		void trace(String message) {
			LogUtility.trace("run", DebugOptions.TTLJOB, EtcdTTLJob.class, message); //$NON-NLS-1$
		}

		@Override
		protected IStatus run(IProgressMonitor arg0) {
			trace("TTL Job starting"); //$NON-NLS-1$
			long waittime = getStartWaitTime();
			while (true) {
				if (arg0.isCanceled())
					return Status.CANCEL_STATUS;
				synchronized (this) {
					try {
						Thread.sleep(DELAY);
						if (arg0.isCanceled())
							return Status.CANCEL_STATUS;
						waittime -= DELAY;
						if (waittime <= 0) {
							try {
								new EtcdSetRequest(getDirectoryUrl() + EtcdDiscoveryContainer.this.localSessionId,
										this.ttl, true).execute();
							} catch (EtcdException e) {
								logEtcdError("TTLJob.run", "Exception sending ttl update", e); //$NON-NLS-1$//$NON-NLS-2$
							}
							waittime = getStartWaitTime();
						}
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}

	private void setNewEtcdIndex(EtcdNode node) {
		watchIndex = node.getModifiedIndex() + 1;
	}

	public class EtcdWatchJob extends Job {

		public EtcdWatchJob() {
			super("EtcdDiscovery Watch Job"); //$NON-NLS-1$
		}

		void trace(String message) {
			LogUtility.trace("run", DebugOptions.WATCHJOB, EtcdWatchJob.class, message); //$NON-NLS-1$
		}

		@Override
		protected IStatus run(IProgressMonitor monitor) {
			trace("Watch Job starting"); //$NON-NLS-1$
			while (!watchDone) {
				if (monitor.isCanceled())
					return Status.CANCEL_STATUS;
				String url = getDirectoryUrl();
				try {
					EtcdResponse response = new EtcdWatchRequest(url, Integer.toString(watchIndex)).execute();
					if (monitor.isCanceled())
						return Status.CANCEL_STATUS;
					if (etcdTargetID == null || localSessionId == null)
						return Status.CANCEL_STATUS;
					if (response.isError()) {
						logEtcdError("watchJobExec", "Etcd error response to watch request", //$NON-NLS-1$ //$NON-NLS-2$
								new EtcdException("Error response", response.getErrorResponse())); //$NON-NLS-1$
					} else {
						EtcdSuccessResponse success = response.getSuccessResponse();
						String action = success.getAction();
						EtcdNode node = success.getNode();
						if (node == null) {
							logEtcdError("handleEtcdWatchResponse", "node in response cannot be null", //$NON-NLS-1$ //$NON-NLS-2$
									new EtcdException("node cannot be null")); //$NON-NLS-1$
							continue;
						}
						String fullKey = node.getKey();
						String noPrefix = removeKeyPrefix(fullKey);
						if (action.equals(EtcdProtocol.ACTION_DELETE) && fullKey.endsWith(localSessionId)) {
							trace("watch job run. Thread loop is done"); //$NON-NLS-1$
							watchDone = true;
							continue;
						} else if (fullKey.endsWith(localSessionId) || noPrefix.startsWith(localSessionId)) {
							setNewEtcdIndex(node);
							continue;
						} else {
							handleEtcdWatchResponse(action, node);
							setNewEtcdIndex(node);
						}
					}
				} catch (EtcdException e) {
					logEtcdError("watchJob.run", "Unexpected exception in watch job", e); //$NON-NLS-1$ //$NON-NLS-2$
				}
			}
			trace("watch job exiting normally"); //$NON-NLS-1$
			return Status.OK_STATUS;
		}
	}

	private EtcdServiceInfoKey parseServiceInfoKey(String fullKey) {
		fullKey = removeKeyPrefix(fullKey);
		// Now split into sessionKey/serviceInfoKey
		int slashIndex = fullKey.lastIndexOf('/');
		if (slashIndex < 0)
			return null;
		String sessionKey = fullKey.substring(0, slashIndex);
		String siKey = fullKey.substring(slashIndex + 1);
		// Check to make sure sessionKey has UUID syntax
		try {
			UUID.fromString(sessionKey);
		} catch (IllegalArgumentException e) {
			return null;
		}
		try {
			UUID.fromString(siKey);
		} catch (IllegalArgumentException e) {
			return null;
		}
		return new EtcdServiceInfoKey(sessionKey, siKey);
	}

	private void handleRemoveNode(EtcdNode node) {
		trace("handleRemoveNode", "node=" + node); //$NON-NLS-1$ //$NON-NLS-2$
		EtcdServiceInfoKey key = parseServiceInfoKey(node.getKey());
		if (key != null) {
			EtcdServiceInfo si = null;
			synchronized (services) {
				si = services.remove(key);
			}
			if (si != null)
				fireServiceUndiscovered(key.getFullKey(), si);

		} else
			logEtcdError("handleRemoveNode", "Could not get EtcdServiceInfoKey for node=" + node); //$NON-NLS-1$ //$NON-NLS-2$
	}

	private void handleRemoveSession(String sessionKey) {
		if (sessionKey != null) {
			Map<EtcdServiceInfoKey, EtcdServiceInfo> removed = new HashMap<EtcdServiceInfoKey, EtcdServiceInfo>();
			synchronized (services) {
				for (Iterator<EtcdServiceInfoKey> it = services.keySet().iterator(); it.hasNext(); ) {
					EtcdServiceInfoKey key = it.next();
					if (key.matchSessionId(sessionKey)) {
						EtcdServiceInfo esi = services.get(key);
						if (esi != null)
							removed.put(key, esi);
						it.remove();
					}
				}
			}
			for (EtcdServiceInfoKey key : removed.keySet())
				fireServiceUndiscovered(key.getFullKey(), removed.get(key));
		} else
			logEtcdError("handleRemoveDirectory", "Could not remove sessionKey=" + sessionKey); //$NON-NLS-1$ //$NON-NLS-2$
	}

	private void handleRemoveDirectory(EtcdNode node) {
		trace("handleRemoveDirectory", "node=" + node); //$NON-NLS-1$ //$NON-NLS-2$
		handleRemoveSession(removeKeyPrefix(node.getKey()));
	}

	private void handleAddNode(EtcdNode node) {
		trace("handleAddNode", "node=" + node); //$NON-NLS-1$ //$NON-NLS-2$
		EtcdServiceInfoKey siKey = parseServiceInfoKey(node.getKey());
		if (siKey != null) {
			EtcdServiceInfo si = null;
			try {
				si = EtcdServiceInfo.deserializeFromString(node.getValue());
				synchronized (services) {
					services.put(siKey, si);
				}
				fireServiceTypeDiscovered(si.getServiceID().getServiceTypeID());
				fireServiceDiscovered(siKey.getFullKey(), si);
			} catch (JSONException e) {
				logEtcdError("handleEtcdServiceInfoAdd", "Error deserializing nodeValue for node=" + node, //$NON-NLS-1$ //$NON-NLS-2$
						new EtcdException(e));
			}
		} else
			logEtcdError("handleAddNode", "Could not get key "); //$NON-NLS-1$ //$NON-NLS-2$
	}

	private void handleAddDirectory(EtcdNode node) {
		trace("handleAddDirectory", "node=" + node); //$NON-NLS-1$ //$NON-NLS-2$
		EtcdNode[] entryNodes = node.getNodes();
		if (entryNodes != null)
			for (EtcdNode en : entryNodes)
				if (en.isDirectory())
					handleAddDirectory(en);
				else
					handleAddNode(en);
	}

	private void handleCreateAction(EtcdNode node) {
		trace("handleExpireAction", "node=" + node); //$NON-NLS-1$ //$NON-NLS-2$
		handleNodeOrDirectory(node, true);
	}

	private void handleDeleteAction(EtcdNode node) {
		trace("handleDeleteAction", "node=" + node); //$NON-NLS-1$ //$NON-NLS-2$
		handleNodeOrDirectory(node, false);
	}

	private void handleExpireAction(EtcdNode node) {
		trace("handleExpireAction", "node=" + node); //$NON-NLS-1$ //$NON-NLS-2$
		EtcdServiceInfoKey key = parseServiceInfoKey(node.getKey());
		if (node.isDirectory() || key == null) {
			handleRemoveDirectory(node);
		} else {
			handleRemoveNode(node);
		}
	}

	private void handleGetAction(EtcdNode node) {
		trace("handleGetAction", "node=" + node); //$NON-NLS-1$ //$NON-NLS-2$
		handleNodeOrDirectory(node, true);
	}

	private void handleKeyAction(EtcdNode node) {
		trace("handleKeyAction", "node=" + node); //$NON-NLS-1$ //$NON-NLS-2$
	}

	private void handleNodeOrDirectory(EtcdNode node, boolean add) {
		if (node.isDirectory()) {
			if (add)
				handleAddDirectory(node);
			else
				handleRemoveDirectory(node);
		} else {
			if (add)
				handleAddNode(node);
			else
				handleRemoveNode(node);
		}
	}

	private void handleSetAction(EtcdNode node) {
		trace("handleSetAction", "node=" + node); //$NON-NLS-1$ //$NON-NLS-2$
		handleNodeOrDirectory(node, true);
	}

	private void handleUnexpectedAction(String action, EtcdNode node) {
		// trace("handleUnexpectedAction", "action="+action+",node=" + node);
		// //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}

	private void handleEtcdWatchResponse(String action, EtcdNode node) {
		// trace("handleEtcdWatchResponse", "action=" + action + ",node=" + node);
		// //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		if (action.equals(EtcdProtocol.ACTION_CREATE))
			handleCreateAction(node);
		else if (action.equals(EtcdProtocol.ACTION_DELETE))
			handleDeleteAction(node);
		else if (action.equals(EtcdProtocol.ACTION_EXPIRE))
			handleExpireAction(node);
		else if (action.equals(EtcdProtocol.ACTION_GET))
			handleGetAction(node);
		else if (action.equals(EtcdProtocol.ACTION_KEY))
			handleKeyAction(node);
		else if (action.equals(EtcdProtocol.ACTION_SET))
			handleSetAction(node);
		else
			handleUnexpectedAction(action, node);
	}

	private void fireServiceUndiscovered(String key, IServiceInfo iinfo) {
		trace("fireServiceUndiscovered", "key=" + key + ",serviceInfo=" + iinfo); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		fireServiceUndiscovered(new ServiceContainerEvent(iinfo, getConfig().getID()));
	}

	private void fireServiceDiscovered(String key, IServiceInfo iinfo) {
		trace("fireServiceDiscovered", "key="+key+",serviceInfo=" + iinfo); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		fireServiceDiscovered(new ServiceContainerEvent(iinfo, getConfig().getID()));
	}

	private void fireServiceTypeDiscovered(IServiceTypeID serviceTypeID) {
		fireServiceTypeDiscovered(new ServiceTypeContainerEvent(serviceTypeID, getConfig().getID()));
	}

	private void trace(String methodName, String message) {
		LogUtility.trace(methodName, DebugOptions.DEBUG, getClass(), message);
	}

	private void logEtcdError(String method, String message, EtcdException e) {
		LogUtility.logError(method, DebugOptions.EXCEPTIONS_THROWING, getClass(), message, e);
	}

	private void logEtcdError(String method, String message) {
		logEtcdError(method, message, null);
	}

	private void logAndThrowEtcdError(String method, String message, EtcdException e) {
		logEtcdError(method, message, e);
		throw new RuntimeException(message, e);
	}

	public IServiceInfo getServiceInfo(IServiceID aServiceID) {
		synchronized (services) {
			for (EtcdServiceInfo info : services.values())
				if (info.getServiceID().equals(aServiceID))
					return info;
		}
		return null;
	}

	public IServiceInfo[] getServices() {
		return services.values().toArray(new IServiceInfo[services.size()]);
	}

	private Collection<EtcdServiceInfo> getLocalServices() {
		List<EtcdServiceInfo> results = new ArrayList<EtcdServiceInfo>();
		synchronized (services) {
			for (EtcdServiceInfoKey key : services.keySet())
				if (key.matchSessionId(this.localSessionId))
					results.add(services.get(key));
		}
		return results;
	}

	@Override
	public void unregisterAllServices() {
		synchronized (services) {
			Collection<EtcdServiceInfo> locallyPublished = getLocalServices();
			for (EtcdServiceInfo info : locallyPublished)
				unregisterService(info);
		}
	}

	public IServiceInfo[] getServices(IServiceTypeID aServiceTypeID) {
		List<IServiceInfo> results = new ArrayList<IServiceInfo>();
		synchronized (services) {
			for (EtcdServiceInfo info : services.values()) {
				IServiceTypeID stid = info.getServiceID().getServiceTypeID();
				if (stid.equals(aServiceTypeID))
					results.add(info);
			}
		}
		return results.toArray(new IServiceInfo[results.size()]);
	}

	public IServiceTypeID[] getServiceTypes() {
		Set<IServiceTypeID> results = new HashSet<IServiceTypeID>();
		synchronized (services) {
			for (EtcdServiceInfo info : services.values())
				results.add(info.getServiceID().getServiceTypeID());
		}
		return results.toArray(new IServiceTypeID[results.size()]);
	}

	private int convertLongTTLToIntTTL(long ttl) {
		return (ttl > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) ttl;
	}

	private EtcdServiceInfoKey findEtcdServiceInfoKey(IServiceID serviceID, boolean matchSessionId) {
		synchronized (services) {
			for (EtcdServiceInfoKey key : services.keySet()) {
				EtcdServiceInfo info = services.get(key);
				if (info.getServiceID().equals(serviceID) && key.matchSessionId(this.localSessionId))
					return key;
			}
		}
		return null;
	}

	private String verifySlash(String prefix) {
		if (!prefix.endsWith("/")) //$NON-NLS-1$
			return prefix + "/"; //$NON-NLS-1$
		else
			return prefix;
	}

}
