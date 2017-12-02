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
			this.sessId = sessionId;
			this.serviceInfoId = UUID.randomUUID().toString();
			this.fullKey = verifySlash(sessionId) + this.serviceInfoId;
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

	// our published services
	private final Map<EtcdServiceInfoKey, EtcdServiceInfo> publishedServices = new HashMap<EtcdServiceInfoKey, EtcdServiceInfo>();
	// The targetID
	private EtcdServiceID targetID;
	private String sessionId;
	private String keyPrefix;

	private String dirUrl;
	private EtcdDiscoveryJob discoveryJob;
	private TTLJob ttlJob;
	private boolean watchDone;
	private int etcdIndex;

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
				: new EtcdServiceInfoKey(this.sessionId, endpointid);
		int etcdTTL = convertLongTTLToIntTTL(si.getTTL());
		String siString = null;
		try {
			siString = si.serializeToJsonString();
		} catch (JSONException e) {
			throw new IllegalArgumentException("Exception serializing serviceInfo=" + si, e); //$NON-NLS-1$
		}
		String fullKey = createFullKey(siKey);
		executeEtcdRequest("registerService", new EtcdSetRequest(fullKey, siString, etcdTTL), //$NON-NLS-1$
				"Error in EtcdServiceInfo set request serviceInfo=" + si); //$NON-NLS-1$
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
		executeEtcdRequest("unregisterService", new EtcdDeleteRequest(fullKey), //$NON-NLS-1$
				"Error in EtcdDeleteRequest with serviceInfo=" + serviceInfo); //$NON-NLS-1$
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

	public synchronized void connect(ID aTargetID, IConnectContext connectContext) throws ContainerConnectException {
		if (this.targetID != null)
			throw new ContainerConnectException("Already connected"); //$NON-NLS-1$
		EtcdDiscoveryContainerConfig config = (EtcdDiscoveryContainerConfig) getConfig();
		if (config == null)
			throw new ContainerConnectException("Container has been disposed"); //$NON-NLS-1$

		fireContainerEvent(new ContainerConnectingEvent(getID(), aTargetID, connectContext));

		// set targetID from config
		if (aTargetID == null) {
			targetID = config.getTargetID();
		} else {
			if (!(aTargetID instanceof EtcdServiceID))
				throw new ContainerConnectException("targetID must be of type EtcdServiceID"); //$NON-NLS-1$
			targetID = (EtcdServiceID) aTargetID;
		}
		trace("connect", "targetID=" + this.targetID); //$NON-NLS-1$ //$NON-NLS-2$
		// Set sessionId from config
		sessionId = config.getSessionId();
		if (sessionId == null)
			throw new ContainerConnectException("SessionId cannot be null"); //$NON-NLS-1$
		this.keyPrefix = verifySlash("/" + getID().getName()); //$NON-NLS-1$
		// Then set directory URL
		this.dirUrl = this.targetID.getLocation().toString() + this.keyPrefix;
		// Do a get request on directoryUrl to verify that the EtcdDiscoveryContainer
		// location
		// is present
		try {
			EtcdResponse topResponse = new EtcdGetRequest(getDirectoryUrl(), true).execute();
			if (topResponse.isError()) {
				trace("doConnect", "etcd directoryURL=" + getDirectoryUrl() + " does not exist, attempting to create"); //$NON-NLS-1$//$NON-NLS-2$ //$NON-NLS-3$
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
			this.etcdIndex = node.getCreatedIndex();
			int sessionTTL = config.getSessionTTL();
			// Now create a directory with our unique sessionid
			EtcdResponse sessionExistsResponse = new EtcdSetRequest(getDirectoryUrl() + this.sessionId, sessionTTL)
					.execute();
			if (sessionExistsResponse.isError())
				throw new ContainerConnectException("Could not create etcd session directory"); //$NON-NLS-1$

			handleAddDirectory(node);
			this.etcdIndex = sessionExistsResponse.getSuccessResponse().getNode().getCreatedIndex() + 1;
			// Else the directory already exists or was successfully created
			trace("connect", "directoryUrl=" + getDirectoryUrl() + this.sessionId); //$NON-NLS-1$ //$NON-NLS-2$
			ttlJob = new TTLJob(sessionTTL);
			ttlJob.schedule();
			// Now setup and start discovery job
			discoveryJob = new EtcdDiscoveryJob();
			discoveryJob.schedule();
		} catch (EtcdException e) {
			throw new ContainerConnectException(
					"Exception communicating with etcd service at directoryURL=" + getDirectoryUrl(), e); //$NON-NLS-1$
		}
		// Fire container connected event
		fireContainerEvent(new ContainerConnectedEvent(this.getID(), aTargetID));
	}

	public ID getConnectedID() {
		return targetID;
	}

	public synchronized void disconnect() {
		if (targetID != null) {
			ID anID = getConnectedID();
			fireContainerEvent(new ContainerDisconnectingEvent(this.getID(), anID));
			if (ttlJob != null) {
				ttlJob.cancel();
				ttlJob = null;
			}
			// If we haven't registered any service infos, then
			try {
				new EtcdDeleteRequest(getDirectoryUrl() + this.sessionId, true).execute();
				Thread.sleep(200);
			} catch (EtcdException e) {
				logEtcdError("shutdownEtcdConnection", "Error with etcd shutdown", e); //$NON-NLS-1$ //$NON-NLS-2$
			} catch (InterruptedException e) {
				// Nothing to do
			}
			targetID = null;
			sessionId = null;
			keyPrefix = null;
			dirUrl = null;
			fireContainerEvent(new ContainerDisconnectedEvent(this.getID(), anID));
		}
	}

	public class TTLJob extends Job {
		
		private static final int DELAY = 1000;
		private int ttl;
		
		public TTLJob(int ttl) {
			super("Etcd TTL Job"); //$NON-NLS-1$
			this.ttl = ttl;
		}

		void trace(String message) {
			LogUtility.trace("run", DebugOptions.TTLJOB, EtcdDiscoveryJob.class, message); //$NON-NLS-1$
		}

		@Override
		protected IStatus run(IProgressMonitor arg0) {
			trace("TTL Job starting"); //$NON-NLS-1$
			long waittime = (this.ttl * 1000) - 2000;
			while (true) {
				if (arg0.isCanceled()) return Status.CANCEL_STATUS;
				synchronized (this) {
					try {
						Thread.sleep(DELAY);
						if (arg0.isCanceled()) return Status.CANCEL_STATUS;
						waittime -= DELAY;
						if (waittime <= 0) {
							try {
								new EtcdSetRequest(getDirectoryUrl() + EtcdDiscoveryContainer.this.sessionId, this.ttl, true).execute();
							} catch (EtcdException e) {
								logEtcdError("TTLJob.run","Exception sending ttl update",e);  //$NON-NLS-1$//$NON-NLS-2$
							}
							waittime = this.ttl * 1000;
						} else 
							trace("waittime="+waittime); //$NON-NLS-1$
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				wakeUp(DELAY);
			}
		}
	}
	
	public class EtcdDiscoveryJob extends Job {

		public EtcdDiscoveryJob() {
			super("Etcd Watch Job"); //$NON-NLS-1$
		}

		private void computeNewEtcdIndex(EtcdNode node) {
			int modifiedIndex = node.getModifiedIndex();
			modifiedIndex += 1;
			etcdIndex = modifiedIndex;
		}

		void trace(String message) {
			LogUtility.trace("run", DebugOptions.WATCHJOB, EtcdDiscoveryJob.class, message); //$NON-NLS-1$
		}

		@Override
		protected IStatus run(IProgressMonitor monitor) {
			trace("watch job starting"); //$NON-NLS-1$
			while (!watchDone) {
				if (monitor.isCanceled())
					return Status.CANCEL_STATUS;
				String url = getDirectoryUrl();
				try {
					EtcdResponse response = new EtcdWatchRequest(url, Integer.toString(etcdIndex)).execute();
					if (monitor.isCanceled())
						return Status.CANCEL_STATUS;
					if (targetID == null || sessionId == null)
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
						// We will get a delete that ends with our session id
						if (action.equals(EtcdProtocol.ACTION_DELETE) && fullKey.endsWith(sessionId)) {
							// We have requested close
							trace("fullKey=" + fullKey + " matched sessionId=" + sessionId + ". Thread loop is done"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
							watchDone = true;
						} else {
							handleEtcdWatchResponse(action, node);
							computeNewEtcdIndex(node);
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
			synchronized (publishedServices) {
				si = publishedServices.remove(key);
				if (si != null)
					fireServiceUndiscovered(si);
			}
		} else
			logEtcdError("handleRemoveNode", "Could not get EtcdServiceInfoKey for node=" + node); //$NON-NLS-1$ //$NON-NLS-2$
	}

	private void handleRemoveDirectory(EtcdNode node) {
		trace("handleRemoveDirectory", "node=" + node); //$NON-NLS-1$ //$NON-NLS-2$
		String sessionKey = removeKeyPrefix(node.getKey());
		if (sessionKey != null) {
			List<EtcdServiceInfo> removed = new ArrayList<EtcdServiceInfo>();
			synchronized (publishedServices) {
				for (EtcdServiceInfoKey key : publishedServices.keySet()) {
					if (key.matchSessionId(sessionId)) {
						EtcdServiceInfo esi = publishedServices.remove(key);
						if (esi != null)
							removed.add(esi);
					}
				}
			}
			removed.forEach(si -> fireServiceUndiscovered(si));
		} else
			logEtcdError("handleRemoveDirectory", "Could not get sessionKey for node=" + node); //$NON-NLS-1$ //$NON-NLS-2$
	}

	private void handleAddNode(EtcdNode node) {
		trace("handleAddNode", "node=" + node); //$NON-NLS-1$ //$NON-NLS-2$
		EtcdServiceInfoKey siKey = parseServiceInfoKey(node.getKey());
		if (siKey != null) {
			EtcdServiceInfo si = null;
			try {
				si = EtcdServiceInfo.deserializeFromString(node.getValue());
				synchronized (publishedServices) {
					publishedServices.put(siKey, si);
				}
				fireServiceTypeDiscovered(si.getServiceID().getServiceTypeID());
				fireServiceDiscovered(si);
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
				handleAddNode(en);
	}

	private void handleCreateAction(EtcdNode node) {
		trace("handleExpireAction", "node=" + node); //$NON-NLS-1$ //$NON-NLS-2$
		if (node.isDirectory()) {
			handleAddDirectory(node);
		} else {
			handleAddNode(node);
		}
	}

	private void handleDeleteAction(EtcdNode node) {
		trace("handleDeleteAction", "node=" + node); //$NON-NLS-1$ //$NON-NLS-2$
		if (node.isDirectory()) {
			handleRemoveDirectory(node);
		} else {
			handleRemoveNode(node);
		}
	}

	private void handleExpireAction(EtcdNode node) {
		trace("handleExpireAction", "node=" + node); //$NON-NLS-1$ //$NON-NLS-2$
		if (node.isDirectory()) {
			handleRemoveDirectory(node);
		} else {
			handleRemoveNode(node);
		}
	}

	private void handleGetAction(EtcdNode node) {
		trace("handleGetAction", "node=" + node); //$NON-NLS-1$ //$NON-NLS-2$
		if (node.isDirectory()) {
			handleAddDirectory(node);
		} else {
			handleAddNode(node);
		}
	}

	private void handleKeyAction(EtcdNode node) {
		trace("handleKeyAction", "node=" + node); //$NON-NLS-1$ //$NON-NLS-2$
	}

	private void handleSetAction(EtcdNode node) {
		trace("handleSetAction", "node=" + node); //$NON-NLS-1$ //$NON-NLS-2$
		if (node.isDirectory()) {
			handleAddDirectory(node);
		} else {
			handleAddNode(node);
		}
	}

	private void handleUnexpectedAction(String action, EtcdNode node) {
		trace("handleUnexpectedAction", "action="+action+",node=" + node); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}

	private void handleEtcdWatchResponse(String action, EtcdNode node) {
		trace("handleEtcdWatchResponse", "action=" + action + ",node=" + node); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
																				// //$NON-NLS-4$
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
			handleUnexpectedAction(action,node);
	}

	private void fireServiceUndiscovered(IServiceInfo iinfo) {
		fireServiceUndiscovered(new ServiceContainerEvent(iinfo, getConfig().getID()));
	}

	private void fireServiceDiscovered(IServiceInfo iinfo) {
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
		synchronized (publishedServices) {
			for (EtcdServiceInfo info : publishedServices.values())
				if (info.getServiceID().equals(aServiceID))
					return info;
		}
		return null;
	}

	public IServiceInfo[] getServices() {
		return publishedServices.values().toArray(new IServiceInfo[publishedServices.size()]);
	}

	private Collection<EtcdServiceInfo> getLocalServices() {
		List<EtcdServiceInfo> results = new ArrayList<EtcdServiceInfo>();
		synchronized (publishedServices) {
			for (EtcdServiceInfoKey key : publishedServices.keySet())
				if (key.matchSessionId(this.sessionId))
					results.add(publishedServices.get(key));
		}
		return results;
	}

	@Override
	public void unregisterAllServices() {
		synchronized (publishedServices) {
			Collection<EtcdServiceInfo> locallyPublished = getLocalServices();
			for (EtcdServiceInfo info : locallyPublished)
				unregisterService(info);
		}
	}

	public IServiceInfo[] getServices(IServiceTypeID aServiceTypeID) {
		List<IServiceInfo> results = new ArrayList<IServiceInfo>();
		synchronized (publishedServices) {
			for (EtcdServiceInfo info : publishedServices.values()) {
				IServiceTypeID stid = info.getServiceID().getServiceTypeID();
				if (stid.equals(aServiceTypeID))
					results.add(info);
			}
		}
		return results.toArray(new IServiceInfo[results.size()]);
	}

	public IServiceTypeID[] getServiceTypes() {
		Set<IServiceTypeID> results = new HashSet<IServiceTypeID>();
		synchronized (publishedServices) {
			for (EtcdServiceInfo info : publishedServices.values())
				results.add(info.getServiceID().getServiceTypeID());
		}
		return results.toArray(new IServiceTypeID[results.size()]);
	}

	private int convertLongTTLToIntTTL(long ttl) {
		return (ttl > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) ttl;
	}

	private EtcdServiceInfoKey findEtcdServiceInfoKey(IServiceID serviceID, boolean matchSessionId) {
		synchronized (publishedServices) {
			for (EtcdServiceInfoKey key : publishedServices.keySet()) {
				EtcdServiceInfo info = publishedServices.get(key);
				if (info.getServiceID().equals(serviceID) && key.matchSessionId(this.sessionId))
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
