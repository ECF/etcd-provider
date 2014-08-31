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

import org.eclipse.core.runtime.Assert;
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
			this.fullKey = sessionId + "." + this.serviceInfoId; //$NON-NLS-1$			
		}

		public EtcdServiceInfoKey(String sessionId, String serviceInfoId) {
			this.sessId = sessionId;
			this.serviceInfoId = serviceInfoId;
			this.fullKey = sessionId + "." + this.serviceInfoId; //$NON-NLS-1$
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

	// All published services
	protected final Map<EtcdServiceInfoKey, EtcdServiceInfo> publishedServices = new HashMap<EtcdServiceInfoKey, EtcdServiceInfo>();
	// The targetID
	protected EtcdServiceID targetID;
	protected String sessionId;
	protected String directoryUrl;

	public EtcdDiscoveryContainer(EtcdDiscoveryContainerConfig config) {
		super(EtcdNamespace.NAME, config);
	}

	public EtcdDiscoveryContainer() throws MalformedURLException,
			URISyntaxException {
		super(EtcdNamespace.NAME, new EtcdDiscoveryContainerConfig(
				EtcdDiscoveryContainer.class.getName()));
	}

	public void registerService(IServiceInfo serviceInfo) {
		trace("registerService", "serviceInfo=" + serviceInfo); //$NON-NLS-1$ //$NON-NLS-2$
		EtcdServiceInfo si = (serviceInfo instanceof EtcdServiceInfo) ? (EtcdServiceInfo) serviceInfo
				: new EtcdServiceInfo(serviceInfo);
		EtcdServiceInfoKey siKey = new EtcdServiceInfoKey();
		int etcdTTL = convertLongTTLToIntTTL(si.getTTL());
		String siString = null;
		try {
			siString = si.serializeToJsonString();
		} catch (JSONException e) {
			throw new IllegalArgumentException(
					"Exception serializing serviceInfo=" + si, e); //$NON-NLS-1$
		}
		String fullKey = createFullKey(siKey);
		executeEtcdRequest(
				"registerService", new EtcdSetRequest(fullKey, siString, etcdTTL), "Error in EtcdServiceInfo set request serviceInfo=" + si); //$NON-NLS-1$ //$NON-NLS-2$
	}

	protected String createFullKey(EtcdServiceInfoKey key) {
		return this.directoryUrl + key.getFullKey();
	}

	public void unregisterService(IServiceInfo serviceInfo) {
		trace("unregisterService", "serviceInfo=" + serviceInfo); //$NON-NLS-1$ //$NON-NLS-2$
		EtcdServiceInfoKey key = findEtcdServiceInfoKey(
				serviceInfo.getServiceID(), true);
		if (key == null) {
			logEtcdError(
					"unregisterService", "Could not find serviceInfo=" + serviceInfo, null); //$NON-NLS-1$ //$NON-NLS-2$
			return;
		}
		String fullKey = createFullKey(key);
		executeEtcdRequest(
				"unregisterService", new EtcdDeleteRequest(fullKey), "Error in EtcdDeleteRequest with serviceInfo=" + serviceInfo); //$NON-NLS-1$ //$NON-NLS-2$
	}

	protected void executeEtcdRequest(String methodName, EtcdRequest request,
			String exceptionMessage) {
		try {
			EtcdResponse response = request.execute();
			if (response.isError())
				throw new EtcdException(exceptionMessage, response.getErrorResponse());
		} catch (EtcdException e) {
			logAndThrowEtcdError(methodName,
					"Error communicating with etcd server", e); //$NON-NLS-1$ 
		}
	}

	@Override
	public String getContainerName() {
		return EtcdDiscoveryContainerInstantiator.NAME;
	}

	public synchronized void connect(ID aTargetID,
			IConnectContext connectContext) throws ContainerConnectException {

		if (this.targetID != null)
			throw new ContainerConnectException("Already connected"); //$NON-NLS-1$
		EtcdDiscoveryContainerConfig config = (EtcdDiscoveryContainerConfig) getConfig();
		if (config == null)
			throw new ContainerConnectException("Container has been disposed"); //$NON-NLS-1$

		fireContainerEvent(new ContainerConnectingEvent(getID(), aTargetID,
				connectContext));

		// set targetID from config
		if (aTargetID == null) {
			targetID = config.getTargetID();
		} else {
			if (!(aTargetID instanceof EtcdServiceID))
				throw new ContainerConnectException(
						"targetID must be of type EtcdServiceID"); //$NON-NLS-1$
			targetID = (EtcdServiceID) aTargetID;
		}
		trace("connect", "targetID=" + this.targetID); //$NON-NLS-1$ //$NON-NLS-2$
		
		// Set sessionId from config
		sessionId = config.getSessionId();
		if (sessionId == null)
			throw new ContainerConnectException("SessionId cannot be null"); //$NON-NLS-1$
		trace("connect", "sessionId=" + this.sessionId); //$NON-NLS-1$ //$NON-NLS-2$
		
		// Then set directory URL
		this.directoryUrl = verifySlash(verifySlash(this.targetID.getLocation()
				.toString()) + getID().getName());
		trace("connect", "directoryUrl=" + this.directoryUrl); //$NON-NLS-1$ //$NON-NLS-2$

		// Do a get request on directoryUrl to verify that the EtcdDiscoveryContainer location
		// is present on server (and that server is there)
		try {
			EtcdResponse directoryExistsResponse = new EtcdGetRequest(
					this.directoryUrl, false).execute();
			if (directoryExistsResponse.isError()) {
				trace("doConnect", "etcd directoryURL=" + this.directoryUrl + " does not exist, attempting to create"); //$NON-NLS-1$//$NON-NLS-2$ //$NON-NLS-3$
				// Try to create directory
				directoryExistsResponse = new EtcdSetRequest(this.directoryUrl)
						.execute();
				if (directoryExistsResponse.isError()) {
					trace("doConnect ERROR", "etcd directoryURL could not be created, throwing"); //$NON-NLS-1$ //$NON-NLS-2$
					throw new ContainerConnectException(
							"Could not create etcd directoryURL=" + this.directoryUrl); //$NON-NLS-1$
				}
			}
			// Else the directory already exists or was successfully created
			trace("connect", "Directory exists=" + this.directoryUrl); //$NON-NLS-1$ //$NON-NLS-2$
			// Now setup and start discovery job
			if (discoveryJob == null) {
				discoveryJob = new EtcdDiscoveryJob();
				discoveryJob.schedule();
			}
		} catch (EtcdException e) {
			throw new ContainerConnectException(
					"Exception communicating with etcd service at directoryURL=" + this.directoryUrl, e); //$NON-NLS-1$
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
			fireContainerEvent(new ContainerDisconnectingEvent(this.getID(),
					anID));
			// If we haven't registered any service infos, then
			try {
				EtcdResponse response = new EtcdSetRequest(
						this.directoryUrl + this.sessionId,
						"c", ((EtcdDiscoveryContainerConfig) getConfig()).getCloseTTL()).execute(); //$NON-NLS-1$
				if (response.isError())
					throw new EtcdException(
							"Error in shudownEtcdConnection", response.getErrorResponse()); //$NON-NLS-1$
			} catch (EtcdException e) {
				logEtcdError(
						"shutdownEtcdConnection", "Error with etcd shutdown", e); //$NON-NLS-1$ //$NON-NLS-2$
			}
			targetID = null;
			fireContainerEvent(new ContainerDisconnectedEvent(this.getID(),
					anID));
		}
	}

	protected EtcdDiscoveryJob discoveryJob;
	protected boolean done;

	public class EtcdDiscoveryJob extends Job {

		public EtcdDiscoveryJob() {
			super("Etcd Discovery"); //$NON-NLS-1$
		}

		private String etcdIndex;

		private void computeNewEtcdIndex(EtcdNode node) {
			int modifiedIndex = node.getModifiedIndex();
			modifiedIndex += 1;
			this.etcdIndex = Integer.toString(modifiedIndex);
		}

		void trace(String message) {
			LogUtility
					.trace("run", DebugOptions.WATCHJOB, EtcdDiscoveryJob.class, message); //$NON-NLS-1$
		}

		@Override
		protected IStatus run(IProgressMonitor monitor) {
			trace("watch job starting"); //$NON-NLS-1$
			while (!done) {
				if (monitor.isCanceled())
					return Status.CANCEL_STATUS;
				String url = EtcdDiscoveryContainer.this.directoryUrl;
				try {
					EtcdResponse response = (etcdIndex == null) ? new EtcdGetRequest(
							url, false).execute() : new EtcdWatchRequest(url,
							etcdIndex).execute();
					if (monitor.isCanceled())
						return Status.CANCEL_STATUS;
					if (targetID == null || sessionId == null)
						return Status.CANCEL_STATUS;
					if (response.isError()) {
						logEtcdError(
								"watchJobExec", "Etcd error response to watch request", new EtcdException("Error response", response.getErrorResponse())); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
					} else {
						EtcdSuccessResponse success = response.getSuccessResponse();
						String action = success.getAction();
						if (action == null) {
							logEtcdError(
									"handleEtcdWatchResponse", "Action in response cannot be null", new EtcdException("action cannot be null")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
							continue;
						}
						EtcdNode node = success.getNode();
						if (node == null) {
							logEtcdError(
									"handleEtcdWatchResponse", "node in response cannot be null", new EtcdException("node cannot be null")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
							continue;
						}
						String fullKey = node.getKey();
						if (action.equals(EtcdProtocol.ACTION_SET)
								&& fullKey.endsWith(sessionId)) {
							// We have requested close
							trace("fullKey=" + fullKey + " matched sessionId=" + sessionId + ". Thread loop is done"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
							done = true;
						} else {
							handleEtcdWatchResponse(action, fullKey, node);
							computeNewEtcdIndex(node);
						}
					}
				} catch (EtcdException e) {
					logEtcdError(
							"watchJob.run", "Unexpected exception in watch job", e); //$NON-NLS-1$ //$NON-NLS-2$
				}
			}
			trace("watch job exiting normally"); //$NON-NLS-1$
			return Status.OK_STATUS;
		}
	}

	protected EtcdServiceInfoKey parseServiceInfoKey(String fullKey) {
		// first strip off everything before final '/'
		int lastSlashIndex = fullKey.lastIndexOf('/');
		if (lastSlashIndex <= 0 || lastSlashIndex >= fullKey.length())
			return null;
		String serviceInfoKey = fullKey.substring(lastSlashIndex + 1);
		if (serviceInfoKey == null)
			return null;
		// Now split into sessionKey.serviceInfoKey
		int dotIndex = serviceInfoKey.lastIndexOf('.');
		if (dotIndex < 0)
			return null;
		String sessionKey = serviceInfoKey.substring(0, dotIndex);
		String siKey = serviceInfoKey.substring(dotIndex + 1);
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

	protected void handleEtcdWatchResponse(String action, String fullKey,
			EtcdNode node) {
		trace("handleEtcdWatchResponse", "action=" + action + ",fullKey=" + fullKey + ",node=" + node); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
		EtcdServiceInfoKey siKey = parseServiceInfoKey(fullKey);
		if (siKey == null) {
			trace("handleEtcdServiceInfoDelete", "Could not parse fullKey=" + fullKey); //$NON-NLS-1$ //$NON-NLS-2$
			return;
		}
		if (action.equals(EtcdProtocol.ACTION_UPDATE)
				|| action.equals(EtcdProtocol.ACTION_GET)
				|| action.equals(EtcdProtocol.ACTION_SET)
				|| action.equals(EtcdProtocol.ACTION_CREATE)
				|| action.equals(EtcdProtocol.ACTION_COMPARE_AND_SWAP))
			handleEtcdServiceInfoAdd(action, siKey, node);
		else if (action.equals(EtcdProtocol.ACTION_DELETE)
				|| action.equals(EtcdProtocol.ACTION_EXPIRE)
				|| action.equals(EtcdProtocol.ACTION_COMPARE_AND_DELETE))
			handleEtcdServiceInfoDelete(action, siKey, node);
	}

	protected void handleEtcdServiceInfoDelete(String action,
			EtcdServiceInfoKey key, EtcdNode node) {
		// Now we actually do the delete. First create a EtcdServiceInfoKey
		EtcdServiceInfo si = null;
		synchronized (publishedServices) {
			si = publishedServices.remove(key);
		}
		if (si != null)
			fireServiceUndiscovered(si);
	}

	protected void fireServiceUndiscovered(IServiceInfo iinfo) {
		Assert.isNotNull(iinfo);
		if (getConfig() != null)
			fireServiceUndiscovered(new ServiceContainerEvent(iinfo,
					getConfig().getID()));
		else
			trace("fireServiceUndiscovered", "This IContainer is already disposed thus shouldn't fire events anymore"); //$NON-NLS-1$ //$NON-NLS-2$
	}

	protected void fireServiceDiscovered(IServiceInfo iinfo) {
		Assert.isNotNull(iinfo);
		if (getConfig() != null) {
			fireServiceDiscovered(new ServiceContainerEvent(iinfo, getConfig()
					.getID()));
		} else 
			trace("fireServiceDiscovered(IServiceInfo iinfo)", "This IContainer is already disposed thus shouldn't fire events anymore"); //$NON-NLS-1$ //$NON-NLS-2$
	}

	protected void fireServiceTypeDiscovered(IServiceTypeID serviceTypeID) {
		Assert.isNotNull(serviceTypeID);
		if (getConfig() != null) {
			fireServiceTypeDiscovered(new ServiceTypeContainerEvent(
					serviceTypeID, getConfig().getID()));
		} else 
			trace("fireServiceTypeDiscovered(IServiceInfo iinfo)", "This IContainer is already disposed thus shouldn't fire events anymore"); //$NON-NLS-1$ //$NON-NLS-2$
	}

	protected void handleEtcdServiceInfoAdd(String action,
			EtcdServiceInfoKey key, EtcdNode node) {
		String nodeValue = node.getValue();
		EtcdServiceInfo si = null;
		try {
			si = EtcdServiceInfo.deserializeFromString(nodeValue);
		} catch (JSONException e) {
			logEtcdError(
					"handleEtcdServiceInfoAdd", "Error deserializing nodeValue for node=" + node, new EtcdException(e)); //$NON-NLS-1$ //$NON-NLS-2$
		}
		synchronized (publishedServices) {
			publishedServices.put(key, si);
		}
		fireServiceTypeDiscovered(si.getServiceID().getServiceTypeID());
		fireServiceDiscovered(si);
	}

	protected void trace(String methodName, String message) {
		LogUtility.trace(methodName, DebugOptions.DEBUG, getClass(), message);
	}

	protected void logEtcdError(String method, String message, EtcdException e) {
		LogUtility.logError(method, DebugOptions.EXCEPTIONS_THROWING,
				getClass(), message, e);
	}

	protected void logAndThrowEtcdError(String method, String message,
			EtcdException e) {
		logEtcdError(method, message, e);
		throw new RuntimeException(e);
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
		return publishedServices.values().toArray(
				new IServiceInfo[publishedServices.size()]);
	}

	protected Collection<EtcdServiceInfo> getLocalServices() {
		List<EtcdServiceInfo> results = new ArrayList<EtcdServiceInfo>();
		synchronized (publishedServices) {
			for(EtcdServiceInfoKey key: publishedServices.keySet()) 
				if (key.matchSessionId(this.sessionId))
					results.add(publishedServices.get(key));
		}
		return results;
	}
	
	@Override
	public void unregisterAllServices() {
		synchronized (publishedServices) {
			Collection<EtcdServiceInfo> locallyPublished = getLocalServices();
			for(EtcdServiceInfo info: locallyPublished) 
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

	private EtcdServiceInfoKey findEtcdServiceInfoKey(IServiceID serviceID,
			boolean matchSessionId) {
		synchronized (publishedServices) {
			for (EtcdServiceInfoKey key : publishedServices.keySet()) {
				EtcdServiceInfo info = publishedServices.get(key);
				if (info.getServiceID().equals(serviceID)
						&& key.matchSessionId(this.sessionId))
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
