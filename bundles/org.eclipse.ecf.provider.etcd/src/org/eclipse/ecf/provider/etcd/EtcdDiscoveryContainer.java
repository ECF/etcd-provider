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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import org.eclipse.ecf.discovery.identity.IServiceID;
import org.eclipse.ecf.discovery.identity.IServiceTypeID;
import org.eclipse.ecf.internal.provider.etcd.DebugOptions;
import org.eclipse.ecf.internal.provider.etcd.LogUtility;
import org.eclipse.ecf.provider.etcd.identity.EtcdNamespace;
import org.eclipse.ecf.provider.etcd.identity.EtcdServiceID;
import org.eclipse.ecf.provider.etcd.protocol.EtcdDeleteRequest;
import org.eclipse.ecf.provider.etcd.protocol.EtcdErrorResponse;
import org.eclipse.ecf.provider.etcd.protocol.EtcdException;
import org.eclipse.ecf.provider.etcd.protocol.EtcdGetRequest;
import org.eclipse.ecf.provider.etcd.protocol.EtcdNode;
import org.eclipse.ecf.provider.etcd.protocol.EtcdProtocol;
import org.eclipse.ecf.provider.etcd.protocol.EtcdResponse;
import org.eclipse.ecf.provider.etcd.protocol.EtcdSetRequest;
import org.eclipse.ecf.provider.etcd.protocol.EtcdSuccessResponse;
import org.eclipse.ecf.provider.etcd.protocol.EtcdWatchRequest;

public class EtcdDiscoveryContainer extends AbstractDiscoveryContainerAdapter {

	private final Map<IServiceID, IServiceInfo> publishedServices = new HashMap<IServiceID, IServiceInfo>();

	private EtcdServiceID targetID;
	private String sessionId;

	public EtcdDiscoveryContainer(EtcdDiscoveryContainerConfig config) {
		super(EtcdNamespace.NAME, config);
	}

	public EtcdDiscoveryContainer() throws MalformedURLException,
			URISyntaxException {
		super(EtcdNamespace.NAME, new EtcdDiscoveryContainerConfig(
				EtcdDiscoveryContainer.class.getName()));
	}

	public IServiceInfo getServiceInfo(IServiceID aServiceID) {
		synchronized (publishedServices) {
			return publishedServices.get(aServiceID);
		}
	}

	public IServiceInfo[] getServices() {
		return publishedServices.values().toArray(
				new IServiceInfo[publishedServices.size()]);
	}

	public IServiceInfo[] getServices(IServiceTypeID aServiceTypeID) {
		List<IServiceInfo> results = new ArrayList<IServiceInfo>();
		synchronized (publishedServices) {
			for (IServiceID sid : publishedServices.keySet())
				if (sid.getServiceTypeID().equals(aServiceTypeID))
					results.add(publishedServices.get(sid));
		}
		return results.toArray(new IServiceInfo[results.size()]);
	}

	public IServiceTypeID[] getServiceTypes() {
		Set<IServiceTypeID> results = new HashSet<IServiceTypeID>();
		synchronized (publishedServices) {
			for (IServiceID sid : publishedServices.keySet())
				results.add(sid.getServiceTypeID());
		}
		return results.toArray(new IServiceTypeID[results.size()]);
	}

	public void registerService(IServiceInfo serviceInfo) {
		// TODO Auto-generated method stub

	}

	public void unregisterService(IServiceInfo serviceInfo) {
		// TODO Auto-generated method stub

	}

	@Override
	public String getContainerName() {
		return EtcdDiscoveryContainerInstantiator.NAME;
	}

	public synchronized void connect(ID aTargetID,
			IConnectContext connectContext) throws ContainerConnectException {
		if (targetID != null) {
			throw new ContainerConnectException("Already connected"); //$NON-NLS-1$
		}
		fireContainerEvent(new ContainerConnectingEvent(this.getID(),
				aTargetID, connectContext));
		EtcdDiscoveryContainerConfig config = (EtcdDiscoveryContainerConfig) getConfig();
		if (config == null) {
			throw new ContainerConnectException("Container has been disposed"); //$NON-NLS-1$
		}
		if (aTargetID == null) {
			targetID = config.getTargetID();
		} else {
			if (!(aTargetID instanceof EtcdServiceID))
				throw new ContainerConnectException(
						"targetID must be of type EtcdServiceID"); //$NON-NLS-1$
			targetID = (EtcdServiceID) aTargetID;
		}
		// Now set sessionId
		sessionId = config.getSessionId();
		if (sessionId == null)
			throw new ContainerConnectException("SessionId cannot be null"); //$NON-NLS-1$

		doConnect();
		fireContainerEvent(new ContainerConnectedEvent(this.getID(), aTargetID));
	}

	protected EtcdServiceID getTargetID() {
		return targetID;
	}

	private String verifySlash(String prefix) {
		if (!prefix.endsWith("/")) //$NON-NLS-1$
			return prefix + "/"; //$NON-NLS-1$
		else
			return prefix;
	}

	protected String getEtcdDirectoryURL() {
		// Make sure that the targetID has '/' suffix
		return verifySlash(verifySlash(getTargetID().getLocation().toString())
				+ getID().getName());
	}

	protected void trace(String methodName, String message) {
		LogUtility.trace(methodName, DebugOptions.DEBUG, getClass(), message);
	}

	protected void doConnect() throws ContainerConnectException {
		String directoryURL = getEtcdDirectoryURL();
		try {
			trace("doConnect", "checking for etcd directoryURL=" + directoryURL); //$NON-NLS-1$//$NON-NLS-2$
			EtcdResponse directoryExistsResponse = new EtcdGetRequest(
					getEtcdDirectoryURL(), false).execute();
			if (directoryExistsResponse.isError()) {
				trace("doConnect", "etcd directoryURL=" + directoryURL + " does not exist, attempting to create"); //$NON-NLS-1$//$NON-NLS-2$ //$NON-NLS-3$
				// Try to create directory
				directoryExistsResponse = new EtcdSetRequest(directoryURL)
						.execute();
				if (directoryExistsResponse.isError()) {
					trace("doConnect ERROR", "etcd directoryURL could not be created, throwing"); //$NON-NLS-1$ //$NON-NLS-2$
					throw new ContainerConnectException(
							"Could not create etcd directoryURL=" + directoryURL); //$NON-NLS-1$
				}
			}
			// Else the directory already exists or was successfully created
			trace("doConnect", "Directory exists=" + directoryURL); //$NON-NLS-1$ //$NON-NLS-2$
			initializeDiscoveryJob();
		} catch (EtcdException e) {
			throw new ContainerConnectException(
					"Exception communicating with etcd service at directoryURL=" + directoryURL, e); //$NON-NLS-1$
		}
	}

	public ID getConnectedID() {
		return targetID;
	}

	public synchronized void disconnect() {
		if (targetID != null) {
			ID anID = getConnectedID();
			fireContainerEvent(new ContainerDisconnectingEvent(this.getID(),
					anID));
			shutdownEtcdConnection();
			targetID = null;
			fireContainerEvent(new ContainerDisconnectedEvent(this.getID(),
					anID));
		}
	}

	private void shutdownEtcdConnection() {
		// If we haven't registered any service infos, then
		try {
			new EtcdSetRequest(getEtcdDirectoryURL()+this.sessionId, "close",30).execute(); //$NON-NLS-1$
		} catch (EtcdException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private EtcdDiscoveryJob discoveryJob;
	private boolean done;

	private synchronized void initializeDiscoveryJob() {
		if (discoveryJob == null) {
			discoveryJob = new EtcdDiscoveryJob();
			discoveryJob.schedule();
		}
	}

	protected String getWatchRequestURL() {
		return getEtcdDirectoryURL();
	}

	class EtcdDiscoveryJob extends Job {

		public EtcdDiscoveryJob() {
			super("Etcd Discovery"); //$NON-NLS-1$
		}

		private String etcdIndex;

		private void computeNewEtcdIndex(EtcdSuccessResponse response) {
			int modifiedIndex = response.getNode().getModifiedIndex();
			modifiedIndex += 1;
			this.etcdIndex = Integer.toString(modifiedIndex);
		}

		void trace(String message) {
			LogUtility.trace("run", DebugOptions.WATCHJOB, EtcdDiscoveryJob.class, message); //$NON-NLS-1$
		}
		
		@Override
		protected IStatus run(IProgressMonitor monitor) {
			trace("watch job starting"); //$NON-NLS-1$
			while (!done) {
				if (monitor.isCanceled())
					return Status.CANCEL_STATUS;
				String url = getWatchRequestURL();
				try {
					EtcdResponse response = (etcdIndex == null) ? new EtcdGetRequest(
							url, false).execute() : new EtcdWatchRequest(url,
							etcdIndex).execute();
					if (monitor.isCanceled())
						return Status.CANCEL_STATUS;
					if (targetID == null || sessionId == null)
						return Status.CANCEL_STATUS;
					System.out.println(response);
					if (response.isError()) {
						EtcdErrorResponse error = response.getError();
						// XXX todo handle error response
					} else {
						EtcdSuccessResponse success = response.getResponse();
						EtcdNode node = success.getNode();
						String action = success.getAction();
						String key = node.getKey();
						System.out.println("key="+key); //$NON-NLS-1$
						if (action.equals("set") && key.endsWith(sessionId)) { //$NON-NLS-1$
							// We have requested close
							trace("key="+key+" matched sessionId="+sessionId+". Thread loop is done"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
							done = true;
						} else {
							computeNewEtcdIndex(success);

							// XXX todo handle success response							
						}
					}
				} catch (EtcdException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			trace("watch job exiting normally"); //$NON-NLS-1$
			return Status.OK_STATUS;
		}
	}

}
