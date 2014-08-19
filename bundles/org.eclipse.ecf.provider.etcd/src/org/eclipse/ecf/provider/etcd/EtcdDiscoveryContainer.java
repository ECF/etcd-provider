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
import org.eclipse.ecf.provider.etcd.identity.EtcdNamespace;
import org.eclipse.ecf.provider.etcd.identity.EtcdServiceID;
import org.eclipse.ecf.provider.etcd.protocol.EtcdException;
import org.eclipse.ecf.provider.etcd.protocol.EtcdGetRequest;
import org.eclipse.ecf.provider.etcd.protocol.EtcdResponse;
import org.eclipse.ecf.provider.etcd.protocol.EtcdSetRequest;

public class EtcdDiscoveryContainer extends AbstractDiscoveryContainerAdapter {

	private final Map<IServiceID, IServiceInfo> publishedServices = new HashMap<IServiceID, IServiceInfo>();

	private EtcdServiceID targetID;

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

	protected void trace(String method, String message) {
		// XXX todo
		System.out.println(method+":"+message); //$NON-NLS-1$
	}
	
	public void connect(ID aTargetID, IConnectContext connectContext)
			throws ContainerConnectException {
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
		doConnect();
		fireContainerEvent(new ContainerConnectedEvent(this.getID(), aTargetID));
	}
	
	protected EtcdServiceID getTargetID() {
		return targetID;
	}

	protected String getEtcdDirectoryURL() throws ContainerConnectException {
		String urlPrefix = getTargetID().getLocation().toString();
		// Make sure that the targetID has '/' suffix
		if (!urlPrefix.endsWith("/")) //$NON-NLS-1$
			urlPrefix = urlPrefix + "/"; //$NON-NLS-1$
		return urlPrefix + getID().getName();
	}
	
	private void doConnect() throws ContainerConnectException {
		String directoryURL = getEtcdDirectoryURL();
		try {
			trace("doConnect","checking for etcd directoryURL="+directoryURL);  //$NON-NLS-1$//$NON-NLS-2$
			EtcdResponse directoryExistsResponse = new EtcdGetRequest(getEtcdDirectoryURL(),false).execute();
			if (directoryExistsResponse.isError()) {
				trace("doConnect","etcd directoryURL="+directoryURL+" does not exist, attempting to create");  //$NON-NLS-1$//$NON-NLS-2$ //$NON-NLS-3$
				// Try to create directory
				directoryExistsResponse = new EtcdSetRequest(directoryURL).execute();
				if (directoryExistsResponse.isError()) {
					trace("doConnect ERROR","etcd directoryURL could not be created, throwing"); //$NON-NLS-1$ //$NON-NLS-2$
					throw new ContainerConnectException("Could not create etcd directoryURL="+directoryURL); //$NON-NLS-1$
				}
			}
			// Else the directory already exists or was successfully created
			trace("doConnect","Directory exists="+directoryURL); //$NON-NLS-1$ //$NON-NLS-2$
		} catch (EtcdException e) {
			throw new ContainerConnectException("Exception communicating with etcd service at directoryURL="+directoryURL,e);
		}
	}

	private void startDiscoveryJob() {
		// TODO Auto-generated method stub

	}

	private void stopDiscoveryJob() {
		// TODO Auto-generated method stub

	}

	public ID getConnectedID() {
		return targetID;
	}

	public void disconnect() {
		ID anID = getConnectedID();
		fireContainerEvent(new ContainerDisconnectingEvent(this.getID(), anID));
		stopDiscoveryJob();
		targetID = null;
		fireContainerEvent(new ContainerDisconnectedEvent(this.getID(), anID));
	}

}
