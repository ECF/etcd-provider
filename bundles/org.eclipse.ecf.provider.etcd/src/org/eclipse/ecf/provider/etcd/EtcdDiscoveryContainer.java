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

	public void connect(ID aTargetID, IConnectContext connectContext)
			throws ContainerConnectException {
		if (targetID != null) {
			throw new ContainerConnectException("Already connected"); //$NON-NLS-1$
		}
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
		fireContainerEvent(new ContainerConnectingEvent(this.getID(),
				aTargetID, connectContext));
		fireContainerEvent(new ContainerConnectedEvent(this.getID(), aTargetID));
		startDiscoveryJob();
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
