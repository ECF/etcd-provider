/*******************************************************************************
 * Copyright (c) 2014 Composent, Inc. All rights reserved. This
 * program and the accompanying materials are made available under the terms of
 * the Eclipse Public License v1.0 which accompanies this distribution, and is
 * available at http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors: Scott Lewis - initial API and implementation
 ******************************************************************************/
package org.eclipse.ecf.provider.etcd;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eclipse.ecf.core.ContainerConnectException;
import org.eclipse.ecf.core.identity.ID;
import org.eclipse.ecf.core.security.IConnectContext;
import org.eclipse.ecf.discovery.AbstractDiscoveryContainerAdapter;
import org.eclipse.ecf.discovery.IServiceInfo;
import org.eclipse.ecf.discovery.identity.IServiceID;
import org.eclipse.ecf.discovery.identity.IServiceTypeID;
import org.eclipse.ecf.provider.etcd.identity.EtcdNamespace;

public class EtcdDiscoveryContainer extends AbstractDiscoveryContainerAdapter {

	private final Map<IServiceID, IServiceInfo> publishedServices = new HashMap<IServiceID,IServiceInfo>();
	
	public EtcdDiscoveryContainer(EtcdDiscoveryContainerConfig config) {
		super(EtcdNamespace.NAME, config);
	}

	public IServiceInfo getServiceInfo(IServiceID aServiceID) {
		synchronized (publishedServices) {
			return publishedServices.get(aServiceID);
		}
	}

	public IServiceInfo[] getServices() {
		return publishedServices.values().toArray(new IServiceInfo[publishedServices.size()]);
	}

	public IServiceInfo[] getServices(IServiceTypeID aServiceTypeID) {
		List<IServiceInfo> results = new ArrayList<IServiceInfo>();
		synchronized (publishedServices) {
			for(IServiceID sid: publishedServices.keySet()) 
				if (sid.getServiceTypeID().equals(aServiceTypeID))
					results.add(publishedServices.get(sid));
		}
		return results.toArray(new IServiceInfo[results.size()]);
	}

	public IServiceTypeID[] getServiceTypes() {
		Set<IServiceTypeID> results = new HashSet<IServiceTypeID>();
		synchronized (publishedServices) {
			for(IServiceID sid: publishedServices.keySet()) 
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
		return EtcDiscoveryContainerInstantiator.NAME;
	}

	public void connect(ID targetID, IConnectContext connectContext)
			throws ContainerConnectException {
		// TODO Auto-generated method stub
		
	}

	public ID getConnectedID() {
		// TODO Auto-generated method stub
		return null;
	}

	public void disconnect() {
		// TODO Auto-generated method stub
		
	}

}
