/*******************************************************************************
 * Copyright (c) 2014 Composent, Inc. All rights reserved. This
 * program and the accompanying materials are made available under the terms of
 * the Eclipse Public License v1.0 which accompanies this distribution, and is
 * available at http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors: Scott Lewis - initial API and implementation
 ******************************************************************************/
package org.eclipse.ecf.provider.etcd;

import java.net.URI;

import org.eclipse.ecf.discovery.IServiceProperties;
import org.eclipse.ecf.discovery.ServiceInfo;
import org.eclipse.ecf.discovery.identity.IServiceTypeID;

public class EtcdServiceInfo extends ServiceInfo {

	private static final long serialVersionUID = -1975271151544601442L;

	public EtcdServiceInfo() {
	}

	public EtcdServiceInfo(URI anURI, String aServiceName,
			IServiceTypeID aServiceTypeID) {
		super(anURI, aServiceName, aServiceTypeID);
	}

	public EtcdServiceInfo(URI anURI, String aServiceName,
			IServiceTypeID aServiceTypeID, IServiceProperties props) {
		super(anURI, aServiceName, aServiceTypeID, props);
	}

	public EtcdServiceInfo(URI anURI, String aServiceName,
			IServiceTypeID aServiceTypeID, int priority, int weight,
			IServiceProperties props) {
		super(anURI, aServiceName, aServiceTypeID, priority, weight, props);
	}

	public EtcdServiceInfo(URI anURI, String aServiceName,
			IServiceTypeID aServiceTypeID, int priority, int weight,
			IServiceProperties props, long ttl) {
		super(anURI, aServiceName, aServiceTypeID, priority, weight, props, ttl);
	}

}
