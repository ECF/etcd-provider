/*******************************************************************************
 * Copyright (c) 2014 Composent, Inc. All rights reserved. This
 * program and the accompanying materials are made available under the terms of
 * the Eclipse Public License v1.0 which accompanies this distribution, and is
 * available at http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors: Scott Lewis - initial API and implementation
 ******************************************************************************/
package org.eclipse.ecf.provider.etcd.identity;

import java.net.URI;

import org.eclipse.ecf.core.identity.Namespace;
import org.eclipse.ecf.discovery.identity.IServiceTypeID;
import org.eclipse.ecf.discovery.identity.ServiceID;

public class EtcdServiceID extends ServiceID {

	private static final long serialVersionUID = -1416677955466105800L;

	public EtcdServiceID(Namespace namespace, IServiceTypeID type, URI anURI) {
		super(namespace, type, anURI);
	}

}
