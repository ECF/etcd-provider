/*******************************************************************************
 * Copyright (c) 2014 Composent, Inc. All rights reserved. This
 * program and the accompanying materials are made available under the terms of
 * the Eclipse Public License v1.0 which accompanies this distribution, and is
 * available at http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors: Scott Lewis - initial API and implementation
 ******************************************************************************/
package org.eclipse.ecf.provider.etcd.identity;

import org.eclipse.ecf.core.identity.ID;
import org.eclipse.ecf.core.identity.IDCreateException;
import org.eclipse.ecf.core.identity.Namespace;

public class EtcdNamespace extends Namespace {

	private static final long serialVersionUID = -4500842169680081611L;
	public static final String SCHEME = "etcd"; //$NON-NLS-1$
	public static final String NAME = "ecf.namespace.etcd"; //$NON-NLS-1$
	
	public EtcdNamespace() {
		super(NAME, "Etcd Discovery Namespace"); //$NON-NLS-1$
	}

	@Override
	public ID createInstance(Object[] parameters) throws IDCreateException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getScheme() {
		return SCHEME;
	}

}
