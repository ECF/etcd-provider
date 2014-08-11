/*******************************************************************************
 * Copyright (c) 2014 Composent, Inc. All rights reserved. This
 * program and the accompanying materials are made available under the terms of
 * the Eclipse Public License v1.0 which accompanies this distribution, and is
 * available at http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors: Scott Lewis - initial API and implementation
 ******************************************************************************/
package org.eclipse.ecf.provider.etcd;

import org.eclipse.ecf.core.ContainerCreateException;
import org.eclipse.ecf.core.ContainerTypeDescription;
import org.eclipse.ecf.core.IContainer;
import org.eclipse.ecf.core.provider.IContainerInstantiator;
import org.eclipse.ecf.discovery.IDiscoveryAdvertiser;
import org.eclipse.ecf.discovery.IDiscoveryLocator;

public class EtcDiscoveryContainerInstantiator implements
		IContainerInstantiator {

	public static final String NAME = "ecf.discovery.etcd"; //$NON-NLS-1$
	
	public IContainer createInstance(ContainerTypeDescription description,
			Object[] parameters) throws ContainerCreateException {
		// TODO Auto-generated method stub
		return null;
	}

	public String[] getSupportedAdapterTypes(
			ContainerTypeDescription description) {
		if (description.getName().equals(NAME))
			return new String[] { IContainer.class.getName(),
					IDiscoveryAdvertiser.class.getName(),
					IDiscoveryLocator.class.getName() };
		return new String[0];
	}

	@SuppressWarnings("rawtypes")
	public Class[][] getSupportedParameterTypes(
			ContainerTypeDescription description) {
		return new Class[][] { { String.class } };
	}

	public String[] getSupportedIntents(ContainerTypeDescription description) {
		return null;
	}

}
