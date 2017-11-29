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

public class EtcdDiscoveryContainerInstantiator implements
		IContainerInstantiator {

	public static final String NAME = "ecf.discovery.etcd"; //$NON-NLS-1$

	private EtcdDiscoveryContainer createContainer(EtcdDiscoveryContainerConfig config) throws ContainerCreateException {
		try {
			if (config == null) return new EtcdDiscoveryContainer();
			else return new EtcdDiscoveryContainer(config);
		} catch (Exception e) {
			ContainerCreateException cce = new ContainerCreateException(
					"Could not create etcd discovery container", e); //$NON-NLS-1$
			cce.setStackTrace(e.getStackTrace());
			throw cce;
		}
	}
	
	public IContainer createInstance(ContainerTypeDescription description,
			Object[] parameters) throws ContainerCreateException {

		EtcdDiscoveryContainer result = null;
		if (parameters == null || parameters.length == 0) {
			result = createContainer(null);
		} else if (parameters[0] instanceof EtcdDiscoveryContainerConfig) {
			EtcdDiscoveryContainerConfig edcc = (EtcdDiscoveryContainerConfig) parameters[0];
			if (edcc != null) 
				result = createContainer(edcc);
		}
		return result;
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
		return new Class[][] { { String.class, Void.class } };
	}

	public String[] getSupportedIntents(ContainerTypeDescription description) {
		return null;
	}

}
