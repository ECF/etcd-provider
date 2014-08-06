/*******************************************************************************
 * Copyright (c) 2014 Composent, Inc. All rights reserved. This
 * program and the accompanying materials are made available under the terms of
 * the Eclipse Public License v1.0 which accompanies this distribution, and is
 * available at http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors: Scott Lewis - initial API and implementation
 ******************************************************************************/
package org.eclipse.ecf.internal.provider.etcd;

import org.eclipse.ecf.core.identity.Namespace;
import org.eclipse.ecf.provider.etcd.identity.EtcdNamespace;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;

public class Activator implements BundleActivator {

	private static Activator plugin;
	
	public static Activator getDefault() {
		return plugin;
	}
	
	private static BundleContext context;
	
	public static BundleContext getContext() {
		return context;
	}
	
	public Activator() {
		plugin = this;
	}
	
	public void start(BundleContext ctxt) throws Exception {
		context = ctxt;
		context.registerService(Namespace.class, new EtcdNamespace(), null);
	}

	public void stop(BundleContext context) throws Exception {
		context = null;
		plugin = null;
	}

}
