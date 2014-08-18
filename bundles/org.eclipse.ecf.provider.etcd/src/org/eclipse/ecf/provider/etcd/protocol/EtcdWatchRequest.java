/*******************************************************************************
 * Copyright (c) 2014 Composent, Inc. All rights reserved. This
 * program and the accompanying materials are made available under the terms of
 * the Eclipse Public License v1.0 which accompanies this distribution, and is
 * available at http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors: Scott Lewis - initial API and implementation
 ******************************************************************************/
package org.eclipse.ecf.provider.etcd.protocol;

public class EtcdWatchRequest extends EtcdGetRequest {

	public EtcdWatchRequest(String url) {
		this(url, null);
	}

	public EtcdWatchRequest(String url, String waitIndex) {
		super(url);
		setQueryBoolean(WAIT);
		if (waitIndex != null)
			setQueryParam(WAITINDEX, waitIndex);
	}

}
