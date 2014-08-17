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
		this(url, 0);
	}

	public EtcdWatchRequest(String url, int waitIndex) {
		super(url);
		this.url = this.url + "?wait=true"; //$NON-NLS-1$
		if (waitIndex > 0)
			this.url = this.url + "&waitIndex=" + String.valueOf(waitIndex); //$NON-NLS-1$
	}

}
