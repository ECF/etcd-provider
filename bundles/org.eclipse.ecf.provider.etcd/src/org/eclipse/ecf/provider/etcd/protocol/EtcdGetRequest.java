/*******************************************************************************
 * Copyright (c) 2014 Composent, Inc. All rights reserved. This
 * program and the accompanying materials are made available under the terms of
 * the Eclipse Public License v1.0 which accompanies this distribution, and is
 * available at http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors: Scott Lewis - initial API and implementation
 ******************************************************************************/
package org.eclipse.ecf.provider.etcd.protocol;

public class EtcdGetRequest extends EtcdRequest {

	public EtcdGetRequest(String url) {
		super(url);
	}

	public EtcdGetRequest(String directoryURL, boolean recursive) {
		super(directoryURL);
		if (recursive)
			setQueryBoolean(RECURSIVE);
	}

}
