/*******************************************************************************
 * Copyright (c) 2014 Composent, Inc. All rights reserved. This
 * program and the accompanying materials are made available under the terms of
 * the Eclipse Public License v1.0 which accompanies this distribution, and is
 * available at http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors: Scott Lewis - initial API and implementation
 ******************************************************************************/
package org.eclipse.ecf.provider.etcd;

public class EtcdException extends Exception {

	private static final long serialVersionUID = 7690024036798827062L;

	public EtcdException() {
	}

	public EtcdException(String message) {
		super(message);
	}

	public EtcdException(Throwable cause) {
		super(cause);
	}

	public EtcdException(String message, Throwable cause) {
		super(message, cause);
	}

}
