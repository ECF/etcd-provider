/*******************************************************************************
 * Copyright (c) 2014 Composent, Inc. All rights reserved. This
 * program and the accompanying materials are made available under the terms of
 * the Eclipse Public License v1.0 which accompanies this distribution, and is
 * available at http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors: Scott Lewis - initial API and implementation
 ******************************************************************************/
package org.eclipse.ecf.provider.etcd;

public abstract class AbstractEtcdRequest extends AbstractEtcdProtocol {

	public static final int GET = 0x00;
	public static final int PUT = 0x10;
	public static final int DELETE = 0x100;

	public abstract int getHttpRequestType();

}
