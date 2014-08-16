/*******************************************************************************
 * Copyright (c) 2014 Composent, Inc. All rights reserved. This
 * program and the accompanying materials are made available under the terms of
 * the Eclipse Public License v1.0 which accompanies this distribution, and is
 * available at http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors: Scott Lewis - initial API and implementation
 ******************************************************************************/
package org.eclipse.ecf.provider.etcd;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.eclipse.core.runtime.Assert;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class EtcdNode {

	public static final String CREATEDINDEX_KEY = "createdIndex"; //$NON-NLS-1$
	public static final String KEY_KEY = "key"; //$NON-NLS-1$
	public static final String DIR_KEY = "dir"; //$NON-NLS-1$
	public static final String MODIFIEDINDEX_KEY = "modifiedIndex"; //$NON-NLS-1$
	public static final String EXPIRATION_KEY = "expiration"; //$NON-NLS-1$
	public static final String NODES_KEY = "nodes"; //$NON-NLS-1$
	public static final String TTL_KEY = "ttl"; //$NON-NLS-1$
	public static final String VALUE_KEY = "value"; //$NON-NLS-1$

	private final int createdIndex;
	private final boolean directory;
	private final String expiration;
	private final String key;
	private final int modifiedIndex;
	private final Integer ttl;
	private final String value;

	private final EtcdNode[] nodes;

	private EtcdNode[] createNodes(JSONObject jsonObject) throws JSONException {
		JSONArray array = jsonObject.optJSONArray(NODES_KEY);
		if (array != null) {
			List<EtcdNode> nodes = new ArrayList<EtcdNode>();
			for (int i = 0; i < array.length(); i++)
				nodes.add(new EtcdNode(array.getJSONObject(i)));
			return nodes.toArray(new EtcdNode[nodes.size()]);
		} else
			return null;
	}

	public EtcdNode(JSONObject jsonObject) throws JSONException {
		Assert.isNotNull(jsonObject);
		this.createdIndex = jsonObject.getInt(CREATEDINDEX_KEY);
		this.directory = jsonObject.optBoolean(DIR_KEY);
		this.expiration = jsonObject.optString(EXPIRATION_KEY);
		this.key = jsonObject.getString(KEY_KEY);
		this.modifiedIndex = jsonObject.getInt(MODIFIEDINDEX_KEY);
		this.ttl = jsonObject.optInt(TTL_KEY);
		this.value = jsonObject.optString(VALUE_KEY);
		this.nodes = createNodes(jsonObject);
	}

	public int getCreatedIndex() {
		return createdIndex;
	}

	public boolean isDirectory() {
		return directory;
	}

	public String getExpiration() {
		return expiration;
	}

	public String getKey() {
		return key;
	}

	public int getModifiedIndex() {
		return modifiedIndex;
	}

	public Integer getTtl() {
		return ttl;
	}

	public String getValue() {
		return value;
	}

	public EtcdNode[] getNodes() {
		return nodes;
	}

	@Override
	public String toString() {
		return "EtcdNode [createdIndex=" + createdIndex + ", directory=" //$NON-NLS-1$ //$NON-NLS-2$
				+ directory + ", expiration=" + expiration + ", key=" + key //$NON-NLS-1$ //$NON-NLS-2$
				+ ", modifiedIndex=" + modifiedIndex + ", ttl=" + ttl //$NON-NLS-1$ //$NON-NLS-2$
				+ ", value=" + value + ", nodes=" + Arrays.toString(nodes) //$NON-NLS-1$ //$NON-NLS-2$
				+ "]"; //$NON-NLS-1$
	}

}
