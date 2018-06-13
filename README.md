etcd-provider
=============

ECF discovery provider that uses etcd service to publish and discover remote service endpoint descriptions.   See https://github.com/coreos/etcd to download, install, and configure the etcd server.   

This provider used an etcd server to publish and discover Remote Services.   It's necessary to install the provider bundle:  org.eclipse.ecf.provider.etcd, and configure it so that it points to a running etcd server/service.

For this provider to work, an etcd server must be running at some available hostname and port before the process using the etcd provider is started.  Note that when the provider is started, it will immediately attempt to connect to the etcd server.  If that connection cannot be made it will result in an ERROR to the OSGi log.

The entire list of configurable properties for the etcd provider is in [this class](https://github.com/ECF/etcd-provider/blob/master/bundles/org.eclipse.ecf.provider.etcd/src/org/eclipse/ecf/provider/etcd/EtcdDiscoveryContainerConfig.java)

The most important properties are:

| Property Name | Default Value |
| --- | --- |
| ecf.discovery.etcd.hostname | 127.0.0.1 |
| ecf.discovery.etcd.port | 2379 |
| ecf.discovery.etcd.containerId | org.eclipse.ecf.provider.etcd.EtcdDiscoveryContainer |

For example, to set the etcd server to:  'disco.ecf-project.org' set the java system propery...e.g. 

<other java start params> -Decf.discovery.etcd.hostname=disco.ecf-project.org

LICENSE
=======

Python.Java Remote Services is distributed with the Apache 2 license. See LICENSE in this directory for more
information.
