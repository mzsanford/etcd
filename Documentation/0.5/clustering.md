# Clustering Guide

This guide will walk you through configuring a three machine etcd cluster with
the following details:

|Name	|Address	|
|-------|---------------|
|infra0	|10.0.1.10	|
|infra1	|10.0.1.11	|
|infra2	|10.0.1.12	|

## Static

As we know the cluster members, their addresses and the size of the cluster
before starting we can use an offline bootstrap configuration. Each machine
will get either the following command line or environment variables:

```
ETCD_INITIAL_CLUSTER=”infra0=http://10.0.1.10:2379,infra1=http://10.0.1.11:2379,infra2=http://10.0.1.12:2379”
ETCD_INITIAL_CLUSTER_STATE=new
```

```
-initial-cluster infra0=http://10.0.1.10:2379,http://10.0.1.11:2379,infra2=http://10.0.1.12:2379 \
	-initial-cluster-state new
```

On each machine you would start etcd with these flags:

```
$ etcd -name infra0 -advertise-peer-urls https://10.0.1.10:2379 \
	-initial-cluster infra0=http://10.0.1.10:2379,infra1=http://10.0.1.11:2379,infra2=http://10.0.1.12:2379 \
	-initial-cluster-state new
$ etcd -name infra1 -advertise-peer-urls https://10.0.1.11:2379 \
	-initial-cluster infra0=http://10.0.1.10:2379,infra1=http://10.0.1.11:2379,infra2=http://10.0.1.12:2379 \
	-initial-cluster-state new
$ etcd -name infra2 -advertise-peer-urls https://10.0.1.12:2379 \
	-initial-cluster infra0=http://10.0.1.10:2379,infra1=http://10.0.1.11:2379,infra2=http://10.0.1.12:2379 \
	-initial-cluster-state new
```

The command line parameters starting with `-initial-cluster` will be ignored on
subsequent runs of etcd. You are free to remove the environment variables or
command line flags after the initial bootstrap process. If you need to make
changes to the configuration later see our guide on runtime configuration.

### Error Cases

In the following case we have not included our new host in the list of
enumerated nodes. If this is a new cluster, the node must be added to the list
of initial cluster members.
```
$ etcd -name infra1 -advertise-peer-urls http://10.0.1.11:2379 \
	-initial-cluster infra0=http://10.0.1.10:2379 \
	-initial-cluster-state new
etcd: infra1 not listed in the initial cluster config
exit 1
```

In this case we are attempting to map a node (infra0) on a different address
(127.0.0.1:2379) than its enumerated address in the cluster list
(10.0.1.10:2379). If this node is to listen on multiple addresses, all
addresses must be reflected in the “initial-cluster” configuration directive.

```
$ etcd -name infra0 -advertise-peer-urls http://127.0.0.1:2379 \
	-initial-cluster infra0=http://10.0.1.10:2379,infra1=http://10.0.1.11:2379,infra2=http://10.0.1.12:2379 \
	-initial-cluster-state=new
etcd: infra0 has different advertised URLs in the cluster and advertised peer URLs list
exit 1
```

## Discovery

In a number of cases you might not know the IPs of your cluster peers ahead of
time. This is common when utilizing cloud providers or when your network uses
DHCP. In these cases you can use an existing etcd cluster to bootstrap a new
one. We call this process “discovery”.

Discovery uses an existing cluster to bootstrap itself.  If you are using your
own etcd cluster you can create a URL like so:

```
$ curl https://myetcd.local/v2/keys/discovery/6c007a14875d53d9bf0ef5a6fc0257c817f0fb83/_config/size -d value=5
```

The URL you will use in this case will be
`https://myetcd.local/v2/keys/discovery/6c007a14875d53d9bf0ef5a6fc0257c817f0fb83`
and the machines will use the
`https://myetcd.local/v2/keys/discovery/6c007a14875d53d9bf0ef5a6fc0257c817f0fb83`
directory for registration as they start.

If you do not have access to an existing cluster you can use the hosted
discovery.etcd.io service.  You can create a private discovery URL using the
"new" endpoint like so:

```
$ curl https://discovery.etcd.io/new?size=3
https://discovery.etcd.io/3e86b59982e49066c5d813af1c2e2579cbf573de
```

This will create the cluster with an initial expected size of 3 members. If you
do not specify a size a default of 3 will be used.

```
ETCD_DISCOVERY=https://discovery.etcd.io/3e86b59982e49066c5d813af1c2e2579cbf573de
```

```
-discovery https://discovery.etcd.io/3e86b59982e49066c5d813af1c2e2579cbf573de
```

Now we start etcd with those relevant flags on each machine:

```
$ etcd -name infra0 -advertise-peer-urls http://10.0.1.10:2379 -discovery https://discovery.etcd.io/3e86b59982e49066c5d813af1c2e2579cbf573de
$ etcd -name infra1 -advertise-peer-urls http://10.0.1.11:2379 -discovery https://discovery.etcd.io/3e86b59982e49066c5d813af1c2e2579cbf573de
$ etcd -name infra2 -advertise-peer-urls http://10.0.1.12:2379 -discovery https://discovery.etcd.io/3e86b59982e49066c5d813af1c2e2579cbf573de
```

This will cause each machine to register itself with the etcd service and begin
the cluster once all machines have been registered.

### Error and Warning Cases

#### Discovery Server Errors

```
$ etcd -name infra0 -advertise-peer-urls http://10.0.1.10:2379 -discovery https://discovery.etcd.io/3e86b59982e49066c5d813af1c2e2579cbf573de
etcd: error: the cluster doesn’t have a size configuration value in https://discovery.etcd.io/3e86b59982e49066c5d813af1c2e2579cbf573de/_config
exit 1
```

#### User Errors

```
$ etcd -name infra0 -advertise-peer-urls http://10.0.1.10:2379 -discovery https://discovery.etcd.io/3e86b59982e49066c5d813af1c2e2579cbf573de
etcd: error: the cluster using discovery https://discovery.etcd.io/3e86b59982e49066c5d813af1c2e2579cbf573de has already started with all 5 members
exit 1
```

#### Warnings

This is a harmless warning notifying you that the discovery URL will be
ignored on this machine.

```
$ etcd -name infra0 -advertise-peer-urls http://10.0.1.10:2379 -discovery https://discovery.etcd.io/3e86b59982e49066c5d813af1c2e2579cbf573de
etcd: warn: ignoring discovery URL: etcd has already been initialized and has a valid log in /var/lib/etcd
```

# 0.4 to 0.5+ Migration Guide

In etcd 0.5 we introduced the ability to listen on more than one address and to
advertise multiple addresses. This makes using etcd easier when you have
complex networking, such as private and public networks on various cloud
providers.

To make understanding this feature easier, we changed the naming of some flags,
but we support the old flags to make the migration from the old to new version
easier.

|Old Flag		|New Flag		|Migration Behavior									|
|-----------------------|-----------------------|---------------------------------------------------------------------------------------|
|-peer-addr		|-advertise-peer-urls 	|If specified, peer-addr will be used as the only peer URL. Error if both flags specified.|
|-addr			|-advertise-client-urls	|If specified, addr will be used as the only client URL. Error if both flags specified.|
|-peer-bind-addr	|-listen-peer-urls	|If specified, peer-bind-addr will be used as the only peer bind URL. Error if both flags specified.|
|-bind-addr		|-listen-client-urls	|If specified, bind-addr will be used as the only client bind URL. Error if both flags specified.|
|-peers			|none			|Deprecated. The -initial-cluster flag provides a similar concept with different semantics. Please read this guide on cluster startup.|
|-peers-file		|none			|Deprecated. The -initial-cluster flag provides a similar concept with different semantics. Please read this guide on cluster startup.|
