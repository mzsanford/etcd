package discovery

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/coreos/etcd/client"
)

var (
	ErrInvalidURL     = errors.New("discovery: invalid URL")
	ErrBadSizeKey     = errors.New("discovery: size key is bad")
	ErrSizeNotFound   = errors.New("discovery: size key not found")
	ErrTokenNotFound  = errors.New("discovery: token not found")
	ErrDuplicateID    = errors.New("discovery: found duplicate id")
	ErrFullCluster    = errors.New("discovery: cluster is full")
	ErrTooManyRetries = errors.New("discovery: too many retries")
)

const (
	// Number of retries discovery will attempt before giving up and erroring out.
	nRetries = uint(3)
)

type Discoverer interface {
	Discover() (string, error)
}

type discovery struct {
	cluster string
	id      uint64
	config  string
	c       client.Client
	retries uint
	url     *url.URL

	// Injectable for testing. nil means Seconds.
	timeoutTimescale time.Duration
}

func New(durl string, id uint64, config string) (Discoverer, error) {
	u, err := url.Parse(durl)
	if err != nil {
		return nil, err
	}
	token := u.Path
	u.Path = ""
	c, err := client.NewHTTPClient(&http.Transport{}, u.String(), time.Second*5)
	if err != nil {
		return nil, err
	}
	// discovery service redirects /[key] to /v2/keys/[key]
	// set the prefix of client to "" to handle this
	c.SetPrefix("")
	return &discovery{
		cluster:          token,
		id:               id,
		config:           config,
		c:                c,
		url:              u,
		timeoutTimescale: time.Second,
	}, nil
}

func (d *discovery) Discover() (string, error) {
	// fast path: if the cluster is full, returns the error
	// do not need to register itself to the cluster in this
	// case.
	if _, _, err := d.checkCluster(); err != nil {
		return "", err
	}

	if err := d.createSelf(); err != nil {
		// Fails, even on a timeout, if createSelf times out.
		// TODO(barakmich): Retrying the same node might want to succeed here
		// (ie, createSelf should be idempotent for discovery).
		return "", err
	}

	nodes, size, err := d.checkCluster()
	if err != nil {
		return "", err
	}

	all, err := d.waitNodes(nodes, size)
	if err != nil {
		return "", err
	}

	return nodesToCluster(all), nil
}

func (d *discovery) createSelf() error {
	resp, err := d.c.Create(d.selfKey(), d.config, -1)
	if err != nil {
		return err
	}

	// ensure self appears on the server we connected to
	w := d.c.Watch(d.selfKey(), resp.Node.CreatedIndex)
	_, err = w.Next()
	return err
}

func (d *discovery) checkCluster() (client.Nodes, int, error) {
	configKey := path.Join("/", d.cluster, "_config")
	// find cluster size
	resp, err := d.c.Get(path.Join(configKey, "size"))
	if err != nil {
		if err == client.ErrKeyNoExist {
			return nil, 0, ErrSizeNotFound
		}
		if err == client.ErrTimeout {
			return d.checkClusterRetry()
		}
		return nil, 0, err
	}
	size, err := strconv.Atoi(resp.Node.Value)
	if err != nil {
		return nil, 0, ErrBadSizeKey
	}

	resp, err = d.c.Get(d.cluster)
	if err != nil {
		if err == client.ErrTimeout {
			return d.checkClusterRetry()
		}
		return nil, 0, err
	}
	nodes := make(client.Nodes, 0)
	// append non-config keys to nodes
	for _, n := range resp.Node.Nodes {
		if !strings.Contains(n.Key, configKey) {
			nodes = append(nodes, n)
		}
	}

	snodes := sortableNodes{nodes}
	sort.Sort(snodes)

	// find self position
	for i := range nodes {
		if strings.Contains(nodes[i].Key, d.selfKey()) {
			break
		}
		if i >= size-1 {
			return nil, size, ErrFullCluster
		}
	}
	return nodes, size, nil
}

func (d *discovery) logAndBackoffForRetry(step string) {
	d.retries++
	retryTime := d.timeoutTimescale * (0x1 << d.retries)
	log.Println("discovery: during", step, "connection to", d.url, "timed out, retrying in", retryTime)
	time.Sleep(retryTime)
}

func (d *discovery) checkClusterRetry() (client.Nodes, int, error) {
	if d.retries < nRetries {
		d.logAndBackoffForRetry("cluster status check")
		return d.checkCluster()
	}
	return nil, 0, ErrTooManyRetries
}

func (d *discovery) waitNodesRetry() (client.Nodes, error) {
	if d.retries < nRetries {
		d.logAndBackoffForRetry("waiting for other nodes")
		nodes, n, err := d.checkCluster()
		if err != nil {
			return nil, err
		}
		return d.waitNodes(nodes, n)
	}
	return nil, ErrTooManyRetries
}

func (d *discovery) waitNodes(nodes client.Nodes, size int) (client.Nodes, error) {
	if len(nodes) > size {
		nodes = nodes[:size]
	}
	w := d.c.RecursiveWatch(d.cluster, nodes[len(nodes)-1].ModifiedIndex+1)
	all := make(client.Nodes, len(nodes))
	copy(all, nodes)
	// wait for others
	for len(all) < size {
		resp, err := w.Next()
		if err != nil {
			if err == client.ErrTimeout {
				return d.waitNodesRetry()
			}
			return nil, err
		}
		all = append(all, resp.Node)
	}
	return all, nil
}

func (d *discovery) selfKey() string {
	return path.Join("/", d.cluster, fmt.Sprintf("%d", d.id))
}

func nodesToCluster(ns client.Nodes) string {
	s := make([]string, len(ns))
	for i, n := range ns {
		s[i] = n.Value
	}
	return strings.Join(s, ",")
}

type sortableNodes struct{ client.Nodes }

func (ns sortableNodes) Len() int { return len(ns.Nodes) }
func (ns sortableNodes) Less(i, j int) bool {
	return ns.Nodes[i].CreatedIndex < ns.Nodes[j].CreatedIndex
}
func (ns sortableNodes) Swap(i, j int) { ns.Nodes[i], ns.Nodes[j] = ns.Nodes[j], ns.Nodes[i] }
