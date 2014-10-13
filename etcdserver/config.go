package etcdserver

import (
	"fmt"
	"net/http"

	"github.com/coreos/etcd/pkg/types"
)

// ServerConfig holds the configuration of etcd as taken from the command line or discovery.
type ServerConfig struct {
	Name         string
	DiscoveryURL string
	ClientURLs   types.URLs
	DataDir      string
	SnapCount    uint64
	Cluster      *Cluster
	ClusterState ClusterState
	Transport    *http.Transport
}

// Verify sanity-checks the config struct and returns an error for things that
// should never happen.
func (c *ServerConfig) Verify() error {
	// Make sure the cluster at least contains the local server.
	m := c.Cluster.FindName(c.Name)
	if m == nil {
		return fmt.Errorf("could not find name %v in cluster", c.Name)
	}

	// No identical IPs in the cluster peer list
	urlMap := make(map[string]bool)
	for _, m := range *c.Cluster {
		for _, url := range m.PeerURLs {
			if urlMap[url] {
				return fmt.Errorf("duplicate url %v in server config", url)
			}
			urlMap[url] = true
		}
	}
	return nil
}
