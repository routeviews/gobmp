// Package topic computes the names of Kafka topics to send processed data to.
// The names depend on the message type, router IP address, peer IP address,
// and peer ASN, and can be configured via a configuration file.
// The configuration file will be automatically re-read upon receiving SIGHUP.
package topic

import (
	"net/netip"
	"strings"
)

// SetConfigLocation sets the location of the configuration file.
// The default location is "", meaning no configuration file.
// The configuration will not be updated automatically when SetConfigLocation is called.
func SetConfigLocation(loc string) {
	config.SetConfigLocation(loc)
}

// UpdateConfig updates configuration from the configuration file.
// This is used to update the configuration by hand, instead of relying on a SIGHUP.
func UpdateConfig() error {
	return config.Update()
}

// Cleanup cleans up topic's internal state and should be called before program termination.
func Cleanup() {
	config.bye <- 1
}

// GetTopic computes a destination topic from the topic type t and a map of variable substitutions vars.
// If the topic template for the specified topic type is not found in the configuration,
// an empty string is returned.
// A missing template variable is substituted with an empty string.
func GetTopic(t string, vars map[string]string) string {
	config.mu.Lock()
	templ := config.topicTemplate[t]
	config.mu.Unlock()
	if templ == "" {
		return ""
	}

	routerIP := vars["router_ip"]
	if routerIP == "" {
		vars["named_router"] = ""
	} else {
		vars["named_router"], _ = config.GetNamedRouter(netip.MustParseAddr(routerIP))
	}

	res := templ
	for key, value := range vars {
		res = strings.Replace(res, "{{"+key+"}}", value, -1)
	}

	// glog.V(5).Infof("template(%s)=%#v, vars %#v, result: %s", t, templ, vars, res)
	return res
}
