// Package topic computes the names of Kafka topics to send processed data to.
// The names depend on the message type, router IP address, and XXX,
// and can be configured via a configuration file.
// The configuration file will be automatically re-read upon receiving SIGHUP.
package topic

import (
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

func substitute(template string, subs map[string]string) string {
	for key, value := range subs {
		template = strings.Replace(template, "{{"+key+"}}", value, -1)
	}
	return template
}
