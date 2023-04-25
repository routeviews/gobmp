package topic

import (
	"net/netip"

	"github.com/golang/glog"
)

func Debug() {
	for _, ip := range []string{"203.159.70.49", "1.2.3.5", "fed0::400"} {
		s, err := config.GetNamedRouter(netip.MustParseAddr(ip))
		glog.V(5).Infof("ip: %s, group: %s, err: %+v", ip, s, err)
	}

	subs := map[string]string{}
	ip := "128.223.51.23"
	s, _ := config.GetNamedRouter(netip.MustParseAddr(ip))
	subs["named_router"] = s
	glog.V(5).Infof("rawTemplate: %s, ip: %s, result: %s", config.rawTemplate, ip,
		substitute(config.rawTemplate, subs))
}
