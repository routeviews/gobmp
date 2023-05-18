package topic

import "net/netip"

type configPrefix struct {
	P netip.Prefix
}

func (p *configPrefix) UnmarshalText(text []byte) error {
	var err error
	p.P, err = netip.ParsePrefix(string(text))
	if err != nil {
		var a netip.Addr
		a, err = netip.ParseAddr(string(text))
		if err != nil {
			return err
		}
		if a.Is4() {
			p.P = netip.PrefixFrom(a, 32)
		} else {
			p.P = netip.PrefixFrom(a, 128)
		}
	}
	return err
}

type YAMLConfiguration struct {
	Topics  map[string]string `yaml:"topics"`
	Routers []struct {
		Name    string         `yaml:"name"`
		Matches []configPrefix `yaml:"matches"`
	} `yaml:"named_routers"`
}
