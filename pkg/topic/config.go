package topic

import (
	"errors"
	"net/netip"
	"os"
	"sync"

	"go4.org/netipx"
	"gopkg.in/yaml.v2"
)

type routerConfig struct {
	Name    string
	Matches *netipx.IPSet
}

type topicConfig struct {
	mu                sync.Mutex
	hup               chan os.Signal
	bye               chan int
	configLocation    string
	collectorTemplate string
	routerTemplate    string
	rawTemplate       string
	routers           []routerConfig
}

func (t *topicConfig) GetNamedRouter(a netip.Addr) (string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, r := range t.routers {
		if r.Matches.Contains(a) {
			return r.Name, nil
		}
	}
	return "", errors.New("no matching router")
}

func (t *topicConfig) SetConfigLocation(loc string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.configLocation = loc
}

func (t *topicConfig) Update() error {
	if t.configLocation == "" {
		return errors.New("config location not set")
	}

	data, err := os.ReadFile("config.yaml")
	if err != nil {
		return err
	}

	err = t.UpdateData(data)
	if err != nil {
		return err
	}

	return nil
}

func (t *topicConfig) UpdateData(data []byte) error {
	var y YAMLConfiguration
	err := yaml.Unmarshal(data, &y)
	if err != nil {
		return err
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	t.collectorTemplate = y.CollectorTemplate
	t.routerTemplate = y.RouterTemplate
	t.rawTemplate = y.RawTemplate
	t.routers = make([]routerConfig, len(y.Routers))
	for i, r := range y.Routers {
		t.routers[i].Name = r.Name
		var b netipx.IPSetBuilder

		for _, m := range r.Matches {
			b.AddPrefix(m.P)
		}
		t.routers[i].Matches, _ = b.IPSet()
	}

	return nil
}
