package topic

import (
	"net/netip"
	"testing"
)

func TestStopSignalHandler(t *testing.T) {
	Cleanup()
}

func TestNamedRouters(t *testing.T) {
	err := config.UpdateData([]byte(`---
bmp_raw: "routeviews.{{named_router}}.bmp_raw"
named_routers:
  - name: group1234
    matches:
      - 1.2.3.4
  - name: group10
    matches:
      - 10.0.0.0/8
  - name: fallback
    matches:
      - 0.0.0.0/0
`))
	if err != nil {
		t.Error(err)
	}
	var s string
	s, err = config.GetNamedRouter(netip.MustParseAddr("1.2.3.4"))
	if err != nil {
		t.Error(err)
	}
	if s != "group1234" {
		t.Errorf("unexpected group %s for router %s", s, "1.2.3.4")
	}
}
