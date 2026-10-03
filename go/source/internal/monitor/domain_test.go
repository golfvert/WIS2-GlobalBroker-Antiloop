package monitor

import "testing"

func TestDomainFromTopic(t *testing.T) {
	for topic, want := range map[string]string{
		"origin/a/wis2/se-smhi/data/core/weather": "wis2",
		"origin/a/wigos/se-smhi/x":                "wigos",
		"cache/a/wis2/se-smhi/x":                  "wis2",
		"origin":                                  "wigos",
		"":                                        "wigos",
	} {
		if got := domainFromTopic(topic); got != want {
			t.Errorf("%q: got %q want %q", topic, got, want)
		}
	}
}
