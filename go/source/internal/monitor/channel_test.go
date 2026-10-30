package monitor

import (
	"encoding/json"
	"testing"
)

// The WME event carries the original topic as data.content.channel (not
// data.channel), matching the flow's "Exception" change nodes.
func TestChannelInContent(t *testing.T) {
	var body []byte
	r := &Reporter{
		GlobalBrokerID: "gb",
		CentreID:       "se-smhi",
		Publish: func(topic string, qos byte, retained bool, payload []byte) (int, error) {
			body = payload
			return 1, nil
		},
	}
	topic := "origin/a/wis2/se-smhi/data/core/weather"
	r.ReportTopicFailure(topic, []byte(`{"id":"x"}`))

	var ev struct {
		Data map[string]json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &ev); err != nil {
		t.Fatal(err)
	}
	if _, ok := ev.Data["channel"]; ok {
		t.Error("data.channel must not be present")
	}
	var content struct {
		Channel string `json:"channel"`
	}
	if err := json.Unmarshal(ev.Data["content"], &content); err != nil {
		t.Fatal(err)
	}
	if content.Channel != topic {
		t.Errorf("content.channel: got %q want %q", content.Channel, topic)
	}
}
