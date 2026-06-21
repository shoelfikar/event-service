package model

import "testing"

func TestParseEventType(t *testing.T) {
	cases := map[string]*EventType{
		"play_opened":      {Category: "play", Action: "opened"},
		"play_started":     {Category: "play", Action: "started"},
		"play_closed":      {Category: "play", Action: "closed"},
		"source_connected": {Category: "source", Action: "connected"},
		"source_closed":    {Category: "source", Action: "closed"},
		"push_connected":   {Category: "push", Action: "connected"},
		"ingest_opened":    {Category: "ingest", Action: "opened"},
		"ad_injected":      {Category: "ad", Action: "injected"},
		"":                 nil,
		"garbage":          nil,
		"play_foo":         nil,
		"foo_opened":       nil,
	}
	for in, want := range cases {
		got := ParseEventType(in)
		if want == nil {
			if got != nil {
				t.Errorf("%q: expected nil, got %+v", in, got)
			}
			continue
		}
		if got == nil || got.Category != want.Category || got.Action != want.Action {
			t.Errorf("%q: got %+v, want %+v", in, got, want)
		}
	}
}
