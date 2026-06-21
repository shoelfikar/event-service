// Package ua parses raw user-agent strings into device dimensions.
//
// Parity note: backendV2 uses ua-parser-js. This package uses
// github.com/mileusna/useragent, which is close but not byte-identical. The
// device_type vocabulary is normalized to match the TS side's common values
// ("mobile" | "tablet" | "desktop" | "bot" | "unknown"). Brand/model fidelity
// is lower than ua-parser-js; these feed only secondary analytics columns.
package ua

import uaparser "github.com/mileusna/useragent"

// Parsed mirrors backendV2's ParsedUserAgent. Empty string means SQL NULL.
type Parsed struct {
	DeviceType  string
	BrowserName string
	OSName      string
	DeviceBrand string
	DeviceModel string
}

// Empty matches the TS EMPTY shape: device_type 'unknown', rest null.
func Empty() Parsed { return Parsed{DeviceType: "unknown"} }

// Parse extracts device/browser/OS dimensions from a UA string.
// Blank input yields Empty().
func Parse(s string) Parsed {
	if s == "" {
		return Empty()
	}
	r := uaparser.Parse(s)

	deviceType := "desktop" // ua-parser-js normalizes undefined → desktop
	switch {
	case r.Bot:
		deviceType = "bot"
	case r.Tablet:
		deviceType = "tablet"
	case r.Mobile:
		deviceType = "mobile"
	}

	return Parsed{
		DeviceType:  deviceType,
		BrowserName: r.Name,
		OSName:      r.OS,
		DeviceBrand: r.Device, // mileusna exposes a single device string
		DeviceModel: "",
	}
}
