// Package geoip wraps the MaxMind GeoLite2-City database.
//
// Parity: the realtime Redis viewers path reads country.iso_code,
// country.names.en, city.names.en and location.{latitude,longitude}; the
// session_geoip enrichment (mirroring backendV2's GeoIpService.enrichAndSave)
// reads the full record (continent, registered country, geoname ids, accuracy,
// time zone). Use the SAME .mmdb file as backendV2 to keep the derived `cities`
// Redis key (`<country_code>:<city>`) and the session_geoip rows consistent.
package geoip

import (
	"net"

	"github.com/oschwald/geoip2-golang"
)

// Result is the full GeoLite2-City lookup, covering both the realtime Redis
// viewers path (Country*/City/Lat/Lng) and the session_geoip enrichment (the
// remaining fields). Mirrors backendV2's GeoLite2LookupResult.
type Result struct {
	ContinentCode      string `json:"continent_code"`
	ContinentGeonameID uint   `json:"continent_geoname_id"`
	ContinentName      string `json:"continent_name"`

	CountryISO       string `json:"country_iso_code"`
	CountryGeonameID uint   `json:"country_geoname_id"`
	CountryName      string `json:"country_name"`

	RegisteredCountryISO  string `json:"registered_country_iso"`
	RegisteredCountryName string `json:"registered_country_name"`

	CityGeonameID uint   `json:"city_geoname_id"`
	City          string `json:"city_name"`

	Lat            float64 `json:"latitude"`
	Lng            float64 `json:"longitude"`
	AccuracyRadius uint16  `json:"accuracy_radius"`
	TimeZone       string  `json:"time_zone"`
}

type Reader struct {
	db *geoip2.Reader
}

func Open(path string) (*Reader, error) {
	db, err := geoip2.Open(path)
	if err != nil {
		return nil, err
	}
	return &Reader{db: db}, nil
}

func (r *Reader) Close() error { return r.db.Close() }

// BuildEpoch returns the MaxMind database build timestamp (Unix seconds), useful
// for confirming which GeoLite2 build is loaded and matches backendV2.
func (r *Reader) BuildEpoch() uint {
	return r.db.Metadata().BuildEpoch
}

// Lookup resolves an IP. Returns nil for blank/invalid IPs or lookup misses,
// matching backendV2's null-returning behaviour.
func (r *Reader) Lookup(ip string) *Result {
	if ip == "" {
		return nil
	}
	parsed := net.ParseIP(ip)
	if parsed == nil {
		return nil
	}
	rec, err := r.db.City(parsed)
	if err != nil || rec == nil {
		return nil
	}
	res := &Result{
		ContinentCode:      rec.Continent.Code,
		ContinentGeonameID: rec.Continent.GeoNameID,
		ContinentName:      rec.Continent.Names["en"],

		CountryISO:       rec.Country.IsoCode,
		CountryGeonameID: rec.Country.GeoNameID,
		CountryName:      rec.Country.Names["en"],

		RegisteredCountryISO:  rec.RegisteredCountry.IsoCode,
		RegisteredCountryName: rec.RegisteredCountry.Names["en"],

		CityGeonameID: rec.City.GeoNameID,
		City:          rec.City.Names["en"],

		Lat:            rec.Location.Latitude,
		Lng:            rec.Location.Longitude,
		AccuracyRadius: rec.Location.AccuracyRadius,
		TimeZone:       rec.Location.TimeZone,
	}
	return res
}
