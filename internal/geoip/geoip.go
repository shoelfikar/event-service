// Package geoip wraps the MaxMind GeoLite2-City database.
//
// Parity: backendV2 reads country.iso_code, country.names.en, city.names.en and
// location.{latitude,longitude}. Use the SAME .mmdb file as backendV2 to keep
// the derived `cities` Redis key (`<country_code>:<city>`) consistent.
package geoip

import (
	"net"

	"github.com/oschwald/geoip2-golang"
)

type Result struct {
	CountryISO  string
	CountryName string
	City        string
	Lat         float64
	Lng         float64
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
		CountryISO:  rec.Country.IsoCode,
		CountryName: rec.Country.Names["en"],
		City:        rec.City.Names["en"],
		Lat:         rec.Location.Latitude,
		Lng:         rec.Location.Longitude,
	}
	return res
}
