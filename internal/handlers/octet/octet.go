package octet

import (
	"strings"

	"github.com/rs/zerolog/log"

	finopsdatatypes "github.com/krateoplatformops/finops-data-types/api/v1"

	csvhandler "github.com/krateoplatformops/finops-prometheus-exporter/internal/handlers/csv"
	jsonhandler "github.com/krateoplatformops/finops-prometheus-exporter/internal/handlers/json"
)

type OctetHandler struct{}

func (r *OctetHandler) Resolve(config finopsdatatypes.ExporterScraperConfig, data []byte) ([]byte, error) {
	log.Logger.Warn().Msg("Generic Content-Type: inferring from URL extension")
	switch {
	case strings.Contains(strings.ToLower(config.Spec.ExporterConfig.API.Path), "csv"):
		log.Logger.Warn().Msg("Generic Content-Type: inferring from URL extension, found CSV")
		handler := &csvhandler.CsvHandler{}
		return handler.Resolve(config, data)
	case strings.Contains(strings.ToLower(config.Spec.ExporterConfig.API.Path), "json"):
		log.Logger.Warn().Msg("Generic Content-Type: inferring from URL extension, found JSON")
		handler := &jsonhandler.JsonHandler{}
		return handler.Resolve(config, data)
	default:
		log.Logger.Warn().Msg("Generic Content-Type: inferring from URL extension, nothign found, trying text/csv")
		handler := &csvhandler.CsvHandler{}
		return handler.Resolve(config, data)
	}
}
