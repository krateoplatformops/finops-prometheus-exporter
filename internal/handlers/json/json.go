package json

import (
	"fmt"
	"strings"

	finopsdatatypes "github.com/krateoplatformops/finops-data-types/api/v1"
	helpers "github.com/krateoplatformops/finops-prometheus-exporter/internal/handlers"
	"github.com/rs/zerolog/log"
)

type JsonHandler struct{}

func (r *JsonHandler) Resolve(config finopsdatatypes.ExporterScraperConfig, data []byte) ([]byte, error) {
	log.Logger.Info().Msg("Detected json content-type")
	var jsonDataParsed []byte
	var err error
	if strings.ToLower(config.Spec.ExporterConfig.MetricType) == "cost" {
		jsonDataParsed, err = helpers.TryParseResponseAsFocusJSON(data)
	} else if strings.ToLower(config.Spec.ExporterConfig.MetricType) == "resource" {
		jsonDataParsed, err = helpers.TryParseResponseAsMetricsJSON(data, config)
	} else if strings.ToLower(config.Spec.ExporterConfig.MetricType) == "generic" {
		jsonDataParsed, err = helpers.TryParseUnknownJSONToPrometheusCSV(data, config)
		if err != nil {
			// If Prometheus parsing fails, fall back to generic parser
			log.Logger.Debug().Msg("Prometheus parsing failed, trying generic JSON parser")
			jsonDataParsed, err = helpers.TryParseUnknownJSONToCSV(data, config)
		}
	} else {
		return nil, fmt.Errorf("could not handle metric type: %s, trying again in 5s", config.Spec.ExporterConfig.MetricType)
	}

	if err != nil {
		return nil, fmt.Errorf("an error has occured while parsing json data: %v", err)
	}
	return jsonDataParsed, nil
}
