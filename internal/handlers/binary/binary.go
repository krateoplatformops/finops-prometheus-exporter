package binary

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"strings"

	"github.com/rs/zerolog/log"

	finopsdatatypes "github.com/krateoplatformops/finops-data-types/api/v1"

	octethandler "github.com/krateoplatformops/finops-prometheus-exporter/internal/handlers/octet"
)

type BinaryHandler struct{}

func (r *BinaryHandler) Resolve(config finopsdatatypes.ExporterScraperConfig, data []byte) ([]byte, error) {
	log.Logger.Warn().Msg("Generic Content-Type: inferring from URL extension")
	switch {
	case strings.Contains(strings.ToLower(config.Spec.ExporterConfig.API.Path), ".gz"):
		log.Logger.Warn().Msg("Generic Content-Type: inferring from URL extension, found GZ")
		log.Logger.Info().Msg("Generic Content-Type: gz compression, decompressing...")
		gr, err := gzip.NewReader(bytes.NewBuffer(data))
		if err != nil {
			return nil, fmt.Errorf("could not obtain new gzip reader: %v", err)
		}
		defer gr.Close()
		decompressedData, err := io.ReadAll(gr)
		if err != nil {
			return nil, fmt.Errorf("could not decompress with gzip: %v", err)
		}
		handler := &octethandler.OctetHandler{}
		return handler.Resolve(config, decompressedData)
	default:
		log.Logger.Warn().Msg("Generic Content-Type: inferring from URL extension, nothign found, trying text/csv")
		handler := &octethandler.OctetHandler{}
		return handler.Resolve(config, data)
	}

}
