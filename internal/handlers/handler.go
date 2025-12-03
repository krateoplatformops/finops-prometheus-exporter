package handlers

import finopsdatatypes "github.com/krateoplatformops/finops-data-types/api/v1"

type Handler interface {
	Resolve(config finopsdatatypes.ExporterScraperConfig, data []byte) ([]byte, error)
}
