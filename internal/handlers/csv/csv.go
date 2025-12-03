package csv

import finopsdatatypes "github.com/krateoplatformops/finops-data-types/api/v1"

type CsvHandler struct{}

func (r *CsvHandler) Resolve(config finopsdatatypes.ExporterScraperConfig, data []byte) ([]byte, error) {
	return data, nil
}
