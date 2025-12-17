package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/krateoplatformops/finops-prometheus-exporter/internal/utils"
	"k8s.io/client-go/rest"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
	"gopkg.in/yaml.v3"

	finopsdatatypes "github.com/krateoplatformops/finops-data-types/api/v1"
	localendpoints "github.com/krateoplatformops/finops-prometheus-exporter/internal/helpers/kube/endpoints"
	localrequest "github.com/krateoplatformops/finops-prometheus-exporter/internal/helpers/kube/http/request"
	localstatus "github.com/krateoplatformops/finops-prometheus-exporter/internal/helpers/kube/http/response"
	"github.com/krateoplatformops/plumbing/endpoints"
	"github.com/krateoplatformops/plumbing/http/request"
)

type recordGaugeCombo struct {
	record        []string
	gauge         prometheus.Gauge
	thisIteration bool
}

func ParseConfigFile(file string) (finopsdatatypes.ExporterScraperConfig, *endpoints.Endpoint, error) {
	fileReader, err := os.OpenFile(file, os.O_RDONLY, 0600)
	if err != nil {
		return finopsdatatypes.ExporterScraperConfig{}, &endpoints.Endpoint{}, err
	}
	defer fileReader.Close()
	data, err := io.ReadAll(fileReader)
	if err != nil {
		return finopsdatatypes.ExporterScraperConfig{}, &endpoints.Endpoint{}, err
	}

	parse := finopsdatatypes.ExporterScraperConfig{}

	err = yaml.Unmarshal(data, &parse)
	if err != nil {
		return finopsdatatypes.ExporterScraperConfig{}, &endpoints.Endpoint{}, err
	}

	// Replace variables in API path
	parse.Spec.ExporterConfig.API.Path = utils.ReplaceVariables(parse.Spec.ExporterConfig.API.Path, parse.Spec.ExporterConfig.AdditionalVariables)

	rc, _ := rest.InClusterConfig()

	endpoint, err := localendpoints.FromSecret(context.Background(), rc, parse.Spec.ExporterConfig.API.EndpointRef)
	if err != nil {
		return finopsdatatypes.ExporterScraperConfig{}, &endpoints.Endpoint{}, err
	}
	// Replace variables in server URL
	endpoint.ServerURL = utils.ReplaceVariables(endpoint.ServerURL, parse.Spec.ExporterConfig.AdditionalVariables)
	return parse, &endpoint, nil

}

func makeAPIRequest(config finopsdatatypes.ExporterScraperConfig, endpoint *endpoints.Endpoint) []byte {
	res := &localstatus.Status{Code: 500}
	var err error
	var bodyData []byte

	for ok := true; ok; ok = (res.Code != 200) {
		opts := request.RequestOptions{
			Endpoint: endpoint,
			RequestInfo: request.RequestInfo{
				Path:    config.Spec.ExporterConfig.API.Path,
				Verb:    &config.Spec.ExporterConfig.API.Verb,
				Headers: config.Spec.ExporterConfig.API.Headers,
				Payload: &config.Spec.ExporterConfig.API.Payload,
			},
			ResponseHandler: func(rc io.ReadCloser) error {
				bodyData, _ = io.ReadAll(rc)
				return nil
			},
		}
		// log.Info().Msgf("Parsed Endpoint awsAccessKey: %s", opts.Endpoint.AwsAccessKey)
		// log.Info().Msgf("Parsed Endpoint awsSecretKey: %s", opts.Endpoint.AwsSecretKey)
		// log.Info().Msgf("Parsed Endpoint awsRegion: %s", opts.Endpoint.AwsRegion)
		// log.Info().Msgf("Parsed Endpoint awsService: %s", opts.Endpoint.AwsService)

		// log.Info().Msgf("Endpoint HasAwsAuth: %t", opts.Endpoint.HasAwsAuth())

		res = localrequest.Do(context.Background(), opts)

		if res.Code != 200 {
			log.Warn().Msgf("Received status code %d", res.Code)
			log.Warn().Msgf("Error - Body: %s", res.Message)

			log.Logger.Warn().Msgf("Retrying connection in 5s...")
			time.Sleep(5 * time.Second)

			log.Logger.Info().Msgf("Parsing Endpoint again...")
			rc, _ := rest.InClusterConfig()
			endpoint, err := localendpoints.FromSecret(context.Background(), rc, config.Spec.ExporterConfig.API.EndpointRef)
			if err != nil {
				continue
			}
			endpoint.ServerURL = utils.ReplaceVariables(endpoint.ServerURL, config.Spec.ExporterConfig.AdditionalVariables)
		}
	}

	data := bodyData

	// "Content-Encoding: gzip" is automatically handlded by go's HTTP transport
	log.Logger.Debug().Msgf("Content-Type: %s", strings.ToLower(res.Header.Get("Content-Type")))
	log.Logger.Debug().Msgf("Content-Length: %s", strings.ToLower(res.Header.Get("Content-Length")))

	handler, ok := utils.GetHandler(strings.ToLower(res.Header.Get("Content-Type")))
	if !ok {
		log.Error().Err(err).Msgf("Content-Type not supported: %s", strings.ToLower(res.Header.Get("Content-Type")))
	}
	jsonDataParsed, err := handler.Resolve(config, utils.TrapBOM(data))
	if err != nil {
		log.Error().Err(err).Msg("error resolving data")
	}
	return jsonDataParsed
}

func getRecordsFromFile(data []byte) [][]string {
	reader := csv.NewReader(bytes.NewReader(data))
	reader.LazyQuotes = true

	records, err := reader.ReadAll()
	if err != nil {
		log.Logger.Warn().Err(err).Msg("error while reading file")
		return nil
	}

	return records
}

func updatedMetrics(registry *prometheus.Registry, prometheusMetrics map[string]recordGaugeCombo) {
	for {
		config, endpoint, err := ParseConfigFile("/config/config.yaml")
		if err != nil {
			log.Logger.Error().Err(err).Msg("error while parsing configuration, trying again in 5s...")
			time.Sleep(5 * time.Second)
			continue
		}
		data := makeAPIRequest(config, endpoint)
		records := getRecordsFromFile(data)

		// Obtain various indexes
		// BilledCost for value of metric
		valueIndex := -1
		if strings.ToLower(config.Spec.ExporterConfig.MetricType) == "cost" {
			valueIndex, err = utils.GetIndexOf(records, "BilledCost")
			if err != nil {
				log.Logger.Warn().Err(err).Msg("error while selecting column BilledCost, retrying...")
				time.Sleep(5 * time.Second)
				continue
			}
		} else if strings.ToLower(config.Spec.ExporterConfig.MetricType) == "resource" {
			valueIndex = 3
		} else if strings.ToLower(config.Spec.ExporterConfig.MetricType) == "generic" {
			if config.Spec.ExporterConfig.Generic != nil {
				valueIndex = config.Spec.ExporterConfig.Generic.ValueColumnIndex
			} else {
				log.Logger.Error().Err(err).Msg("Generic object cannot be null with generic metric type, halting")
			}
		} else {
			log.Logger.Error().Err(err).Msgf("Unknow metric type: %s, trying again in 5s...", config.Spec.ExporterConfig.MetricType)
			time.Sleep(5 * time.Second)
			continue
		}

		notFound := true
		log.Info().Msgf("Analyzing %d records...", len(records))
		for i, record := range records {
			// Skip header line
			if i == 0 {
				continue
			}

			notFound = true
			if _, ok := prometheusMetrics[utils.CustomJoinWihtoutX(records[0], record, " ")]; ok {
				metricValue, err := strconv.ParseFloat(record[valueIndex], 64)
				if err != nil {
					log.Logger.Warn().Err(err).Msgf("skipping this record for this iteration, error while parsing metric value: %s", record[valueIndex])
					continue
				}
				gaugeObj := prometheusMetrics[utils.CustomJoinWihtoutX(records[0], record, " ")]
				gaugeObj.gauge.Set(metricValue)
				gaugeObj.thisIteration = true
				prometheusMetrics[utils.CustomJoinWihtoutX(records[0], record, " ")] = gaugeObj
				notFound = false
			}

			if notFound {
				labels := prometheus.Labels{}
				for j, value := range record {
					if strings.ToLower(config.Spec.ExporterConfig.MetricType) == "cost" && strings.HasPrefix(records[0][j], "x_") {
						continue
					}
					if !strings.Contains(records[0][j], "Tags") {
						labels[records[0][j]] = value
					} else {
						replacer := strings.NewReplacer("{", "", "}", "", "=", ":", ",", ";", "\"", "")
						labels[records[0][j]] = replacer.Replace(value)
					}
				}

				name := ""
				if strings.ToLower(config.Spec.ExporterConfig.MetricType) == "cost" {
					name = "billed_cost"
				} else if strings.ToLower(config.Spec.ExporterConfig.MetricType) == "resource" {
					name = strings.ReplaceAll(strings.ToLower(labels[records[0][1]]), " ", "_")
				} else if strings.ToLower(config.Spec.ExporterConfig.MetricType) == "generic" {
					name = config.Spec.ExporterConfig.Generic.MetricName
				}
				newMetricsRow := prometheus.NewGauge(prometheus.GaugeOpts{
					Name:        name,
					ConstLabels: labels,
				})
				metricValue, err := strconv.ParseFloat(records[i][valueIndex], 64)
				if err != nil {
					log.Logger.Warn().Err(err).Msgf("skipping this record for this iteration, error while parsing metric value: %s", records[i][valueIndex])
					continue
				}
				newMetricsRow.Set(metricValue)
				prometheusMetrics[utils.CustomJoinWihtoutX(records[0], record, " ")] = recordGaugeCombo{record: record, gauge: newMetricsRow, thisIteration: true}
				registry.MustRegister(newMetricsRow)
			}
		}

		for key, gaugeObj := range prometheusMetrics {
			if !gaugeObj.thisIteration {
				registry.Unregister(gaugeObj.gauge)
				delete(prometheusMetrics, key)
			} else {
				gaugeObj.thisIteration = false
				prometheusMetrics[key] = gaugeObj
			}
		}
		log.Debug().Msgf("Polling interval set to %s, starting sleep...", config.Spec.ExporterConfig.PollingInterval.Duration.String())
		time.Sleep(config.Spec.ExporterConfig.PollingInterval.Duration)
	}
}

func main() {
	registry := prometheus.NewRegistry()
	go updatedMetrics(registry, map[string]recordGaugeCombo{})

	handler := promhttp.HandlerFor(registry, promhttp.HandlerOpts{})

	http.Handle("/metrics", handler)
	http.ListenAndServe(":2112", nil)
}
