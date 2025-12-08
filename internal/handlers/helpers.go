package handlers

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	finopsdatatypes "github.com/krateoplatformops/finops-data-types/api/v1"
	configmetrics "github.com/krateoplatformops/finops-prometheus-exporter/internal/config"
	"github.com/rs/zerolog/log"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TryParseResponseAsFocusJSON(jsonData []byte) ([]byte, error) {
	var focusConfigList finopsdatatypes.FocusConfigList
	err := json.Unmarshal(jsonData, &focusConfigList)
	if err != nil {
		log.Logger.Warn().Err(err).Msg("Parsing failed")
		//log.Logger.Info().Msg(string(jsonData))
		return []byte{}, err
	}

	return []byte(GetOutputStrFOCUS(focusConfigList)), nil
}

func GetOutputStrFOCUS(configList finopsdatatypes.FocusConfigList) string {
	outputStr := ""
	for i, config := range configList.Items {
		v := reflect.ValueOf(config.Spec.FocusSpec)

		if i == 0 {
			for i := 0; i < v.NumField(); i++ {
				outputStr += v.Type().Field(i).Name + ","
			}
			outputStr = strings.TrimSuffix(outputStr, ",") + "\n"
		}

		for i := 0; i < v.NumField(); i++ {
			outputStr += GetStringValue(v.Field(i).Interface()) + ","
		}
		outputStr = strings.TrimSuffix(outputStr, ",") + "\n"
	}
	outputStr = strings.TrimSuffix(outputStr, "\n")
	log.Logger.Info().Msg(outputStr)
	return outputStr
}

func TryParseResponseAsMetricsJSON(jsonData []byte, config finopsdatatypes.ExporterScraperConfig) ([]byte, error) {
	data := configmetrics.Metrics{}
	err := json.Unmarshal(jsonData, &data)
	if err != nil {
		log.Logger.Error().Err(err).Msg("error decoding metrics response")
		if e, ok := err.(*json.SyntaxError); ok {
			log.Logger.Error().Msgf("syntax error at byte offset %d", e.Offset)
		}
		log.Logger.Info().Msgf("response: %q", jsonData)
		log.Logger.Error().Err(err).Msg("error while reading file")
		return nil, err
	}
	return []byte(GetOutputStrMetrics(data, config)), nil
}

func TryParseUnknownJSONToCSV(jsonData []byte, config finopsdatatypes.ExporterScraperConfig) ([]byte, error) {
	var arrayRecords []map[string]interface{}
	err := json.Unmarshal(jsonData, &arrayRecords)
	if err != nil {
		var wrapper map[string]interface{}
		err2 := json.Unmarshal(jsonData, &wrapper)
		if err2 != nil {
			log.Logger.Error().Err(err2).Msg("error decoding input JSON")
			if e, ok := err2.(*json.SyntaxError); ok {
				log.Logger.Error().Msgf("syntax error at byte offset %d", e.Offset)
			}
			log.Logger.Info().Msgf("response: %q", jsonData)
			return nil, err2
		}

		found := false
		for _, v := range wrapper {
			arr, ok := v.([]interface{})
			if ok {
				for _, item := range arr {
					m, ok := item.(map[string]interface{})
					if !ok {
						continue
					}
					arrayRecords = append(arrayRecords, m)
					found = true
				}
				if found {
					break
				}
			}
		}

		if !found {
			log.Logger.Error().Msg("JSON object does not contain an array of objects")
			return nil, fmt.Errorf("no array of objects found in top-level object")
		}
	}

	if len(arrayRecords) == 0 {
		log.Logger.Warn().Msg("JSON contains no records")
		return []byte(""), nil
	}

	var b strings.Builder
	w := csv.NewWriter(&b)

	header := make([]string, 0, len(arrayRecords[0]))
	for k := range arrayRecords[0] {
		header = append(header, k)
	}

	if err := w.Write(header); err != nil {
		log.Logger.Error().Err(err).Msg("failed writing CSV header")
		return nil, err
	}

	for _, rec := range arrayRecords {
		row := make([]string, len(header))
		for i, key := range header {
			if rec[key] != nil {
				row[i] = fmt.Sprint(rec[key])
			}
		}
		if err := w.Write(row); err != nil {
			log.Logger.Error().Err(err).Msg("failed writing CSV row")
			return nil, err
		}
	}

	w.Flush()

	if err := w.Error(); err != nil {
		log.Logger.Error().Err(err).Msg("CSV writer error")
		return nil, err
	}

	return []byte(b.String()), nil
}

func GetOutputStrMetrics(configList configmetrics.Metrics, config finopsdatatypes.ExporterScraperConfig) string {
	stringCSV := "ResourceId,metricName,timestamp,average,unit\n"
	for _, value := range configList.Value {
		for _, timeseries := range value.Timeseries {
			for _, metric := range timeseries.Data {
				stringCSV += config.Spec.ExporterConfig.AdditionalVariables["ResourceId"] + "," + value.Name.Value + "," + metric.Timestamp.Format(time.RFC3339) + "," + metric.Average.AsDec().String() + "," + value.Unit + "\n"
			}
		}
	}
	return strings.TrimSuffix(stringCSV, "\n")
}

func GetStringValue(value any) string {
	str, ok := value.(string)
	if ok {
		return str
	}

	integer, ok := value.(int)
	if ok {
		return strconv.FormatInt(int64(integer), 10)
	}

	integer64, ok := value.(int64)
	if ok {
		return strconv.FormatInt(integer64, 10)
	}

	resourceQuantity, ok := value.(resource.Quantity)
	if ok {
		return resourceQuantity.AsDec().String()
	}

	metav1Time, ok := value.(metav1.Time)
	if ok {
		return metav1Time.Format(time.RFC3339)
	}

	tags, ok := value.([]finopsdatatypes.TagsType)
	if ok {
		res := ""
		for _, tag := range tags {
			res += tag.Key + "=" + tag.Value + ";"
		}
		return strings.TrimSuffix(res, ";")
	}

	return ""
}

// Prometheus parsing

// PrometheusResponse represents the top-level Prometheus API response
type PrometheusResponse struct {
	Status string         `json:"status"`
	Data   PrometheusData `json:"data"`
}

// PrometheusData contains the result type and results array
type PrometheusData struct {
	ResultType string             `json:"resultType"`
	Result     []PrometheusResult `json:"result"`
}

// PrometheusResult represents a single metric result
type PrometheusResult struct {
	Metric map[string]string `json:"metric"`
	Value  []interface{}     `json:"value,omitempty"`  // For instant queries [timestamp, value]
	Values [][]interface{}   `json:"values,omitempty"` // For range queries [[timestamp, value], ...]
}

// TryParseUnknownJSONToPrometheusCSV attempts to parse JSON as Prometheus format and convert to CSV
func TryParseUnknownJSONToPrometheusCSV(jsonData []byte, config finopsdatatypes.ExporterScraperConfig) ([]byte, error) {
	// Try to unmarshal as Prometheus format
	var promResponse PrometheusResponse
	err := json.Unmarshal(jsonData, &promResponse)
	if err != nil {
		log.Logger.Debug().Err(err).Msg("Failed to parse as Prometheus format")
		return nil, err
	}

	// Validate it's actually Prometheus format
	if promResponse.Status == "" || promResponse.Data.ResultType == "" {
		return nil, fmt.Errorf("not a valid Prometheus response format")
	}

	// Convert to CSV
	csv := PrometheusToCSV(promResponse, config)
	if csv == "" {
		return nil, fmt.Errorf("no data in Prometheus response")
	}

	log.Logger.Info().Msg("Successfully parsed as Prometheus format")
	return []byte(csv), nil
}

// PrometheusToCSV converts a Prometheus response to CSV format
func PrometheusToCSV(response PrometheusResponse, config finopsdatatypes.ExporterScraperConfig) string {
	if len(response.Data.Result) == 0 {
		return ""
	}

	var output strings.Builder

	// Determine if we have instant or range query results
	hasValues := len(response.Data.Result) > 0 && len(response.Data.Result[0].Values) > 0
	hasValue := len(response.Data.Result) > 0 && len(response.Data.Result[0].Value) > 0

	// Collect all unique metric labels for header
	labelSet := make(map[string]bool)
	for _, result := range response.Data.Result {
		for key := range result.Metric {
			labelSet[key] = true
		}
	}

	// Create ordered label list and sanitized mapping for consistent column order
	labels := make([]string, 0, len(labelSet))
	for label := range labelSet {
		labels = append(labels, label)
	}

	// Sort labels for consistent output
	sort.Strings(labels)

	// Create a mapping from original labels to sanitized labels, handling duplicates
	labelMapping := make(map[string]string)
	usedSanitized := make(map[string]int)

	for _, label := range labels {
		sanitized := sanitizePrometheusLabel(label)

		// Handle duplicate sanitized names
		if count, exists := usedSanitized[sanitized]; exists {
			usedSanitized[sanitized] = count + 1
			sanitized = fmt.Sprintf("%s_%d", sanitized, count)
		} else {
			usedSanitized[sanitized] = 1
		}

		labelMapping[label] = sanitized
	}

	// Write CSV header - value comes before timestamp
	for _, label := range labels {
		output.WriteString(labelMapping[label])
		output.WriteString(",")
	}
	output.WriteString("value,timestamp\n")

	// Process each result
	for _, result := range response.Data.Result {
		// Handle range queries (multiple timestamp-value pairs)
		if hasValues {
			for _, valuePoint := range result.Values {
				if len(valuePoint) != 2 {
					continue
				}

				// Write metric labels using the original label order
				for _, label := range labels {
					if val, exists := result.Metric[label]; exists {
						output.WriteString(val)
					}
					output.WriteString(",")
				}

				// Write value and timestamp (swapped order)
				value := formatPrometheusValue(valuePoint[1])
				timestamp := formatPrometheusTimestamp(valuePoint[0])
				output.WriteString(value)
				output.WriteString(",")
				output.WriteString(timestamp)
				output.WriteString("\n")
			}
		} else if hasValue {
			// Handle instant queries (single timestamp-value pair)
			if len(result.Value) == 2 {
				// Write metric labels using the original label order
				for _, label := range labels {
					if val, exists := result.Metric[label]; exists {
						output.WriteString(val)
					}
					output.WriteString(",")
				}

				// Write value and timestamp (swapped order)
				value := formatPrometheusValue(result.Value[1])
				timestamp := formatPrometheusTimestamp(result.Value[0])
				output.WriteString(value)
				output.WriteString(",")
				output.WriteString(timestamp)
				output.WriteString("\n")
			}
		}
	}

	return strings.TrimSuffix(output.String(), "\n")
}

// formatPrometheusTimestamp converts Prometheus timestamp to RFC3339 format
func formatPrometheusTimestamp(ts interface{}) string {
	switch v := ts.(type) {
	case float64:
		// Prometheus timestamps are Unix timestamps in seconds
		t := time.Unix(int64(v), 0)
		return t.Format(time.RFC3339)
	case int64:
		t := time.Unix(v, 0)
		return t.Format(time.RFC3339)
	default:
		return fmt.Sprint(v)
	}
}

// formatPrometheusValue converts Prometheus value to string
func formatPrometheusValue(val interface{}) string {
	switch v := val.(type) {
	case string:
		return v
	case float64:
		return fmt.Sprintf("%f", v)
	case int64:
		return fmt.Sprintf("%d", v)
	default:
		return fmt.Sprint(v)
	}
}

// sanitizePrometheusLabel converts Prometheus label names to valid CSV column names
func sanitizePrometheusLabel(label string) string {
	// Remove leading and trailing underscores (common in Prometheus internal labels like __name__)
	label = strings.Trim(label, "_")

	// Replace remaining problematic characters
	// Prometheus allows: [a-zA-Z_][a-zA-Z0-9_]*
	// CSV prefers alphanumeric and underscores without leading/trailing underscores

	var result strings.Builder
	for _, r := range label {
		if r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			result.WriteRune(r)
		} else if r == '-' {
			// Convert hyphens to underscores
			result.WriteRune('_')
		} else {
			// Replace other special characters with underscore
			result.WriteRune('_')
		}
	}

	sanitized := result.String()

	// Ensure the result is not empty and doesn't start with a number
	if sanitized == "" {
		return "label"
	}
	if sanitized[0] >= '0' && sanitized[0] <= '9' {
		sanitized = "label_" + sanitized
	}

	return sanitized
}
