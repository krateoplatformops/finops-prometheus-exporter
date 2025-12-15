package handlers

import (
	"bytes"
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
			if !ok {
				continue
			}
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

		if !found {
			log.Logger.Error().Msg("JSON object does not contain an array of objects")
			return nil, fmt.Errorf("no array of objects found in top-level object")
		}
	}

	if len(arrayRecords) == 0 {
		log.Logger.Warn().Msg("JSON contains no records")
		return []byte(""), nil
	}

	keySet := make(map[string]struct{})
	for _, rec := range arrayRecords {
		for k := range rec {
			keySet[k] = struct{}{}
		}
	}

	header := make([]string, 0, len(keySet))
	for k := range keySet {
		header = append(header, k)
	}
	sort.Strings(header)

	var b strings.Builder
	w := csv.NewWriter(&b)

	if err := w.Write(header); err != nil {
		log.Logger.Error().Err(err).Msg("failed writing CSV header")
		return nil, err
	}

	for _, rec := range arrayRecords {
		row := make([]string, len(header))
		for i, key := range header {
			if val, ok := rec[key]; ok && val != nil {
				row[i] = fmt.Sprint(val)
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

type PrometheusResponse struct {
	Status string         `json:"status"`
	Data   PrometheusData `json:"data"`
}

type PrometheusData struct {
	ResultType string             `json:"resultType"`
	Result     []PrometheusResult `json:"result"`
}

type PrometheusResult struct {
	Metric map[string]string `json:"metric"`
	Value  []interface{}     `json:"value,omitempty"`
	Values [][]interface{}   `json:"values,omitempty"`
}

func TryParseUnknownJSONToPrometheusCSV(
	jsonData []byte,
	config finopsdatatypes.ExporterScraperConfig,
) ([]byte, error) {

	var resp PrometheusResponse
	if err := json.Unmarshal(jsonData, &resp); err != nil {
		return nil, fmt.Errorf("not prometheus json")
	}

	// ✅ Strong validation
	if resp.Status != "success" {
		return nil, fmt.Errorf("not prometheus json")
	}

	if resp.Data.ResultType != "vector" && resp.Data.ResultType != "matrix" {
		return nil, fmt.Errorf("not prometheus json")
	}

	if len(resp.Data.Result) == 0 {
		return nil, fmt.Errorf("not prometheus json")
	}

	csvBytes, err := PrometheusToCSV(resp, config)
	if err != nil {
		return nil, err
	}

	return csvBytes, nil
}

func PrometheusToCSV(
	response PrometheusResponse,
	config finopsdatatypes.ExporterScraperConfig,
) ([]byte, error) {

	// Collect unique labels
	labelSet := map[string]struct{}{}
	for _, r := range response.Data.Result {
		for k := range r.Metric {
			labelSet[k] = struct{}{}
		}
	}

	if len(labelSet) == 0 {
		return nil, fmt.Errorf("no labels found")
	}

	labels := make([]string, 0, len(labelSet))
	for l := range labelSet {
		labels = append(labels, l)
	}
	sort.Strings(labels)

	// Sanitize headers
	labelMap := make(map[string]string)
	used := map[string]int{}

	for _, l := range labels {
		s := sanitizePrometheusLabel(l)
		if cnt, ok := used[s]; ok {
			cnt++
			used[s] = cnt
			s = fmt.Sprintf("%s_%d", s, cnt)
		} else {
			used[s] = 1
		}
		labelMap[l] = s
	}

	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)

	// Header
	header := make([]string, 0, len(labels)+2)
	for _, l := range labels {
		header = append(header, labelMap[l])
	}
	header = append(header, "value", "timestamp")
	if err := writer.Write(header); err != nil {
		return nil, err
	}

	// Rows
	for _, result := range response.Data.Result {

		emitRow := func(ts, val interface{}) {
			row := make([]string, 0, len(labels)+2)

			for _, l := range labels {
				row = append(row, result.Metric[l])
			}

			row = append(row,
				formatPrometheusValue(val),
				formatPrometheusTimestamp(ts),
			)

			_ = writer.Write(row)
		}

		if len(result.Values) > 0 {
			// Range query
			for _, pair := range result.Values {
				if len(pair) == 2 {
					emitRow(pair[0], pair[1])
				}
			}
		} else if len(result.Value) == 2 {
			// Instant query
			emitRow(result.Value[0], result.Value[1])
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return nil, err
	}

	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

// Helpers

func formatPrometheusTimestamp(ts interface{}) string {
	switch v := ts.(type) {
	case float64:
		secs := int64(v)
		nanos := int64((v - float64(secs)) * 1e9)
		return time.Unix(secs, nanos).UTC().Format(time.RFC3339Nano)
	case int64:
		return time.Unix(v, 0).UTC().Format(time.RFC3339Nano)
	case string:
		return v
	default:
		return fmt.Sprint(v)
	}
}

func formatPrometheusValue(val interface{}) string {
	switch v := val.(type) {
	case string:
		return v
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64)
	case int64:
		return strconv.FormatInt(v, 10)
	default:
		return fmt.Sprint(v)
	}
}

func sanitizePrometheusLabel(label string) string {
	if label == "__name__" {
		return "metric_name"
	}

	label = strings.Trim(label, "_")

	var b strings.Builder
	for _, r := range label {
		switch {
		case r == '_',
			r >= 'a' && r <= 'z',
			r >= 'A' && r <= 'Z',
			r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-':
			b.WriteRune('_')
		default:
			b.WriteRune('_')
		}
	}

	s := b.String()
	if s == "" {
		return "label"
	}

	if s[0] >= '0' && s[0] <= '9' {
		s = "label_" + s
	}

	return s
}
