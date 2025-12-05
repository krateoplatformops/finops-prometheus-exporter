package handlers

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"reflect"
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
