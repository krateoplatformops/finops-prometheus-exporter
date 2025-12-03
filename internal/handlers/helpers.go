package handlers

import (
	"encoding/json"
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
