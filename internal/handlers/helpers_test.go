package handlers

import (
	"encoding/csv"
	"strconv"
	"strings"
	"testing"
	"time"

	finopsdatatypes "github.com/krateoplatformops/finops-data-types/api/v1"
	"github.com/rs/zerolog/log"
)

func TestTryParseUnknownJSONToCSV(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected []string // multiple valid CSV outputs for unordered header
	}{
		{
			name: "simple two rows",
			input: []byte(`
            [
              { "test": "name",   "value": 0.34 },
              { "test": "name2",  "value": 1.40 }
            ]`),
			expected: []string{
				"test,value\nname,0.34\nname2,1.4\n",
				"value,test\n0.34,name\n1.4,name2\n",
			},
		},
		{
			name: "single object",
			input: []byte(`
            [
              { "foo": "bar", "count": 10 }
            ]`),
			expected: []string{
				"foo,count\nbar,10\n",
				"count,foo\n10,bar\n",
			},
		},
		{
			name:  "empty array",
			input: []byte(`[]`),
			expected: []string{
				"",
			},
		},
		{
			name: "random wrapper",
			input: []byte(`
            {
              "randomwrapper": [
                { "test": "name",   "value": 0.34 },
                { "test": "name2",  "value": 1.40 }
              ]
            }`),
			expected: []string{
				"test,value\nname,0.34\nname2,1.4\n",
				"value,test\n0.34,name\n1.4,name2\n",
			},
		},
		{
			name: "random wrapper with additional labels",
			input: []byte(`
            {
              "randomwrapper": [
                { "test": "name",   "value": 0.34 },
                { "test": "name2",  "value": 1.40 }
              ],
			  "toplevellabel": "value"
            }`),
			expected: []string{
				"test,value\nname,0.34\nname2,1.4\n",
				"value,test\n0.34,name\n1.4,name2\n",
			},
		},
	}

	failTests := []struct {
		name     string
		input    []byte
		expected []string // multiple valid CSV outputs for unordered header
	}{
		{
			name: "multiple top level variables without wrapper",
			input: []byte(`
            {
              "random": "value",
			  "empty": "test"
            }`),
			expected: []string{
				"",
			},
		},
	}

	config := finopsdatatypes.ExporterScraperConfig{}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			csvData, err := TryParseUnknownJSONToCSV(tc.input, config)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			output := strings.TrimSpace(string(csvData))
			log.Logger.Debug().Msgf("output %s", output)
			matched := false
			for _, exp := range tc.expected {
				if output == strings.TrimSpace(exp) {
					matched = true
					break
				}
			}

			if !matched {
				t.Fatalf("output mismatch.\nGot:\n%q\nExpected one of:\n%q", output, tc.expected)
			}
		})
	}

	for _, tc := range failTests {
		t.Run(tc.name, func(t *testing.T) {
			csvData, err := TryParseUnknownJSONToCSV(tc.input, config)
			if err == nil {
				t.Fatal("no error detected in fail test")
			}

			output := strings.TrimSpace(string(csvData))

			log.Logger.Debug().Msgf("output %s", output)
			matched := false
			for _, exp := range tc.expected {
				if output == strings.TrimSpace(exp) {
					matched = true
					break
				}
			}

			if !matched {
				t.Fatalf("output mismatch.\nGot:\n%q\nExpected one of:\n%q", output, tc.expected)
			}
		})
	}
}

func TestTryParseUnknownJSONToPrometheusCSV(t *testing.T) {
	// Sample Prometheus response JSON
	jsonData := []byte(`{
  "status": "success",
  "data": {
    "resultType": "vector",
    "result": [
      {
        "metric": {
          "__name__": "container_cpu_usage_seconds_total",
          "cpu": "total",
          "endpoint": "https-metrics",
          "id": "/kubelet.slice/kubelet-kubepods.slice/kubelet-kubepods-besteffort.slice/kubelet-kubepods-besteffort-pod0c52bc54_389c_4774_9356_d4ef412ed32e.slice",
          "instance": "172.18.0.2:10250",
          "job": "kubelet",
          "metrics_path": "/metrics/cadvisor",
          "namespace": "krateo-system",
          "node": "krateo-quickstart-worker",
          "pod": "finops-database-handler-7877644c87-2hkrs",
          "service": "kind-prometheus-kube-prome-kubelet"
        },
        "value": [
          1765210903.436,
          "0.744565"
        ]
      },
      {
        "metric": {
          "__name__": "container_cpu_usage_seconds_total",
          "cpu": "total",
          "endpoint": "https-metrics",
          "id": "/kubelet.slice/kubelet-kubepods.slice/kubelet-kubepods-besteffort.slice/kubelet-kubepods-besteffort-pod0c52bc54_389c_4774_9356_d4ef412ed32e.slice/cri-containerd-83c68e46b12c88664d61c11594f57e3795d09591e71f4190614efef32e0a8986.scope",
          "image": "registry.k8s.io/pause:3.10",
          "instance": "172.18.0.2:10250",
          "job": "kubelet",
          "metrics_path": "/metrics/cadvisor",
          "name": "83c68e46b12c88664d61c11594f57e3795d09591e71f4190614efef32e0a8986",
          "namespace": "krateo-system",
          "node": "krateo-quickstart-worker",
          "pod": "finops-database-handler-7877644c87-2hkrs",
          "service": "kind-prometheus-kube-prome-kubelet"
        },
        "value": [
          1765210903.436,
          "0.010262"
        ]
      },
      {
        "metric": {
          "__name__": "container_cpu_usage_seconds_total",
          "cpu": "total",
          "endpoint": "https-metrics",
          "id": "/kubelet.slice/kubelet-kubepods.slice/kubelet-kubepods-besteffort.slice/kubelet-kubepods-besteffort-pod12e6a5d7_cb0f_42c5_b2da_94f99fb8fb31.slice",
          "instance": "172.18.0.2:10250",
          "job": "kubelet",
          "metrics_path": "/metrics/cadvisor",
          "namespace": "krateo-system",
          "node": "krateo-quickstart-worker",
          "pod": "finops-database-handler-uploader-57b55f468f-4h4sk",
          "service": "kind-prometheus-kube-prome-kubelet"
        },
        "value": [
          1765210903.436,
          "2.177452"
        ]
      }
    ]
  }
}`)

	// Create a minimal config (adjust based on your actual struct definition)
	config := finopsdatatypes.ExporterScraperConfig{}

	// Parse the JSON
	result, err := TryParseUnknownJSONToPrometheusCSV(jsonData, config)
	if err != nil {
		t.Fatalf("Failed to parse Prometheus JSON: %v", err)
	}

	// Parse the CSV properly
	reader := csv.NewReader(strings.NewReader(string(result)))
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("Failed to parse CSV: %v", err)
	}

	t.Logf("header: %s", records[0])

	// Find the value and timestamp column indices
	header := records[0]
	valueIdx := -1
	timestampIdx := -1
	for i, col := range header {
		if col == "value" {
			valueIdx = i
		}
		if col == "timestamp" {
			timestampIdx = i
		}
	}

	if valueIdx == -1 || timestampIdx == -1 {
		t.Fatal("Missing value or timestamp columns")
	}

	// ✅ This would catch the bug:
	// Try to parse values in the value column as floats
	for i := 1; i < len(records); i++ {
		valueStr := records[i][valueIdx]
		_, err := strconv.ParseFloat(valueStr, 64)
		if err != nil {
			t.Errorf("Row %d: Failed to parse value '%s' as float: %v",
				i, valueStr, err)
		}

		// Verify timestamp is RFC3339 format
		timestampStr := records[i][timestampIdx]
		_, err = time.Parse(time.RFC3339, timestampStr)
		if err != nil {
			t.Errorf("Row %d: Failed to parse timestamp '%s' as RFC3339: %v",
				i, timestampStr, err)
		}
	}
}

func TestTryParseUnknownJSONToPrometheusCSV_InvalidJSON(t *testing.T) {
	invalidJSON := []byte(`{"not": "prometheus", "format": "at all"}`)
	config := finopsdatatypes.ExporterScraperConfig{}

	_, err := TryParseUnknownJSONToPrometheusCSV(invalidJSON, config)
	if err == nil {
		t.Fatal("Expected error for invalid Prometheus JSON, got nil")
	}

	t.Logf("Got expected error: %v", err)
}

func TestTryParseUnknownJSONToPrometheusCSV_EmptyResults(t *testing.T) {
	emptyResultsJSON := []byte(`{
		"status": "success",
		"data": {
			"resultType": "vector",
			"result": []
		}
	}`)
	config := finopsdatatypes.ExporterScraperConfig{}

	result, err := TryParseUnknownJSONToPrometheusCSV(emptyResultsJSON, config)
	if err == nil {
		t.Fatal("Expected error for empty results, got nil")
	}

	t.Logf("Got expected error: %v", err)
	t.Logf("Result: %s", string(result))
}

func TestPrometheusToCSV_RangeQuery(t *testing.T) {
	// Test with range query format (values instead of value)
	rangeQueryJSON := []byte(`{
		"status": "success",
		"data": {
			"resultType": "matrix",
			"result": [
				{
					"metric": {
						"__name__": "test_metric",
						"job": "test"
					},
					"values": [
						[1765210903, "1.5"],
						[1765210963, "2.5"],
						[1765211023, "3.5"]
					]
				}
			]
		}
	}`)

	config := finopsdatatypes.ExporterScraperConfig{}
	result, err := TryParseUnknownJSONToPrometheusCSV(rangeQueryJSON, config)
	if err != nil {
		t.Fatalf("Failed to parse range query: %v", err)
	}

	csvOutput := string(result)
	t.Logf("Range Query CSV Output:\n%s", csvOutput)

	// Verify we have 3 data rows for the 3 timestamps
	lines := strings.Split(csvOutput, "\n")
	if len(lines) != 4 { // header + 3 data rows
		t.Errorf("Expected 4 lines for range query, got %d", len(lines))
	}

	// Verify all three values are present
	expectedValues := []string{"1.5", "2.5", "3.5"}
	for _, val := range expectedValues {
		if !strings.Contains(csvOutput, val) {
			t.Errorf("Expected value %s not found in range query output", val)
		}
	}
}
