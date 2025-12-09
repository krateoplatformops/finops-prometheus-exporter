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
	tests := []struct {
		name       string
		jsonInput  string
		expectErr  bool
		expectRows int
	}{
		{
			name: "instant vector query (simple)",
			jsonInput: `{
				"status": "success",
				"data": {
					"resultType": "vector",
					"result": [
						{
							"metric": { "__name__": "cpu_usage", "job": "test" },
							"value": [1765210903.436, "0.5"]
						}
					]
				}
			}`,
			expectRows: 1,
		},
		{
			name: "range matrix query",
			jsonInput: `{
				"status": "success",
				"data": {
					"resultType": "matrix",
					"result": [
						{
							"metric": { "__name__": "cpu_usage", "job": "test" },
							"values": [
								[1765210903, "1.5"],
								[1765210963, "2.5"],
								[1765211023, "3.5"]
							]
						}
					]
				}
			}`,
			expectRows: 3,
		},
		{
			name: "real prometheus payload (kubelet cadvisor)",
			jsonInput: `{
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
							"value": [1765210903.436, "0.744565"]
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
							"value": [1765210903.436, "0.010262"]
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
							"value": [1765210903.436, "2.177452"]
						}
					]
				}
			}`,
			expectRows: 3,
		},
		{
			name: "invalid prometheus json",
			jsonInput: `{
				"status": "success",
				"data": {
					"resultType": "vector"
				}
			}`,
			expectErr: true,
		},
		{
			name:      "not prometheus json",
			jsonInput: `{"foo":"bar"}`,
			expectErr: true,
		},
		{
			name: "empty results",
			jsonInput: `{
				"status": "success",
				"data": {
					"resultType": "vector",
					"result": []
				}
			}`,
			expectErr: true,
		},
	}

	config := finopsdatatypes.ExporterScraperConfig{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := TryParseUnknownJSONToPrometheusCSV([]byte(tt.jsonInput), config)

			if tt.expectErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			r := csv.NewReader(strings.NewReader(string(result)))
			records, err := r.ReadAll()
			if err != nil {
				t.Fatalf("invalid CSV: %v", err)
			}

			if len(records)-1 != tt.expectRows {
				t.Fatalf("expected %d rows, got %d", tt.expectRows, len(records)-1)
			}

			header := records[0]
			valueIdx, tsIdx := -1, -1

			for i, h := range header {
				if h == "value" {
					valueIdx = i
				}
				if h == "timestamp" {
					tsIdx = i
				}
			}

			if valueIdx == -1 || tsIdx == -1 {
				t.Fatalf("missing value or timestamp columns")
			}

			for i := 1; i < len(records); i++ {
				row := records[i]

				if len(row) != len(header) {
					t.Fatalf("row %d column mismatch", i)
				}

				if _, err := strconv.ParseFloat(row[valueIdx], 64); err != nil {
					t.Fatalf("row %d invalid value: %v", i, err)
				}

				if _, err := time.Parse(time.RFC3339Nano, row[tsIdx]); err != nil {
					t.Fatalf("row %d invalid timestamp: %v", i, err)
				}
			}
		})
	}
}
