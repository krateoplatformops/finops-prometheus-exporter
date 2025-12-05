package handlers

import (
	"strings"
	"testing"

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
