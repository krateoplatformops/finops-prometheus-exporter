package utils

import (
	"bytes"
	"errors"
	"os"
	"regexp"
	"strings"

	"github.com/rs/zerolog/log"

	"github.com/krateoplatformops/finops-prometheus-exporter/internal/handlers"

	binaryhandler "github.com/krateoplatformops/finops-prometheus-exporter/internal/handlers/binary"
	csvhandler "github.com/krateoplatformops/finops-prometheus-exporter/internal/handlers/csv"
	jsonhandler "github.com/krateoplatformops/finops-prometheus-exporter/internal/handlers/json"
	octethandler "github.com/krateoplatformops/finops-prometheus-exporter/internal/handlers/octet"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

/*
* Function to remove the encoding bytes from a file.
* @param file The file to remove the encoding from.
 */
func TrapBOM(file []byte) []byte {
	return bytes.Trim(file, "\xef\xbb\xbf")
}

/*
* Given the records from the csv file, it returns the index of the "toFind" column.
* @param records The csv file as a 2D array of strings
* @param toFind the column to find
* @return the index of the "toFind" column
 */
func GetIndexOf(records [][]string, toFind string) (int, error) {
	log.Debug().Msgf("Looking for %s", toFind)
	if len(records) > 0 {
		for i, value := range records[0] {
			if strings.EqualFold(value, toFind) {
				return i, nil
			}
		}
	}
	return -1, errors.New(toFind + " not found")
}

func GetClientSet() (*kubernetes.Clientset, error) {
	inClusterConfig, err := rest.InClusterConfig()
	if err != nil {
		return &kubernetes.Clientset{}, err
	}

	inClusterConfig.APIPath = "/apis"
	inClusterConfig.GroupVersion = &schema.GroupVersion{Group: "finops.krateo.io", Version: "v1"}

	clientset, err := kubernetes.NewForConfig(inClusterConfig)
	if err != nil {
		return &kubernetes.Clientset{}, err
	}
	return clientset, nil
}

// replaceVariables replaces all variables in the format <variable> with their values
// from the additionalVariables map or from environment variables if the variable name is uppercase
func ReplaceVariables(text string, additionalVariables map[string]string) string {
	regex, _ := regexp.Compile("<.*?>")
	toReplaceRange := regex.FindStringIndex(text)

	for toReplaceRange != nil {
		// Extract variable name without the < > brackets
		varName := text[toReplaceRange[0]+1 : toReplaceRange[1]-1]

		// Get replacement value from additionalVariables
		varToReplace := additionalVariables[varName]

		// If the variable name is all uppercase, get value from environment
		if varToReplace == strings.ToUpper(varToReplace) {
			varToReplace = os.Getenv(varToReplace)
		}

		// Replace the variable in the text
		text = strings.Replace(text, text[toReplaceRange[0]:toReplaceRange[1]], varToReplace, -1)

		// Find next variable
		toReplaceRange = regex.FindStringIndex(text)
	}

	return text
}

func CustomJoinWihtoutX(headers []string, arrayToJoin []string, sep string) string {
	result := ""
	for i, value := range arrayToJoin {
		if !strings.HasPrefix(headers[i], "x_") {
			result += value + sep
		}
	}
	return result
}

func GetHandler(name string) (handlers.Handler, bool) {
	handlers := map[string]handlers.Handler{
		"text/csv":                 &csvhandler.CsvHandler{},
		"application/json":         &jsonhandler.JsonHandler{},
		"application/octet-stream": &octethandler.OctetHandler{},
		"binary/octet-stream":      &binaryhandler.BinaryHandler{},
	}
	for k, v := range handlers {
		if strings.Contains(name, k) {
			return v, true
		}
	}
	return nil, false
}
