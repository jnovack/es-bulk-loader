// Package config reads and normalizes the run's file inputs: ordered named
// definitions (pipelines, policies, transforms), index settings and mappings,
// the create-index body and the mapping expectations checked before a load.
// It formats input only: it never queries the cluster, so callers pass in
// values such as the selected replica count.
package config

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/runlog"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/template"
)

// Definitions maps a resource name to its raw JSON definition. Map order is
// not file order: ReadNamedDefinitions returns the names as a separate slice.
type Definitions map[string]json.RawMessage

// MappingPlan is what a mappings file requires of an index's live mappings:
// the date_detection value when the file sets one, and the type of each root
// field that declares a type.
type MappingPlan struct {
	DateDetectionExpected *bool
	RootFieldTypes        map[string]string
}

// HasExpectations reports whether the plan checks anything at all.
func (p MappingPlan) HasExpectations() bool {
	return p.DateDetectionExpected != nil || len(p.RootFieldTypes) > 0
}

// BuildMappingPlan centralizes this code path so package behavior stays consistent.
// A read or parse failure of the mappings file is returned as that read
// site's own operation error; only a section that is not a JSON object fails
// as "Failed to build mapping preflight plan".
func BuildMappingPlan(ctx context.Context, mappingsFile string, variables template.Variables) (MappingPlan, error) {
	trimmed := strings.TrimSpace(mappingsFile)
	if trimmed == "" {
		return MappingPlan{}, nil
	}

	normalized, err := NormalizeIndexSection(ctx, trimmed, "mappings", variables)
	if err != nil {
		return MappingPlan{}, err
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(normalized), &parsed); err != nil {
		planErr := fmt.Errorf("parsing mappings preflight source: %w", err)
		return MappingPlan{}, runlog.Fail(runlog.FailureEvent(ctx).Str("path", mappingsFile), "Failed to build mapping preflight plan", planErr)
	}

	plan := MappingPlan{
		RootFieldTypes: make(map[string]string),
	}
	if value, ok := parsed["date_detection"]; ok {
		if expected, ok := value.(bool); ok {
			plan.DateDetectionExpected = &expected
		}
	}

	properties, _ := parsed["properties"].(map[string]any)
	for fieldName, raw := range properties {
		fieldMap, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		fieldType, _ := fieldMap["type"].(string)
		if strings.TrimSpace(fieldType) == "" {
			continue
		}
		plan.RootFieldTypes[fieldName] = fieldType
	}

	return plan, nil
}

// BuildCreateIndexBody formats the create-index request body from the
// settings and mappings files. replicas and replicasSource are chosen by the
// caller (explicit override or cluster topology, ADR 0001) before this call;
// config only formats them, so it never queries the cluster.
func BuildCreateIndexBody(ctx context.Context, replicas int, replicasSource, settingsFile, mappingsFile, defaultPipeline string, variables template.Variables) (string, error) {
	settings, err := NormalizeIndexSettings(ctx, settingsFile, defaultPipeline, replicas, replicasSource, variables)
	if err != nil {
		return "", err
	}
	mappings, err := NormalizeIndexSection(ctx, mappingsFile, "mappings", variables)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(`{"settings": %s, "mappings": %s}`, settings, mappings), nil
}

// NormalizeIndexSettings centralizes this code path so package behavior stays consistent.
func NormalizeIndexSettings(ctx context.Context, path, defaultPipeline string, replicas int, replicasSource string, variables template.Variables) (string, error) {
	settings := make(map[string]json.RawMessage)
	if path != "" {
		content, err := template.ReadFile(path, variables)
		if err != nil {
			return "", runlog.Fail(runlog.FailureEvent(ctx).Str("path", path), "Reading settings file", err)
		}

		var raw map[string]json.RawMessage
		if err := json.Unmarshal(content, &raw); err != nil {
			return "", runlog.Fail(runlog.FailureEvent(ctx).Str("path", path), "Parsing settings file", err)
		}

		source := raw
		if nested, ok := raw["settings"]; ok {
			source = nil
			if err := json.Unmarshal(nested, &source); err != nil {
				return "", runlog.Fail(runlog.FailureEvent(ctx).Str("path", path), "Parsing nested settings file", err)
			}
		}

		for key, value := range source {
			if key == "index" {
				var nested map[string]json.RawMessage
				if err := json.Unmarshal(value, &nested); err == nil {
					for nestedKey, nestedValue := range nested {
						normalizedKey := normalizeSettingKey(nestedKey)
						if _, exists := settings[normalizedKey]; exists {
							continue
						}
						settings[normalizedKey] = nestedValue
					}
					continue
				}
			}

			settings[normalizeSettingKey(key)] = value
		}
	}

	if defaultPipeline != "" {
		if _, ok := settings["default_pipeline"]; !ok {
			settings["default_pipeline"] = json.RawMessage(strconv.Quote(defaultPipeline))
			runlog.From(ctx).Info().Str("pipeline", defaultPipeline).Msg("Using first declared pipeline as index.default_pipeline")
		}
	}

	// ADR: docs/decisions/0001-replica-count-from-node-count.md — override whatever
	// the settings file specifies; the caller chose the value (explicit override
	// or cluster topology) and replicasSource names which rule applied.
	settings["number_of_replicas"] = json.RawMessage(strconv.Itoa(replicas))
	runlog.From(ctx).Info().Int("number_of_replicas", replicas).Str("source", replicasSource).Msg("Setting index replica count")

	normalized, err := json.Marshal(settings)
	if err != nil {
		return "", runlog.Fail(runlog.FailureEvent(ctx).Str("path", path), "Serializing settings", err)
	}

	return string(normalized), nil
}

// normalizeSettingKey centralizes this code path so package behavior stays consistent.
func normalizeSettingKey(key string) string {
	return strings.TrimPrefix(key, "index.")
}

// NormalizeIndexSection centralizes this code path so package behavior stays consistent.
func NormalizeIndexSection(ctx context.Context, path, section string, variables template.Variables) (string, error) {
	if path == "" {
		return "{}", nil
	}

	content, err := template.ReadFile(path, variables)
	if err != nil {
		return "", runlog.Fail(runlog.FailureEvent(ctx).Str("path", path), fmt.Sprintf("Reading %s file", section), err)
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(content, &raw); err != nil {
		return "", runlog.Fail(runlog.FailureEvent(ctx).Str("path", path), fmt.Sprintf("Parsing %s file", section), err)
	}

	if nested, ok := raw[section]; ok {
		return string(nested), nil
	}

	return string(content), nil
}

// ReadNamedDefinitions centralizes this code path so package behavior stays consistent.
func ReadNamedDefinitions(ctx context.Context, path, resourceType string, variables template.Variables) (Definitions, []string, error) {
	if path == "" {
		return nil, nil, nil
	}

	content, err := template.ReadFile(path, variables)
	if err != nil {
		return nil, nil, runlog.Fail(runlog.FailureEvent(ctx).Str("path", path), fmt.Sprintf("Reading %s definitions file", resourceType), err)
	}

	decoder := json.NewDecoder(strings.NewReader(string(content)))
	token, err := decoder.Token()
	if err != nil {
		return nil, nil, runlog.Fail(runlog.FailureEvent(ctx).Str("path", path), fmt.Sprintf("Parsing %s definitions file", resourceType), err)
	}

	delim, ok := token.(json.Delim)
	if !ok || delim != '{' {
		return nil, nil, runlog.Fail(runlog.FailureEvent(ctx).Str("path", path), fmt.Sprintf("%s definitions file must contain a JSON object", resourceType), nil)
	}

	definitions := make(Definitions)
	names := make([]string, 0)
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return nil, nil, runlog.Fail(runlog.FailureEvent(ctx).Str("path", path), fmt.Sprintf("Reading %s definition name", resourceType), err)
		}

		name, ok := keyToken.(string)
		if !ok {
			return nil, nil, runlog.Fail(runlog.FailureEvent(ctx).Str("path", path), fmt.Sprintf("Invalid %s definition name", resourceType), nil)
		}

		var definition json.RawMessage
		if err := decoder.Decode(&definition); err != nil {
			return nil, nil, runlog.Fail(runlog.FailureEvent(ctx).Str("path", path), fmt.Sprintf("Parsing %s definition body", resourceType), err)
		}

		definitions[name] = definition
		names = append(names, name)
	}

	if _, err := decoder.Token(); err != nil {
		return nil, nil, runlog.Fail(runlog.FailureEvent(ctx).Str("path", path), fmt.Sprintf("Parsing %s definitions file", resourceType), err)
	}

	return definitions, names, nil
}
