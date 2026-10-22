// Template expansion for config assets (settings, mappings, pipelines,
// policies, transforms). Consumed by the readers in loader.go and by Run's
// preflight.

package loader

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
)

// templateVariablePattern recognizes only the braced ${NAME} form. A brace-less
// $NAME must stay verbatim: matching it let any literal "$word" in a config
// asset (e.g. $HOME) be silently rewritten from the environment, corrupting
// cluster configuration. Do not re-add it without a default-off opt-in.
var templateVariablePattern = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// unresolvedTemplateError reports ${NAME} placeholders that neither the
// variable map nor the environment could resolve. It is typed so the preflight
// and tests can tell it apart from an I/O failure.
type unresolvedTemplateError struct {
	Path  string
	Names []string
}

// Error lists every unresolved name so a config tree with several typos is
// diagnosed in one run.
func (e *unresolvedTemplateError) Error() string {
	placeholders := make([]string, len(e.Names))
	for i, name := range e.Names {
		placeholders[i] = "${" + name + "}"
	}
	return fmt.Sprintf("unresolved template variables in %s: %s", e.Path, strings.Join(placeholders, ", "))
}

// expandTemplate substitutes ${NAME} from variables, then the environment
// (documented fallback). Names resolvable by neither are returned, sorted and
// de-duplicated, and left verbatim in the output. Expansion is one pass:
// replacement values are data and are never rescanned for placeholders.
func expandTemplate(content string, variables templateVariables) (string, []string) {
	missing := map[string]struct{}{}
	expanded := templateVariablePattern.ReplaceAllStringFunc(content, func(match string) string {
		name := templateVariablePattern.FindStringSubmatch(match)[1]

		if value, ok := variables[name]; ok {
			return value
		}
		if value, ok := os.LookupEnv(name); ok {
			return value
		}
		missing[name] = struct{}{}
		return match
	})

	if len(missing) == 0 {
		return expanded, nil
	}
	unresolved := make([]string, 0, len(missing))
	for name := range missing {
		unresolved = append(unresolved, name)
	}
	sort.Strings(unresolved)
	return expanded, unresolved
}

// readTemplatedFile reads path and expands its placeholders. An unresolved
// placeholder is an error rather than a literal "${NAME}" sent to
// Elasticsearch, where it would be accepted as valid configuration.
func readTemplatedFile(path string, variables templateVariables) ([]byte, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	expanded, unresolved := expandTemplate(string(content), variables)
	if len(unresolved) > 0 {
		return nil, &unresolvedTemplateError{Path: path, Names: unresolved}
	}
	return []byte(expanded), nil
}

// preflightTemplateFiles expands every template-bearing input before any
// cluster call so an unresolved ${VAR} fails the run with nothing changed.
// Empty paths are skipped. Read errors are not this function's concern: the
// existing read sites report them with their existing classification, and
// they must not hide unresolved names in the remaining paths.
func preflightTemplateFiles(variables templateVariables, paths ...string) error {
	var problems []string
	for _, path := range paths {
		if path == "" {
			continue
		}
		_, err := readTemplatedFile(path, variables)
		var unresolved *unresolvedTemplateError
		if errors.As(err, &unresolved) {
			problems = append(problems, unresolved.Error())
		}
	}
	if len(problems) == 0 {
		return nil
	}
	return &RunError{
		Kind: ErrInvalidOptions,
		Op:   "validating template variables",
		Err:  fmt.Errorf("%s", strings.Join(problems, "; ")),
	}
}
