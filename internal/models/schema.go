package models

import (
	"fmt"
	"slices"
)

// strictSchema reports why OpenAI's strict structured outputs would turn schema away. Strict
// mode takes a subset of JSON Schema: the root is an object; every object sets
// additionalProperties to false and lists all its properties, and nothing else, as required
// (an optional field is a union with null); and some keywords are not taken at all.
func strictSchema(schema map[string]any) error {
	if t, _ := schema["type"].(string); t != "object" {
		return fmt.Errorf("#: the root must be an object, not %v", schema["type"])
	}
	return checkStrict(schema, "#")
}

// unstrict are the keywords strict mode does not take; minLength and maxLength it does not
// list as taken either.
var unstrict = []string{"allOf", "not", "if", "then", "else", "dependentRequired", "dependentSchemas",
	"patternProperties", "unevaluatedProperties", "propertyNames", "minLength", "maxLength"}

// isObject reports whether a schema's type is object, alone or in a list ("type": ["object", "null"]).
func isObject(t any) bool {
	switch t := t.(type) {
	case string:
		return t == "object"
	case []any:
		return slices.Contains(t, any("object"))
	}
	return false
}

// checkStrict walks one schema: its keywords, then the schemas it holds, never the values of
// keywords such as enum or default.
func checkStrict(s map[string]any, at string) error {
	for _, k := range unstrict {
		if _, ok := s[k]; ok {
			return fmt.Errorf("%s: strict mode does not take %s", at, k)
		}
	}
	if props, ok := s["properties"].(map[string]any); ok || isObject(s["type"]) {
		if s["additionalProperties"] != false {
			return fmt.Errorf("%s: additionalProperties must be false", at)
		}
		var required []string
		list, _ := s["required"].([]any)
		for _, r := range list {
			name, _ := r.(string)
			if _, ok := props[name]; !ok {
				return fmt.Errorf("%s: required %v is not a property", at, r)
			}
			required = append(required, name)
		}
		for name, sub := range props {
			if !slices.Contains(required, name) {
				return fmt.Errorf("%s: property %q must be required (make it a union with null instead)", at, name)
			}
			if err := checkSub(sub, at+"/properties/"+name); err != nil {
				return err
			}
		}
	}
	if err := checkSub(s["items"], at+"/items"); err != nil {
		return err
	}
	for _, k := range []string{"anyOf", "oneOf", "prefixItems"} {
		list, _ := s[k].([]any)
		for i, sub := range list {
			if err := checkSub(sub, fmt.Sprintf("%s/%s/%d", at, k, i)); err != nil {
				return err
			}
		}
	}
	for _, k := range []string{"$defs", "definitions"} {
		defs, _ := s[k].(map[string]any)
		for name, sub := range defs {
			if err := checkSub(sub, at+"/"+k+"/"+name); err != nil {
				return err
			}
		}
	}
	return nil
}

// checkSub checks a value that holds a schema, when there is one. Strict mode takes no
// schema that is just true or false.
func checkSub(v any, at string) error {
	switch v := v.(type) {
	case map[string]any:
		return checkStrict(v, at)
	case bool:
		return fmt.Errorf("%s: strict mode takes no boolean schema", at)
	}
	return nil
}
