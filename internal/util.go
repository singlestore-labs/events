package internal

import "reflect"

func IsNil[T any](val T) bool {
	v := reflect.ValueOf(val)
	kind := v.Kind()

	// Check if the type is one that can be nil
	if kind == reflect.Ptr || kind == reflect.Interface || kind == reflect.Slice ||
		kind == reflect.Map || kind == reflect.Chan || kind == reflect.Func {
		return v.IsNil()
	}
	return false // Non-nillable types are never nil
}
