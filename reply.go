package bullmq

import "strconv"

// Reply helpers: convert the loosely typed Lua replies.

// replyInt converts an integer-like reply.
func replyInt(v any) (int64, bool) {
	switch x := v.(type) {
	case int64:
		return x, true
	case string:
		n, err := strconv.ParseInt(x, 10, 64)
		return n, err == nil
	case float64:
		return int64(x), true
	case bool:
		if x {
			return 1, true
		}

		return 0, true
	}

	return 0, false
}

// replyString converts a string-like reply.
func replyString(v any) (string, bool) {
	switch x := v.(type) {
	case string:
		return x, true
	case int64:
		return strconv.FormatInt(x, 10), true
	case []byte:
		return string(x), true
	}

	return "", false
}

// replyArray converts an array reply.
func replyArray(v any) ([]any, bool) {
	a, ok := v.([]any)
	return a, ok
}

// replyHash converts a flat [field, value, ...] array into a map.
func replyHash(v any) map[string]string {
	arr, _ := replyArray(v)

	m := make(map[string]string, len(arr)/2)
	for i := 0; i+1 < len(arr); i += 2 {
		k, ok := replyString(arr[i])
		if !ok {
			continue
		}

		val, _ := replyString(arr[i+1])
		m[k] = val
	}

	return m
}

// scriptCodeError converts a negative integer reply into a [ScriptError].
func scriptCodeError(v any) error {
	if n, ok := v.(int64); ok && n < 0 {
		return NewScriptError(n)
	}

	return nil
}
