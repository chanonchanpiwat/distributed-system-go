package mr

import (
	"strconv"
	"strings"
	"testing"
	"unicode"
)


func Map(filename string, contents string) []KeyValue {
	ff := func(r rune) bool { return !unicode.IsLetter(r) }
	words := strings.FieldsFunc(contents, ff)

	kva := []KeyValue{}
	for _, w := range words {
		kv := KeyValue{w, "1"}
		kva = append(kva, kv)
	}

	return kva
}

func Reduce(key string, values []string) string {
	return strconv.Itoa(len(values))
}

func TestDoMap(t *testing.T) {
	doMap(0, "../main/pg-grimm.txt", 10, Map)
}

func TestDoReduce(t *testing.T) {
	doReduce(1, 1, Reduce)
}