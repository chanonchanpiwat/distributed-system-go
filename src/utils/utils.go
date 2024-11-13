package utils

func Map[T any, R any](collection []T, iteratee func(item T, index int) R) []R {
	result := make([]R, len(collection))

	for i := range collection {
		result[i] = iteratee(collection[i], i)
	}

	return result
}

func Filter[T any, Slice ~[]T](collection Slice, predicate func(item T, index int) bool) Slice {
	result := make(Slice, 0, len(collection))

	for i := range collection {
		if predicate(collection[i], i) {
			result = append(result, collection[i])
		}
	}

	return result
}

func Find[T any](collection []T, iteratee func(item T, index int) bool) *T {

	for i, item := range collection {
		if iteratee(item, i) {
			return &collection[i]
		}
	}

	return nil
}
