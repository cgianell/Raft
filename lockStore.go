package raft_lock

type MultiMap[K comparable, V any] map[K][]V

func exists[A any](array []A, predicate func(A) bool) bool {
	for _, elem := range array {
		if predicate(elem) {
			return true
		}
	}
	return false
}

func (m MultiMap[K, V]) put(key K, val V) {
	m[key] = append(m[key], val)
}

func (m MultiMap[K, V]) cleanEmpty(key K) {
	if v, exists := m[key]; exists && len(v) == 0 {
		delete(m, key)
	}
}

func filter[V any](array []V, keepWhenTrue func(V) bool) []V {
	result := make([]V, 0)
	for _, v := range array {
		if keepWhenTrue(v) {
			result = append(result, v)
		}
	}
	return result
}

func (m MultiMap[K, V]) filter(key K, keepWhenTrue func(V) bool) {
	m[key] = filter(m[key], keepWhenTrue)
	m.cleanEmpty(key) //guarantees that when there's a key, then there's at least one element in the array
}

type LockStore[K comparable, ID comparable] MultiMap[K, ID]

func (store LockStore[K, ID]) getLockOwner(key K) (ID, bool) {
	// Gets the lock owner (the Id at the first index if it exists)
	if ids, exists := store[key]; exists {
		return ids[0], true
	} else {
		var doesntExist ID
		return doesntExist, false
	}
}

func (store LockStore[K, ID]) lock(key K, id ID) {
	sameValDoesntExist := !exists(store[key], func(elem ID) bool { return id == elem })
	// this check makes it so that the same client aquiring the lock twice means only the first time counts
	if sameValDoesntExist {
		MultiMap[K, ID](store).put(key, id)
	}
}

func (store LockStore[K, ID]) unlock(key K, id ID) {
	MultiMap[K, ID](store).filter(key, func(elem ID) bool { return id != elem })
}
