package raft

import (
	"math/rand"
)

// randomInt64 returns a non-negative pseudo-random int64 in [0, n).
// It is safe for concurrent use by multiple goroutines.
//
// Go 1.20+ made the package-level math/rand functions safe for concurrent use
// (they use a global automatically-seeded source), so we use them here
// instead of creating a shared *rand.Rand instance that would require locking.
func randomInt64(n int64) int64 {
	if n <= 0 {
		return 0
	}
	return rand.Int63n(n)
}
