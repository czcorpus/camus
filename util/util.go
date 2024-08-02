package util

import (
	"errors"
	"math/big"
)

var (
	ErrPrimeSeachExhausted = errors.New("prime search exhausted")
)

func NearestPrime(v int) (int, error) {
	for i := v; i < v+1000; i++ {
		bi := big.NewInt(int64(i))
		if bi.ProbablyPrime(20) {
			return i, nil
		}
	}
	return -1, ErrPrimeSeachExhausted
}
