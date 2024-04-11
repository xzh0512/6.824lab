package main

import (
	"fmt"
	"sort"
)

func main() {
	arr := []int{1, 3, 5, 2, 8}
	sort.Slice(arr, func(i, j int) bool {
		return arr[i] < arr[j]
	})
	fmt.Printf("%v", arr)
}
