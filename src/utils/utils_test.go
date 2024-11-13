package utils

import (
	"fmt"
	"testing"
)

// TO DO: test laster

func TestMap(t *testing.T) {
	input := []int{1,2,3}
	addOne := func(num int, _index int) int {
		return num + 1
	}
	output := Map(input, addOne)
	fmt.Println(output)
}

func TestFilter(t *testing.T) {
	inputs := []int{1,7,95,6,3}
	moreThanFive := func (num int, _index int) bool  {
		return num > 5	
	}
	output := Filter(inputs, moreThanFive)
	fmt.Println(output)
}

func TestFind(t *testing.T) {
	inputs := []string{"cat", "bat", "ya"}
	findCat := func (item string, _index int) bool {
		return item == "cat"
	}

	output := Find(inputs, findCat)
	*output = "isis"
	fmt.Println(inputs)

}
