package item

import (
	"github.com/milak/list"
)

type Iterator struct {
	current *node
}

func (this *Iterator) HasNext() bool {
	return this.current != nil
}
func (this *Iterator) NextWithNode() (interface{}, *node) {
	if this.current == nil {
		return nil, nil
	}
	result := this.current.item
	node := this.current
	this.current = this.current.next
	return result, node
}
func (this *Iterator) Next() interface{} {
	item,_ := this.NextWithNode()
	return item
}

type emptyIterator struct {
	
}
func NewEmptyIterator() list.Iterator {
	return &emptyIterator{}
}
func (this *emptyIterator) HasNext() bool {
	return false
}
func (this *emptyIterator) Next() interface{} {
	panic("Iterator is empty")
	return nil
}

type roundRobinIterator struct {
	next 		int
	iterators 	[]list.Iterator
}
func NewRoundRobinIterator(aIterators []list.Iterator) list.Iterator {
	if aIterators == nil {
		panic("Iterator list cannot be nil")
	}
	// let's clear empty iterators
	newIterators := []list.Iterator{}
	for _,i := range aIterators {
		if i.HasNext() {
			newIterators = append(newIterators,i)
		}
	}
	return &roundRobinIterator{iterators : newIterators, next : 0}
}
func (this *roundRobinIterator) HasNext() bool {
	return len(this.iterators) > 0
}
func (this *roundRobinIterator) Next() interface{} {
	if len(this.iterators) == 0 {
		panic("Iterator is empty")
	}
	result := this.iterators[this.next].Next()
	if !this.iterators[this.next].HasNext() {
		this.iterators = append(this.iterators[0:this.next],this.iterators[this.next+1:]...)
	} else {
		this.next++
	}
	if this.next >= len(this.iterators) {
		this.next = 0
	}
	return result
}

type orderedIterator struct {
	iterators 	[]list.Iterator
}
func NewOrderedIterator(aIterators []list.Iterator) list.Iterator {
	if aIterators == nil {
		panic("Iterator list cannot be nil")
	}
	// let's clear empty iterators
	newIterators := []list.Iterator{}
	for _,i := range aIterators {
		if i.HasNext() {
			newIterators = append(newIterators,i)
		}
	}
	return &orderedIterator{iterators : aIterators}
}
func (this *orderedIterator) HasNext() bool {
	return len(this.iterators) > 0
}
func (this *orderedIterator) Next() interface{} {
	if len(this.iterators) == 0 {
		panic("Iterator is empty")
	}
	result := this.iterators[0].Next()
	if !this.iterators[0].HasNext() {
		this.iterators = this.iterators[1:]
	}
	return result
}
