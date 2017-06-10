package item

import ()

type ItemList struct {
	head *node
	tail *node
	size int
}
type node struct {
	prev *node
	next *node
	item *Item
}

func NewItemList() *ItemList {
	return &ItemList{head: nil, tail: nil, size: 0}
}
func (this *ItemList) ToArray() []*Item {
	result := make([]*Item, this.size)
	current := this.head
	for current != nil {
		result = append(result, current.item)
		current = current.next
	}
	return result
}
func (this *ItemList) Remove(aNode *node) {
	if aNode == nil {
		panic("Node is nil")
	}
	if this.size == 0 {
		panic("ItemList is empty")
	} else if this.size == 1 {
		if this.tail == aNode { // equivalent to this.head == aNode
			this.tail = nil
			this.head = nil
		} else {
			panic("This item is not part of this itemlist")
		}
	} else {
		// We remove in tail
		if this.tail == aNode {
			this.tail = aNode.prev
			this.tail.next = nil
			// We remove in head
		} else if this.head == aNode {
			this.head = this.head.next
			this.head.prev = nil
		} else {
			prev := aNode.prev
			next := aNode.next
			prev.next = next
			next.prev = prev
		}
	}
	this.size--
}
func (this *ItemList) Iterator() *Iterator {
	return &Iterator{current: this.head}
}
func (this *ItemList) Size() int {
	return this.size
}
func (this *ItemList) IsEmpty() bool {
	return this.size == 0
}
func (this *ItemList) AddInTail(aItem *Item) {
	newNode := &node{item: aItem}
	if this.size == 0 {
		this.head = newNode
		this.tail = newNode
	} else {
		newNode.prev = this.tail
		this.tail.next = newNode
		this.tail = newNode
	}
	this.size++
}
func (this *ItemList) PopHead() *Item {
	var result *Item
	if this.size == 0 {
		result = nil
	} else if this.size == 1 {
		result = this.head.item
		this.head = nil
		this.tail = nil
		this.size = 0
	} else {
		result = this.head.item
		head := this.head
		this.head = head.next
		this.head.prev = nil
		this.size--
	}
	return result
}
