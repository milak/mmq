package item

import (
	"mmq/conf"
)

// Event
type ItemAdded struct {
	Item 	*Item
	Topic	*conf.Topic
}
type ItemRemoved struct {
	Item 	*Item
	Topic	*conf.Topic
}