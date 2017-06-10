package item

import (
	"github.com/milak/mmq/conf"
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