package item

import (
	"github.com/milak/mmqapi/conf"
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