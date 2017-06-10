package dist

import (
	"github.com/milak/mmq/conf"
)
/**
 * Events raised by dist package 
 */
type TopicReceived struct {
	Topic 		*conf.Topic
	From 		*conf.Instance
}
type InstanceReceived struct {
	Instance 	*conf.Instance
	From 		*conf.Instance
}
type InstanceDisconnected struct {
	Instance 	*conf.Instance
}
type ItemReceived struct {
	Item 		*SharedItem
	From		*conf.Instance
}
type ItemContentReceived struct {
	ID 			string
	Content		[]byte // TODO use a Reader instead
	From		*conf.Instance
}
type ItemRemoved struct {
	ID 			string
	From		*conf.Instance
}