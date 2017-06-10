package conf

import (

)

// Event
type TopicAdded struct {
	Topic *Topic
}
type InstanceRemoved struct {
	Instance *Instance
}