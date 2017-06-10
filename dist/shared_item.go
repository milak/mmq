package dist

import (
	"github.com/milak/mmq/item"
)

type SharedItem struct {
	Item 		*item.Item
	Topic		string
	Instances	[]string
	Policy		string
	Groups		string
}
func (this *SharedItem) AddInstance(aInstance string) {
	this.Instances = append(this.Instances,aInstance)
}
func (this *SharedItem) RemoveInstance(aInstance string) {
	newInstances := make([]string,0)
	for _,instance := range this.Instances {
		if aInstance != instance {
			newInstances = append(newInstances,instance)
		}
	}
	this.Instances = newInstances
}