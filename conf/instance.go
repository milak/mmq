package conf

import (

)

type Instance struct {
	Host 		string
	Port 		string
	Connected 	bool
	Groups		[]string
}
func NewInstance(aHost string, aPort string) *Instance{
	return &Instance {Host : aHost, Port: aPort, Connected : false}
}
func (this *Instance) Name() string {
	return this.Host+":"+this.Port
}