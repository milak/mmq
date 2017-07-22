package dist

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"github.com/milak/mmqapi/conf"
	"github.com/milak/mmq/env"
	"net"
)
/**
 * An internal structure used for the map. It links an instance to a connection
 */
type instanceConnection struct {
	instance   *conf.Instance // an instance
	connection *net.Conn      // a connection opened with the instance
	pool		*InstancePool
}
func (this *instanceConnection) SendItem(aItem *SharedItem) {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.Encode(aItem)
	this.pool.protocol.sendCommand("ITEM", buffer.Bytes(), *this.connection)
}
func (this *instanceConnection) SendRemoveItem(aItemID string) {
	this.pool.protocol.sendCommand("ITEM-REMOVE", []byte(aItemID), *this.connection)
}
func (this *instanceConnection) SendItemContent(aItemID string, size int) io.Writer {
	this.pool.protocol.sendStreamedCommand("ITEM-CONTENT",len(aItemID)+1+size,*this.connection)
	(*this.connection).Write([]byte(aItemID))
	(*this.connection).Write([]byte{DIEZE})
	return *this.connection
}
func (this *instanceConnection) Close() error {
	(*this.connection).Close()
	this.instance.Connected = false
	this.pool.instanceClosed(this.instance)
	return nil
}
type InstancePool struct {
	context    			*env.Context
	logger 				*log.Logger
	port 				string 							// will be obtained via configuration
	connections			map[string]*instanceConnection	// a map that links instances to opened net connection
	instancesByGroup 	map[string][]*instanceConnection
	protocol			*protocol
}
func NewInstancePool(aContext *env.Context) *InstancePool {
	result := &InstancePool{context : aContext}
	result.protocol 		= NewProtocol(aContext,result)
	result.logger 			= aContext.Logger
	result.connections 		= make(map[string]*instanceConnection)
	result.instancesByGroup = make(map[string][]*instanceConnection)
	service := aContext.Configuration.GetServiceByName(conf.SERVICE_SYNC)
	if service != nil {
		param := service.GetParameterByName(conf.PARAMETER_PORT)
		if param != nil {
			result.port = param.Value
		}
	}
	return result
}
func (this *InstancePool) Build (aInstance *conf.Instance, aConnection *net.Conn) io.Closer {
	return this.newInstanceConnection(aInstance, aConnection)
}
/**
 * Constructor for InstanceConnection
 */
func (this *InstancePool) newInstanceConnection (aInstance *conf.Instance, aConnection *net.Conn) *instanceConnection{
	result := &instanceConnection{instance : aInstance, connection : aConnection, pool : this}
	this.context.Logger.Println("DEBUG Adding connection to ",aInstance.Name())
	this.connections[aInstance.Name()] = result
	return result
}
func (this *InstancePool) instanceClosed(aInstance *conf.Instance){
	delete(this.connections,aInstance.Name())
}
func (this *InstancePool) GetInstancesByGroup(aGroupName string) []*instanceConnection {
	return nil
}
func (this *InstancePool) GetInstanceByName(aInstanceName string) *instanceConnection {
	return this.connections[aInstanceName]
}
func (this *InstancePool) Connect(aInstance *conf.Instance) error {
	_,err := this.protocol.connect(aInstance)
	if err == nil {
		//this.newInstanceConnection(aInstance, &conn)
		return nil
	} else {
		return err
	}
}