package dist

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/milak/tools/event"
	"github.com/milak/tools/osgi"
	"io"
	"log"
	"github.com/milak/mmqapi/conf"
	"net"
	"strconv"
	"time"
)
// Structure used for exchange of instance information during HELLO part
type currentInstanceInformation struct {
	Host	string
	Port	string
	Version string
	Groups	[]string
}
type protocol struct {
	context				osgi.BundleContext
	logger				*log.Logger
	port 				string
	connectionFactory 	connectionFactory
}
// A const for DIEZE value
const DIEZE byte = byte('#')
func NewProtocol(aContext osgi.BundleContext, aConnectionFactory connectionFactory) *protocol {
	result := &protocol{context : aContext, logger : aContext.GetLogger(), connectionFactory : aConnectionFactory}
	configuration := aContext.GetProperty("configuration").(*conf.Configuration)
	for _,service := range configuration.Services {
		if service.Name == conf.SERVICE_SYNC {
			for _,parameter := range service.Parameters {
				if parameter.Name == conf.PARAMETER_PORT {
					result.port = parameter.Value
				}
			}
		}
	}
	return result
}
/**
 * Internal method that splits a command received from remote 
 */
func (this *protocol) _splitCommand(line []byte) (command string, arguments []byte, remain []byte, needMore int) {
	// Synchronize with first DIEZE
	if line[0] != DIEZE {
		return "",[]byte{},line,0
	}
	// Obtain the COMMAND
	i := 1
	command = ""
	for line[i] != DIEZE {
		command += string(line[i])
		i++
	}
	// Obtain the LENGTH
	i++
	slength := ""
	for line[i] != DIEZE {
		slength += string(line[i])
		i++
	}
	i++
	length,_ := strconv.Atoi(slength)
	// Check wether the whole data has been received
	remain = []byte{}
	needMore = 0
	tailleRestantALire := len(line) - i
	// Missing the end the data
	if length > tailleRestantALire { // la longueur de la donnée annoncée > tailleRestantALire
		arguments = line[i:]
		needMore = length - tailleRestantALire // la longueur de la donnée annoncée - la tailleRestantALire + un dieze
	} else { // All the data has been obtained
		arguments = line[i:i+length]
		// check wether a part of a next command is found in the data received
		if (i+length) != len(line) {
			remain = line[i+length:]
		}
	}
	return command, arguments, remain, needMore
}

/**
 * Send a command to remote Instance :
 *   #<command>#<size of arguments>#<arguments>
 * @param command    : a command cannot be nul
 * @param arguments  : a data to send cannot be nul, if empty, send []byte{}
 * @param connection : an open connection
 */
func (this *protocol) sendCommand(command string, arguments []byte, connection net.Conn){
	this.logger.Println("DEBUG sending",command,string(arguments))
	connection.Write([]byte("#"+command+"#"+strconv.Itoa(len(arguments))+"#"))
	connection.Write(arguments)
}
func (this *protocol) sendStreamedCommand(command string, size int, connection net.Conn){
	this.logger.Println("DEBUG sending",command,size)
	connection.Write([]byte("#"+command+"#"+strconv.Itoa(size)+"#"))
}
func (this *protocol) connect(aInstance *conf.Instance) (net.Conn,error){
	host := aInstance.Host + ":" + aInstance.Port
	//this.logger.Println("Trying to connect to " + host)
	conn, err := net.Dial("tcp", host)
	if err != nil {
		return nil,err
	} else {
		//this.logger.Println("INFO Connection successful")
		this.sendCommand("HELLO", this._prepareInfo(), conn)
		aInstance.Connected = true
		connection := this.connectionFactory.Build(aInstance,&conn)
		go this._keepConnected(aInstance, &conn, connection)
		return conn,nil
	}
}
/**
 * Method used by both side : caller and called
 * all the commands will be received through this link
 */
func (this *protocol) _keepConnected(aInstance *conf.Instance, aConnection *net.Conn, aCloser io.Closer){
	var byteBuffer *bytes.Buffer
	buffer := make([]byte,2000)
	connection := (*aConnection)
	defer func() {
		connection.Close()
	}()
	var command string
	var arguments, remain []byte
	var needMore int
	// while the service is running
	for this.context.GetBundle().GetState() == osgi.ACTIVE {
		time.Sleep(500 * time.Millisecond)
		//this.logger.Println("DEBUG after sleep")
		// reintroduce remain from previous command
		if len(remain) > 0 {
			//this.logger.Println("Reusing remain",remain)
			buffer = remain
		} else {
			//this.logger.Println("Listening to remote")
			count,err := connection.Read(buffer)
			if err != nil {
				if aCloser != nil {
					aCloser.Close()
				}
				this.logger.Println("INFO Lost connection with",aInstance.Name(),err)
				event.Bus.FireEvent(&InstanceDisconnected{Instance : aInstance})
				break
			}
			buffer = buffer[0:count]
		}
		//this.logger.Println("DEBUG after Read",buffer,string(buffer))
		// Parse data received
		command, arguments, remain, needMore = this._splitCommand(buffer)
		//this.logger.Println("DEBUG after split",command,string(arguments),remain,needMore)
		if needMore != 0 {
			//this.logger.Println("Unfinished",arguments,", need",needMore,"bytes")
			var bufferNeeded bytes.Buffer // Todo in the future use a swap in disk for ITEM command
			remain = this._takeMore(connection,needMore,&bufferNeeded)
			arguments = append(arguments,bufferNeeded.Bytes()...)
			//this.logger.Println("Finally got",string(arguments))
		}
		//this.logger.Println("DEBUG after Parse",command,string(arguments))
		// Process the command
		//this.logger.Println("DEBUG Received command " + command,remain)
		// Received HELLO from called
		if command == "HELLO" { // On est côté appelant, on reçoit la réponse de l'appelé, on lui envoie la configuration
			buffer := bytes.NewBuffer(arguments)
			decoder := json.NewDecoder(buffer)
			var info currentInstanceInformation
			decoder.Decode(&info)
			this.logger.Println("DEBUG HELLO received",info)
			aInstance.Groups = info.Groups
			this._sendConfiguration(&connection)
		} else if command == "INSTANCES" { // Receive instance list
			var newInstances []*conf.Instance
			byteBuffer = bytes.NewBuffer(arguments)
			decoder := json.NewDecoder(byteBuffer)
			decoder.Decode(&newInstances)
			for _,newInstance := range newInstances {
				//this.logger.Println("DEBUG Received instance :",newInstance)
				newInstance.Connected = false // ensure the Instance will not be considered as connected
				event.Bus.FireEvent(&InstanceReceived{Instance : newInstance, From : aInstance})
			}
		} else if command == "TOPICS" { // Receive topic list
			var distributedTopics []*conf.Topic
			byteBuffer = bytes.NewBuffer(arguments)
			decoder := json.NewDecoder(byteBuffer)
			decoder.Decode(&distributedTopics)
			for _,topic := range distributedTopics {
				this.logger.Println("DEBUG Received topic :",topic)
				event.Bus.FireEvent(&TopicReceived{Topic : topic, From : aInstance})
			}
		} else if command == "ITEM" { // Receive item
			var item *SharedItem
			byteBuffer = bytes.NewBuffer(arguments)
			decoder := json.NewDecoder(byteBuffer)
			decoder.Decode(&item)
			//this.logger.Println("DEBUG Received item :",item)
			event.Bus.FireEvent(&ItemReceived{Item : item, From : aInstance})
		} else if command == "ITEM-CONTENT" { // Receive item
			this.logger.Println("DEBUG Received item content :",string(arguments))
			var i = 0
			for arguments[i] != DIEZE {
				i++
			}
			id := string(arguments[0:i])
			content := arguments[i+1:]
			this.logger.Println("DEBUG Received item content : id = ",id," content = ",string(content))
			event.Bus.FireEvent(&ItemContentReceived{ID : id, Content : content, From : aInstance})
		} else if command == "ITEM-REMOVE" { // Remove item
			event.Bus.FireEvent(&ItemRemoved{ID : string(arguments), From : aInstance})
		} else if command == "ERROR" {
			this.logger.Println("WARNING Received ERROR :",arguments)
		} else {
			this.logger.Println("WARNING Not supported command")
			this.sendCommand("ERROR",[]byte("NOT SUPPORTED COMMAND '"+command+"'"),connection)
		}
	}
}
/**
 * Send configuration to other side :
 *   * the known instances
 *   * the distributed topics
 */
func (this *protocol) _sendConfiguration(aConnection *net.Conn){
	//this.logger.Println("Sending configuration")
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	configuration := this.context.GetProperty("configuration").(*conf.Configuration)
	encoder.Encode(configuration.Instances)
	this.sendCommand("INSTANCES",buffer.Bytes(),*aConnection)
	buffer.Reset()
	var distibutedTopics []*conf.Topic
	for _,topic := range configuration.Topics {
		if topic.IsDistributed() {
			distibutedTopics = append(distibutedTopics,topic)
		}
	}
	if len (distibutedTopics) > 0 {
		encoder.Encode(distibutedTopics)
		//this.logger.Println("Sending topics ", string(buffer.Bytes()))
		this.sendCommand("TOPICS",buffer.Bytes(),*aConnection)
	}
}
/**
 * Process the connection when called by a remote node.
 */
func (this *protocol) handleConnection (aConn net.Conn) (*conf.Instance, error) {
	host,_,_ := net.SplitHostPort(aConn.LocalAddr().String())
	this.context.SetProperty("host",host)
	this.logger.Println("DEBUG Processing call")
	buffer := make([]byte,1000)
	count,err := aConn.Read(buffer)
	if err != nil {
		//this.logger.Println("Unable to read HELLO from remote",err)
		return nil,err
	}
	if count < 10 {
		//this.logger.Println("Unable to read HELLO from remote ",buffer[0:count])
		this.sendCommand("ERROR",[]byte("Unable to understand"),aConn)
		return nil,errors.New("Unable to understand")
	}
	command, arguments, remain, needMore := this._splitCommand(buffer[0:count])
	if needMore > 0 {
		//this.logger.Println("Unfinished",arguments,", need",needMore,"bytes")
		var bufferNeeded bytes.Buffer
		remain = this._takeMore(aConn,needMore,&bufferNeeded)
		arguments = append(arguments,bufferNeeded.Bytes()...)
		//this.logger.Println("Finally got",string(arguments))
	}
	if command == "" {
		//this.logger.Println("Unable to read HELLO from remote")
		this.sendCommand("ERROR",[]byte("Unable to understand"),aConn)
		return nil,errors.New("Unable to understand")
	}
	//this.logger.Println("Received ",command,"-",arguments,"-",remain)
	if command != "HELLO" {
		//this.logger.Println("Unable to read HELLO from remote ")
		this.sendCommand("ERROR",[]byte("Unable to understand"),aConn)
		return nil,errors.New("Unable to understand " + command)
	}
	infoBuffer := bytes.NewBuffer(arguments)
	var info currentInstanceInformation
	decoder := json.NewDecoder(infoBuffer)
	decoder.Decode(&info)
	instance := conf.NewInstance(info.Host,info.Port)
	instance.Groups = info.Groups
	instance.Connected = true
	//this.logger.Println("Adding caller as new instance",instance)
	event.Bus.FireEvent(&InstanceReceived{Instance : instance, From : nil})
	for len(remain) > 0 {
		command, arguments, remain, needMore = this._splitCommand(remain)
		this.logger.Println("DEBUG Received command " + command,arguments,remain,needMore)
	}
	this.sendCommand("HELLO",this._prepareInfo(),aConn) // TODO échanger leur numéros de version
	this._sendConfiguration(&aConn)
	connection := this.connectionFactory.Build(instance,&aConn)
	go this._keepConnected(instance,&aConn,connection)
	return instance, nil
	// TODO : gerer le fait que les deux peuvent essayer de se connecter en même temps, il y aura alors deux connections entre eux
}
func (this *protocol) _prepareInfo() []byte {
	configuration := this.context.GetProperty("configuration").(*conf.Configuration)
	info := currentInstanceInformation{Host : this.context.GetProperty("host"), Port : this.port, Version : configuration.Version, Groups : configuration.Groups}
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.Encode(info)
	return buffer.Bytes()
}
/**
 * Reads from connection until missing data is received (linked to NeedMore detection in splitCommand)
 */
func (this *protocol) _takeMore(connection net.Conn, needMore int, writer io.Writer) (remain []byte){
	//this.logger.Println("DEBUG taking more",needMore)
	buffer := make([]byte,2000)
	var total = 0
	for total < needMore {
		count,err := connection.Read(buffer)
		//this.logger.Println("DEBUG took more",count," : ",total)
		if err != nil {
			this.logger.Println("WARNING Failed to read following needed bytes")
			return
		}
		if count + total > needMore {
			part := needMore - total
			total += part 
			writer.Write(buffer[0:part])
			remain = buffer[part:count]
		} else {
			total += count
			writer.Write(buffer[0:count])
			remain = []byte{}
		}
	}
	//this.logger.Println("DEBUG got all",total)
	return remain
}