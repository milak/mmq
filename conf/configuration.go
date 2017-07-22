package conf

import (
	"os"
	"encoding/json"
)
const PARAMETER_PORT = "port"
const DEFAULT_SYNC_PORT = "1789"
const APP_VERSION = "0.1" // The version of the current application
type Configuration struct {
	Version 	string
	Groups		[]string
	Topics 		[]*Topic 		`json:"Topics,omitempty"`
	Instances 	[]*Instance 	`json:"Instances,omitempty"`
	fileName 	string
	Services 	[]*Service
}
func NewConfiguration(aFileName string) *Configuration {
	return &Configuration{
		Version 		: APP_VERSION, 
		fileName 		: aFileName,
		Instances 		: make([]*Instance,0), 
		Services 		: make([]*Service,0), 
		Topics 			: make([]*Topic,0)}
}

func (this *Configuration) AddInstance(aInstance *Instance) bool{
	if this.GetInstance(aInstance.Name()) != nil {
		return false
	}
	this.Instances = append(this.Instances,aInstance)
	this.save()
	return true
}
func (this *Configuration) GetInstance(aName string) *Instance{
	for _,instance := range this.Instances {
		if instance.Name() == aName {
			return instance
		}
	}
	return nil
}
/**
 * Remove a instance by name
 */
func (this *Configuration) RemoveInstance(aInstanceName string) *Instance {
	found := false
	var instance *Instance
	for i := range this.Instances {
		instance = this.Instances[i]
		if (instance.Name() == aInstanceName){
			this.Instances = append(this.Instances[0:i],this.Instances[i+1:]...)
			found = true
			break;
		}
	}
	if found {
		this.save()
	}
	return instance
}
/**
 * Add a topic in the list
 */
func (this *Configuration) AddTopic(aTopic *Topic) bool {
	if this.GetTopic(aTopic.Name) != nil {
		return false
	}
	this.Topics = append(this.Topics,aTopic)
	this.save()
	return true
}
/**
 * Get a topic by name
 */
func (this *Configuration) GetTopic(aName string) *Topic {
	for _,topic := range this.Topics {
		if topic.Name == aName {
			return topic
		}
	}
	return nil
}
/**
 * Remove a topic by name
 */
func (this *Configuration) RemoveTopic(aTopicName string) bool {
	found := false
	for i,topic := range this.Topics {
		if (topic.Name == aTopicName){
			this.Topics = append(this.Topics[0:i],this.Topics[i+1:]...)
			found = true
			break;
		}
	}
	if found {
		this.save()
	}
	return found
}
func (this *Configuration) AddService(aService *Service) {
	if aService == nil {
		panic("Service cannot be nil")
	}
	this.Services = append(this.Services,aService)
}
func (this *Configuration) GetServiceByName(aServiceName string) *Service {
	for _,service := range this.Services{
		if service.Name == aServiceName {
			return service
		}
	}
	return nil
}
/**
 * Swap the configuration in file
 */
func (this *Configuration) save(){
	file,err := os.Create(this.fileName)
	if err != nil {
		panic ("Unable to write file " + err.Error())
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "\t")
	encoder.Encode(this)
}
/**
 * Constructor of Configuration if the file exists, the configuration is loaded, if the file doesn't exist, the file is created with default configuration
 */
func InitConfiguration(aFileName string) (*Configuration,bool) {
	created := false
	result := NewConfiguration(aFileName)
	if _, err := os.Stat(aFileName); os.IsNotExist(err) {
		created = true
		result.Groups = []string{"all"}
		// Add ADMIN SERVICE
		service := NewService(SERVICE_ADMIN,false,[]Parameter{*NewParameter("root","web")})
		service.Comment = "This service opens web administration. It requires REST service. Parameter : 'root' directory containing admin web files. Can be replaced by apache httpd."
		result.AddService(service)
		// Add REST_SERVICE
		service = NewService(SERVICE_REST,true,[]Parameter{*NewParameter(PARAMETER_PORT,"80")})
		service.Comment = "This service opens REST API. Parameter : 'port' the listening port."
		result.AddService(service)
		// Add SYNC_SERVICE
		service = NewService(SERVICE_SYNC,false,[]Parameter{*NewParameter(PARAMETER_PORT,"1789")})
		service.Comment = "This service opens SYNC port for clusterisation. Parameter : 'port' the listening port."
		result.AddService(service)
		/*result.Services[3].Name = "PROTOBUF"
		result.Services[3].Comment = "TODO service"
		result.Services[3].Active = false*/
		/*result.Services[3].Name = "AMQP"
		result.Services[3].Comment = "TODO service"
		result.Services[3].Active = false*/
		result.save()
	} else {
		file,_ := os.Open(aFileName)
		defer file.Close()
		decoder := json.NewDecoder(file)
		decoder.Decode(&result)
		if result.Groups == nil {
			result.Groups = []string{"all"}
		}
		if result.Topics == nil {
			result.Topics = make([]*Topic,0)
		}
		if result.Instances == nil {
			result.Instances = make([]*Instance,0)
		}
		// Initialiser les instances à "non connectées"
		for _,instance := range result.Instances {
			instance.Connected = false
		}
		if result.Services == nil {
			result.Services = make ([]*Service,0)
		}
	}
	return result, created
}