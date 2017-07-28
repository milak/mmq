package service

import (
	"log"
	"github.com/milak/mmqapi/conf"
	"github.com/milak/mmq/dist"
	"github.com/milak/tools/event"
	"github.com/milak/tools/osgi"
	"github.com/milak/tools/osgi/service"
	"github.com/milak/tools/logutil"
//	"reflect"
	"time"
)
/**
 * The source part is about synchronisation between instances
 */


/**
 * The main class of the source code
 */
type SyncService struct {
	running 	bool							// boolean indicating if the service is running, setting it to false, should stop listening 
	context 	osgi.BundleContext				// a reference to the context, usefull to get accès to logger and configuration
	logger		*log.Logger						// the logger obtained from context, it is copied here for code readability reason
	pool		*dist.InstancePool
	port		string
}
/**
 * Constructor for the SyncService class
 */
func NewSyncService (aInstancePool *dist.InstancePool) *SyncService {
	result := &SyncService{running : true, pool : aInstancePool}
	event.Bus.AddListener(result)
	return result
}
func (this *SyncService) Start (aBundleContext osgi.BundleContext){
	this.context = aBundleContext
	logServiceRef := this.context.GetService("LogService")
	if logServiceRef == nil {
		this.logger = logutil.DefaultLogger
	} else {
		logService = logServiceRef.Get().(LogService)
		this.logger = logService.GetLogger()
	}
	configuration := this.context.GetProperty("configuration").(*conf.Configuration)
	for s := range configuration.Services {
		service := configuration.Services[s]
		if !service.Active {
			continue
		}
		if service.Name == "SYNC" {
			for _,p := range service.Parameters {
				if p.Name == "port" {
					this.port = p.Value
				}
			}
			this.logger.Println("INFO starting...")
			go this.scanInstances()
			break
		}
	}
}
// Catch events
func (this *SyncService) Event(aEvent interface{}) {
	configuration := this.context.GetProperty("configuration").(*conf.Configuration)
	switch e:= aEvent.(type) {
		case *conf.InstanceRemoved :
			instanceConnection := this.pool.GetInstanceByName(e.Instance.Name())
			if instanceConnection != nil {
				instanceConnection.Close()
			}
		case *dist.TopicReceived :
			this.logger.Println("DEBUG Received topic : " + e.Topic.Name)
			existingTopic := configuration.GetTopic(e.Topic.Name)
			if existingTopic != nil {
				this.logger.Println("DEBUG Skipped because allready known")
			} else {
				configuration.AddTopic(e.Topic)
			}
		case *dist.InstanceReceived :
			host := this.context.GetProperty("host").(string)
			if (e.Instance.Host == host) && (e.Instance.Port == this.port) {
				//this.logger.Println("DEBUG Skipped instance cause it is me :)")
			} else {
				if configuration.AddInstance(e.Instance) {
					this.logger.Println("DEBUG Added instance :",e.Instance)
				}
			}
		case *dist.InstanceDisconnected :
			this.logger.Println("WARNING InstanceDisconnected",e.Instance.Name())
		default:
			//this.logger.Println("Unknown",reflect.TypeOf(aEvent))
	}
}
/**
 * Scan not connected Instances and try to Connect
 */
func (this *SyncService) scanInstances() {
	configuration := this.context.GetProperty("configuration").(*conf.Configuration)
	const SAY_IT = 0
	const WAIT_FOR = 100
	timeBeforeSaying := SAY_IT
	time.Sleep(2 * time.Second)
	for this.running {
		for _,instance := range configuration.Instances {
			if !instance.Connected {
				if timeBeforeSaying == SAY_IT {
					this.logger.Println("INFO trying to connect to ",instance.Name())
				}
				err := this.pool.Connect(instance)
				if err != nil {
					if timeBeforeSaying == SAY_IT {
						this.logger.Println("WARNING Error while connecting to ",instance.Name(),err.Error())
					}
				} else {
					this.logger.Println("INFO Connected to ",instance.Name())
				}
			}
		}
		if timeBeforeSaying == SAY_IT {
			timeBeforeSaying = WAIT_FOR
		}
		timeBeforeSaying--
		time.Sleep(2 * time.Second)
	}
}
func (this *SyncService) GetName () string{
	return "SYNC"
}
func (this *SyncService) Stop (aBundleContext osgi.BundleContext){
	this.running = false
}