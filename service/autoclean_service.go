package service

import (
	"github.com/milak/mmqapi/conf"
	"github.com/milak/tools/osgi"
	osgiservice "github.com/milak/tools/osgi/service"
	"github.com/milak/mmq/item"
	"time"
	"log"
)
/**
 * AutoClean Service removes expired items according to TimeToLive parameter in Topic properties.
 */
type AutoCleanService struct {
	context osgi.BundleContext
	logger	*log.Logger
	store   *item.ItemStore
	running bool
}
/**
 * Create a new AutoCleanService
 */
func NewAutoCleanService(aStore *item.ItemStore) *AutoCleanService {
	result := &AutoCleanService{store : aStore, running : false}
	return result;
}
/**
 * Start the service
 */
func (this *AutoCleanService) Start(aBundleContext osgi.BundleContext){
	this.context = aBundleContext
	this.logger =  this.context.GetService("LogService").Get().(*osgiservice.LogService).GetLogger()
	if !this.running {
		this.running = true
		go this.run()
	}
}
func (this *AutoCleanService) GetVersion() string {
	return "1.0.0"
}
func (this *AutoCleanService) GetSymbolicName() string {
	return "AutoClean"
}
/**
 * Stop the service
 */
func (this *AutoCleanService) Stop(aBundleContext osgi.BundleContext){
	this.running = false
}
func (this *AutoCleanService) run (){
	this.logger.Println("INFO Starting autoclean")
	// TODO prendre en compte lorsqu'un nouveau TOPIC est ajouté ou mis à jour via les évènements
	configuration := this.context.GetProperty("Configuration").(*conf.Configuration)
	topics, timeToLives := computeTimeToLivesAndTopics(this.logger,configuration)
	for this.running && this.context.GetBundle().GetState() == osgi.ACTIVE {
		time.Sleep(1 * time.Second)
		//this.logger.Println("Cleaning")
		for topicIndex,topic := range topics {
			//this.logger.Println("Topic ",topic.Name," ",timeToLives[topicIndex])
			iterator,_ := this.store.List(topic.Name)
			for iterator.HasNext() {
				item := iterator.Next().(*item.Item)
				age := item.GetAge()
				this.logger.Println("Computing for ",item," ",age)
				if age > timeToLives[topicIndex] {
					//fmt.Println("Removing ",item)
					this.store.RemoveItem(topic.Name,item)
				}
			}
		}
	}
}
func computeTimeToLivesAndTopics(aLogger *log.Logger, aConfiguration *conf.Configuration) ([]*conf.Topic, []time.Duration) {
	var topics []*conf.Topic
	var timeToLives []time.Duration
	for _,topic := range aConfiguration.Topics {
		duration,err := topic.GetTimeToLive()
		if err != nil {
			aLogger.Println("WARNING Unable to parse PARAMETER " + conf.PARAMETER_TIME_TO_LIVE + " '"+topic.GetParameterByName(conf.PARAMETER_TIME_TO_LIVE)+"' for topic " +topic.Name + " time to live will not be used",err)
			continue
		}
		if duration == nil {
			continue
		}
		timeToLives = append(timeToLives,*duration)
		topics = append(topics, topic)
	}
	return topics, timeToLives
}