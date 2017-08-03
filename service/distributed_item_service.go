package service

import (
	"github.com/milak/tools/event"
	"github.com/milak/tools/math"
	"github.com/milak/tools/osgi"
	osgiservice "github.com/milak/tools/osgi/service"
	"log"
	"math/rand"
	"github.com/milak/mmqapi/conf"
	"github.com/milak/mmq/dist"
	"github.com/milak/mmq/item"
	"strconv"
	"strings"
	"time"
)

type DistributedItemService struct {
	context                 osgi.BundleContext
	pool                    *dist.InstancePool
	store                   *item.ItemStore
	logger                  *log.Logger
	sharedItems             map[string]*dist.SharedItem
	receivedItemsByInstance map[string][]*dist.SharedItem
	_iterationBeforeLogging	int
}
const _ITERATIONS_BEFORE_LOGGING = 1
func NewDistributedItemService(aPool *dist.InstancePool, aStore *item.ItemStore) *DistributedItemService {
	result := &DistributedItemService{pool: aPool, store: aStore}
	result.sharedItems = make(map[string]*dist.SharedItem)
	result.receivedItemsByInstance = make(map[string][]*dist.SharedItem)
	result._iterationBeforeLogging = _ITERATIONS_BEFORE_LOGGING
	return result
}

// Catch event ItemAdded
func (this *DistributedItemService) Event(aEvent interface{}) {
	switch e := aEvent.(type) {
	case *item.ItemAdded:
		if e.Topic.IsDistributed() {
			distributionPolicy := e.Topic.GetParameterByName(conf.PARAMETER_DISTRIBUTED)
			this.logger.Println("DEBUG Received new distributed item. Policy :", distributionPolicy)
			if distributionPolicy == "0" || distributionPolicy == "1" || distributionPolicy == "" {
				this.logger.Println("DEBUG No need to distribute : distributionPolicy == '" + distributionPolicy + "'")
				return
			}
			groups := e.Topic.GetParameterByName(conf.PARAMETER_DISTRIBUTED_GROUPS)
			if groups == "" {
				groups = "all"
			}
			e.Item.SetShared(true)
			var sharedItem dist.SharedItem
			sharedItem.Topic = e.Topic.Name
			sharedItem.Policy = distributionPolicy
			sharedItem.Groups = groups
			this.sharedItems[e.Item.ID] = &sharedItem
			sharedItem.Item = e.Item
			go this._distributeItem(&sharedItem)
		}
	case *item.ItemRemoved:
		// When a shared item is removed
		if !e.Item.IsShared() {
			return
		}
		this.logger.Println("DEBUG shared item removed", e.Item)
		sharedItem := this.sharedItems[e.Item.ID]
		if sharedItem == nil {
			// TODO see whether it is normal, what to do ?
			return
		}
		delete(this.sharedItems, e.Item.ID)
		for _, i := range sharedItem.Instances {
			instanceConnection := this.pool.GetInstanceByName(i)
			if instanceConnection != nil {
				this.logger.Println("DEBUG Saying it to", i)
				instanceConnection.SendRemoveItem(e.Item.ID)
			} else {
				this.logger.Println("WARNING unable to get connection to ", i)
			}
		}
	case *dist.ItemReceived:
		this.logger.Println("DEBUG ItemReceived item :", e.Item, " from :", e.From)
		sharedItems := this.receivedItemsByInstance[e.From.Name()]
		this.receivedItemsByInstance[e.From.Name()] = append(sharedItems, e.Item)
		this.logger.Println("DEBUG - now contains :")
		for _, i := range this.receivedItemsByInstance[e.From.Name()] {
			this.logger.Println(i)
		}
	case *dist.ItemContentReceived:
		this.logger.Println("DEBUG ItemContentReceived item :", e.ID, " from :", e.From)
		this.logger.Println("DEBUG ItemContentReceived content of the item", string(e.Content))
		this.store.StoreForDistributedItemService(e.ID,&e.Content)
	case *dist.ItemRemoved:
		this.logger.Println("DEBUG ItemRemove item :", e.ID, " from :", e.From)
		sharedItems := this.receivedItemsByInstance[e.From.Name()]
		for i, item := range sharedItems {
			if item.Item.ID == e.ID {
				this.receivedItemsByInstance[e.From.Name()] = append(sharedItems[0:i], sharedItems[i+1:]...)
				break
			}
		}
		this.logger.Println("DEBUG - now contains :")
		for _, i := range this.receivedItemsByInstance[e.From.Name()] {
			this.logger.Println(i)
		}
	case *dist.InstanceDisconnected:
		configuration := this.context.GetProperty("Configuration").(*conf.Configuration)
		me := this.context.GetProperty("InstanceName").(string)
		this.logger.Println("DEBUG Instance disconnected ", e.Instance.Name())
		// Maybe this instance shared items
		sharedItems := this.receivedItemsByInstance[e.Instance.Name()]
		if sharedItems != nil && len(sharedItems) != 0 {
			for _, sharedItem := range sharedItems {
				topic := configuration.GetTopic(sharedItem.Topic)
				if topic == nil {
					this.logger.Println("WARNING a shared item is stored in unknown topic", sharedItem.Topic)
					continue
				}
				// The master instance is KO, i got item shared, let's see if i am the next MASTER
				if len(sharedItem.Instances) == 0 {
					this.logger.Println("WARNING a shared instance is not shared to any one ???")
				} else {
					if sharedItem.Instances[0] != me {
						// i am not the next
						// I keep the content of the item
						// TODO WHat about verifying this instance is alive 
						continue
					}
					strategy := topic.GetParameterByName(conf.PARAMETER_DISTRIBUTION_STRATEGY)
					if strategy == "" {
						strategy = conf.PARAMETER_DISTRIBUTION_STRATEGY_AT_LEAST_ONCE
					}
					/**
					TODO take account the strategy
					PARAMETER_DISTRIBUTION_STRATEGY_AT_LEAST_ONCE
					PARAMETER_DISTRIBUTION_STRATEGY_EXACTLY_ONCE
					PARAMETER_DISTRIBUTION_STRATEGY_AT_MOST_ONCE
					*/
					// I have to take this item as mine :
					// Remove previous first owner (me)
					sharedItem.Instances = sharedItem.Instances[1:]
					// Add this item in my shared items
					this.sharedItems[sharedItem.Item.ID] = sharedItem
					// Add the item in the item list in my topic
					this.store.LinkForDistributedItemService(sharedItem.Item,topic)
					// let's say other instances i manage the item
					for _, i := range sharedItem.Instances {
						instanceConnection := this.pool.GetInstanceByName(i)
						if instanceConnection != nil {
							instanceConnection.SendItem(sharedItem)
						}
					}
				}
			}
		}
		delete(this.receivedItemsByInstance, e.Instance.Name())
		// Maybe i shared instances with this instance
		for _,sharedItem := range this.sharedItems {
			for _,instance := range sharedItem.Instances {
				// Yes, indeed, i shared item with it, let's simply remove, the distribute batch will try to share with other instances 
				if instance == e.Instance.Name() {
					this.logger.Println("DEBUG removing item share with ", instance)
					sharedItem.RemoveInstance(instance)
					// Let's say to other instances
					for _,otherInstance := range sharedItem.Instances {
						instanceConnection := this.pool.GetInstanceByName(otherInstance)
						if instanceConnection != nil {
							instanceConnection.SendItem(sharedItem)
						}
					} 
					break;
				}
			}
		}
		
	}
}

/**
 * Ensures items are sufficently shared according to policy
 */
func (this *DistributedItemService) _distribute() {
	for this.context.GetBundle().GetState() == osgi.ACTIVE {
		time.Sleep(1 * time.Second)
		this._iterationBeforeLogging--
		for _, sharedItem := range this.sharedItems {
			this._distributeItem(sharedItem)
		}
		if this._iterationBeforeLogging == 0 {
			this._iterationBeforeLogging = _ITERATIONS_BEFORE_LOGGING
		}
	}
}
func (this *DistributedItemService) _filterInstances(aGroups string) []*conf.Instance {
	configuration := this.context.GetProperty("Configuration").(*conf.Configuration)
	var retainedInstances []*conf.Instance
	for _, i := range configuration.Instances {
		if i.Connected {
			retained := false
			for _, g := range i.Groups {
				if g == aGroups {
					retained = true
					break
				}
			}
			if !retained {
				continue
			}
			retainedInstances = append(retainedInstances, i)
		}
	}
	return retainedInstances
}
func (this *DistributedItemService) _computeCount(aDistributionPolicy string, aNbInstances int) (int,error) {
	var err error
	var result int
	// ne devrait pas être == conf.DISTRIBUTED_NO
	if aDistributionPolicy == conf.DISTRIBUTED_ALL {
		result = aNbInstances
	} else if strings.HasSuffix(aDistributionPolicy, "%") {
		percent, err := strconv.Atoi(aDistributionPolicy[0 : len(aDistributionPolicy)-1])
		if err != nil {
			return -1, err
		}
		result = (percent * aNbInstances / 100)
		if math.Odd(aNbInstances) {
			result++
		}
		if result == 0 {
			result = 1
		}
	} else {
		result, err = strconv.Atoi(aDistributionPolicy)
		if err != nil {
			return -1, err
		}
		if result > aNbInstances {
			result = aNbInstances
		}
	}
	return result,nil
}
/**
 *
 */
func (this *DistributedItemService) _distributeItem(aSharedItem *dist.SharedItem) {
	// Let's compute how much instances should own the item
	retainedInstances := this._filterInstances(aSharedItem.Groups)
	nbInstances := len(retainedInstances)
	if nbInstances == 0 {
		if this._iterationBeforeLogging == 0 {
			this.logger.Println("WARNING Unable to apply distribution : no suitable connected instances")
		}
		return
	}
	//this.logger.Println("I am connected with :", nbInstances, "instances, retained : ", retainedInstances)
	nbInstances++ // on ajoute l'instance courante en plus
	count,err := this._computeCount(aSharedItem.Policy, nbInstances)
	if err != nil {
		if this._iterationBeforeLogging == 0 {
			this.logger.Println("WARNING Unable to apply distribution : ", aSharedItem.Policy, aSharedItem.Topic, err)
		}
		return
	}
	//this.logger.Println("The item must be distributed with :", count, " instances (including me)")
	count-- // removing myself
	// Do we have enougth ?
	if len(aSharedItem.Instances) >= count {
		//this.logger.Println("I have enougth ",len(aSharedItem.Instances))
		return // ok, nothing to do
	}
	// Let's remove allready owning instances from retainedInstances
	filteredInstances := []*conf.Instance{}
	for _,retained := range retainedInstances {
		found := false
		for _,instance := range aSharedItem.Instances {
			if instance == retained.Name() {
				found = true
				break;
			}
		}
		if !found {
			filteredInstances = append(filteredInstances,retained)
		}
	}
	retainedInstances = filteredInstances
	// How much to add ?
	count = count - len(aSharedItem.Instances)
	if this._iterationBeforeLogging == 0 {
		this.logger.Println("DEBUG I need :", count, " more instances to share with me")
	}
	if count > len(retainedInstances) {
		count = len(retainedInstances)
	}
	this.logger.Println("DEBUG I will take ",count, "instances in", len(retainedInstances)," available instances")
	// distribute item to other instances
	// let's randomly permut 
	now := time.Now()
	rand.Seed(int64(now.Nanosecond()))
	ids := rand.Perm(len(retainedInstances))
	this.logger.Println("DEBUG ids",ids)
	// let's select the instances
	newInstance := make(map[string]bool)
	for count > 0 {
		id := ids[count-1]
		this.logger.Println("DEBUG id",id)
		i := retainedInstances[id]
		this.logger.Println("DEBUG Selected", i)
		newInstance[i.Name()]=true
		aSharedItem.AddInstance(i.Name())
		count--
	}
	if count > 0 {
		if this._iterationBeforeLogging == 0 {
			this.logger.Println("WARNING I am missing :", count, " more instances")
		}
	}
	// Let's share the item
	for _, i := range aSharedItem.Instances {
		instanceConnection := this.pool.GetInstanceByName(i)
		if instanceConnection != nil {
			//this.logger.Println("Distributing with", i)
			instanceConnection.SendItem(aSharedItem)
			// Need to send the content ?
			if _,isNew := newInstance[i]; isNew {
				//this.logger.Println("Not yet shared with him ", i)
				bytes := make([]byte, 2000)
				reader, err := this.store.GetContent(aSharedItem.Item.ID, false)
				if err != nil {
					this.logger.Println("WARNING I can't find the content of an item ", aSharedItem.Item.ID)
					continue
				}
				count, _ := reader.Read(bytes)
				writer := instanceConnection.SendItemContent(aSharedItem.Item.ID, count)
				writer.Write(bytes[0:count])
				/** TODO support BIG FILE
				for count >= 0 {
					writer.Write(bytes[0:count])
					count,_ = e.Item.Read(bytes)
				}*/
			}
		} else {
			if this._iterationBeforeLogging == 0 {
				this.logger.Println("WARNING unable to get connection to ", i)
			}
		}
	}
}
func (this *DistributedItemService) Start(aBundleContext osgi.BundleContext) {
	this.context = aBundleContext
	this.logger = aBundleContext.GetService("LogService").Get().(*osgiservice.LogService).GetLogger()
	event.Bus.AddListener(this)
	go this._distribute()
}
func (this *DistributedItemService) GetVersion() string {
	return "1.0.0"
}
func (this *DistributedItemService) GetSymbolicName() string {
	return "DistributedItem"
}
func (this *DistributedItemService) Stop(aBundleContext osgi.BundleContext) {
	event.Bus.RemoveListener(this)
}
