package item

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/milak/tools/event"
	"github.com/milak/tools/list"
	"io"
	"github.com/milak/mmqapi/conf"
	"github.com/milak/mmq/env"
	"strconv"
)

type ItemStore struct {
	itemsByTopic   map[string]*ItemList
	contentsByItem map[string]*ItemContent
	context        *env.Context
}
type ItemContent struct {
	bytes      []byte
	linkNumber int // the number of link with this content
}
type StoreError struct {
	Message string
	Topic   string
	Item    string
}

func (this StoreError) Error() string {
	return fmt.Sprintf("%s : topic = %s item = %s", this.Message, this.Topic, this.Item)
}
func NewStore(aContext *env.Context) *ItemStore {
	result := &ItemStore{itemsByTopic: make(map[string]*ItemList), contentsByItem: make(map[string]*ItemContent), context: aContext}
	return result
}
/**
 * Warning this method is not a good way to store content for shareditem.
 * TODO : find a best way to do
 */
func (this *ItemStore) StoreForDistributedItemService(aItemId string, aBytes *[]byte){
	this.contentsByItem[aItemId] = &ItemContent{ bytes : *aBytes, linkNumber : 1}
	this.context.Logger.Println("DEBUG Stored for '"+aItemId+"' data ", string(*aBytes))
}
/**
 * Warning this method is not a good way to link an item in a topic.
 * TODO : find a best way to do
 */
func (this *ItemStore) LinkForDistributedItemService(aItem *Item, aTopic *conf.Topic) error {
	// Todo vérifier la taille maximale du topic etc.
	return this._addItemInTopic(aItem, aTopic, false)
}
func (this *ItemStore) Push(aItem *Item, aContent io.Reader) error {
	var buffer bytes.Buffer
	bytes := make([]byte, 2000)
	var total int32 = 0
	count, _ := aContent.Read(bytes)
	for count > 0 {
		total += int32(count)
		buffer.Write(bytes[0:count])
		count, _ = aContent.Read(bytes)
	}
	closer, isCloser := aContent.(io.Closer)
	if isCloser {
		closer.Close()
	}
	if !aItem.HasProperty(PROPERTY_SIZE) {
		aItem.AddProperty(PROPERTY_SIZE, strconv.Itoa(int(total)))
	}
	this.contentsByItem[aItem.ID] = &ItemContent{bytes: buffer.Bytes(), linkNumber: len(aItem.Topics)}
	// Pour chaque topic pour lequel il est enregistré
	for _, topicName := range aItem.Topics {
		topic := this.context.Configuration.GetTopic(topicName)
		if topic == nil {
			this.contentsByItem[aItem.ID] = nil
			return StoreError{"Topic not found", topicName, "nil"}
		}
		if topic.Type == conf.VIRTUAL {
			this.contentsByItem[aItem.ID] = nil
			return StoreError{"Unable to push in virtual topic", topicName, "nil"}
		}
		maxItemSize, err := topic.GetMaxItemSize()
		this.context.Logger.Println("DEBUG Max item size ", maxItemSize)
		if err != nil {
			this.contentsByItem[aItem.ID] = nil
			return errors.New("Unable to get max item size in topic" + err.Error())
		}
		if maxItemSize != 0 && total > maxItemSize {
			this.contentsByItem[aItem.ID] = nil
			return errors.New("Item size exceeds max item size property of topic " + topicName)
		}
		err = this._addItemInTopic(aItem,topic,true)
		if err != nil {
			this.contentsByItem[aItem.ID] = nil
			return err
		}
	}
	this.context.Logger.Println("DEBUG Item successfully added")
	return nil
}
func (this *ItemStore) _addItemInTopic(aItem *Item, aTopic *conf.Topic, aFireEvent bool) error {
	topicName := aTopic.Name
	items := this.itemsByTopic[topicName]
	if items == nil {
		items = NewItemList()
		this.itemsByTopic[topicName] = items
	}
	this.itemsByTopic[topicName].AddInTail(aItem)
	if aFireEvent {
		event.Bus.FireEvent(&ItemAdded{aItem, aTopic})
	}
	MaxItemCountString := aTopic.GetParameterByName(conf.PARAMETER_MAX_ITEM_COUNT)
	if MaxItemCountString != "" && MaxItemCountString != "unlimited" {
		MaxItemCount, err := strconv.Atoi(MaxItemCountString)
		if err != nil {
			return StoreError{"Unable to parse " + conf.PARAMETER_MAX_ITEM_COUNT + " parameter in topic " + topicName + " " + MaxItemCountString, topicName, "nil"}
		}
		if items.Size() > MaxItemCount {
			go this.Pop(topicName)
		}
	}
	return nil
}

/**
 * Get a item by its ID
 * TODO optimize this method
 */
func (this *ItemStore) GetItem(aId string) *Item {
	for _, items := range this.itemsByTopic {
		if items == nil {
			continue
		}
		iterator := items.Iterator()
		for iterator.HasNext() {
			item := iterator.Next().(*Item)
			if item.ID == aId {
				return item
			}
		}
	}
	return nil
}
func (this *ItemStore) GetContent(aItemID string, purge bool) (io.Reader, error) {
	this.context.Logger.Println("DEBUG Getting content for '"+aItemID+"'")
	itemContent := this.contentsByItem[aItemID]
	if itemContent == nil {
		this.context.Logger.Println("WARNING no content found for '"+aItemID+"'")
		return nil, errors.New("Item not found")
	}
	theBytes := itemContent.bytes
	if theBytes == nil {
		this.context.Logger.Println("WARNING no content found for '"+aItemID+"'")
		return nil, errors.New("Item not found")
	}
	result := bytes.NewBuffer(theBytes)
	if purge {
		this.RemoveContent(aItemID)
	}
	return result, nil
}
func (this *ItemStore) Pop(aTopicName string) (*Item, io.Reader, error) {
	topic := this.context.Configuration.GetTopic(aTopicName)
	if topic == nil {
		return nil, nil, StoreError{"Topic not found", aTopicName, "nil"}
	}
	if topic.Type == conf.SIMPLE {
		items := this.itemsByTopic[aTopicName]
		if items == nil || items.IsEmpty() {
			return nil, nil, nil
		} else {
			item := items.PopHead()
			event.Bus.FireEvent(&ItemRemoved{item, topic})
			content, _ := this.GetContent(item.ID, true)
			return item, content, nil
		}
	} else {
		subTopics := topic.TopicList
		strategy := topic.GetParameterByName(conf.PARAMETER_STRATEGY)
		if strategy == "" {
			strategy = conf.ORDERED
		}
		if strategy == conf.ORDERED {
			// Par défaut on est en mode ORDERED : on vide le premier topic avant de vider le second
			for _, subTopicName := range subTopics {
				subTopic := this.context.Configuration.GetTopic(subTopicName)
				if subTopic == nil {
					return nil, nil, StoreError{"Topic not found", subTopicName, "nil"}
				}
				items := this.itemsByTopic[subTopicName]
				// If the topic has at least an item
				if items != nil && !items.IsEmpty() {
					item := items.PopHead()
					event.Bus.FireEvent(&ItemRemoved{item, subTopic})
					content, _ := this.GetContent(item.ID, true)
					return item, content, nil
				}
			}
		} else if strategy == conf.ROUND_ROBIN {
			// TODO implémenter la stratégie ROUND-ROBIN
			// Pour implémenter ROUND-ROBIN, il va falloir conserver un indicateur pour savoir la file que l'on a lu le coup précédent
			return nil, nil, errors.New("ROUND ROBIN strategy not yet implemented")
		} else {
			return nil, nil, errors.New(strategy + " strategy not recognized")
		}
		return nil, nil, nil
	}
}
func (this *ItemStore) RemoveContent(aItemID string) error {
	if this.contentsByItem[aItemID] == nil {
		return errors.New("Cannot find item "+aItemID)
	}
	this.contentsByItem[aItemID].linkNumber--
	if this.contentsByItem[aItemID].linkNumber <= 0 {
		delete(this.contentsByItem, aItemID)
	}
	return nil
}
func (this *ItemStore) RemoveItem(aTopicName string, aItem *Item) error {
	topic := this.context.Configuration.GetTopic(aTopicName)
	if topic == nil {
		return StoreError{"Topic not found", aTopicName, aItem.ID}
	}
	items := this.itemsByTopic[aTopicName]
	iterator := items.Iterator()
	for iterator.HasNext() {
		itemI, node := iterator.NextWithNode()
		item := itemI.(*Item)
		if item.ID == aItem.ID {
			items.Remove(node)
			event.Bus.FireEvent(&ItemRemoved{item, topic})
			// TODO determine if i have to remove item (cause can be in other topics
			this.RemoveContent(aItem.ID)
			return nil
		}
	}
	return StoreError{"Item not found in topic", aTopicName, aItem.ID}
}
func (this *ItemStore) List(aTopicName string) (list.Iterator, error) {
	topic := this.context.Configuration.GetTopic(aTopicName)
	if topic == nil {
		return nil, StoreError{"Topic not found", aTopicName, "nil"}
	}
	var result list.Iterator = nil
	if topic.Type == conf.SIMPLE {
		itemList := this.itemsByTopic[aTopicName]
		if itemList == nil {
			topic := this.context.Configuration.GetTopic(aTopicName)
			if topic != nil {
				result = NewEmptyIterator()
			} else {
				return nil, errors.New("Topic not found " + aTopicName)
			}
		} else {
			result = itemList.Iterator()
		}
	} else if topic.Type == conf.VIRTUAL {
		subTopics := topic.TopicList
		iterators := []list.Iterator{}
		for _, subTopicName := range subTopics {
			items := this.itemsByTopic[subTopicName]
			if items == nil {
				//faut-il vérifier que le topic existe ?
				subTopic := this.context.Configuration.GetTopic(subTopicName)
				if subTopic == nil {
					return nil, errors.New(subTopicName + " subTopic not found")
				}
				continue
			}
			if items.IsEmpty() {
				continue
			}
			iterators = append(iterators, items.Iterator())
		}
		strategy := topic.GetParameterByName(conf.PARAMETER_STRATEGY)
		if strategy == "" {
			strategy = conf.ORDERED
		}
		if strategy == conf.ORDERED {
			result = NewOrderedIterator(iterators)
		} else if strategy == conf.ROUND_ROBIN {
			result = NewRoundRobinIterator(iterators)
		} else {
			return nil, errors.New(strategy + " strategy not recognized")
		}
	} else {
		return nil, errors.New(topic.Type + " type not recognized")
	}
	return result, nil
}
func (this *ItemStore) Count(aTopicName string) int {
	itemList := this.itemsByTopic[aTopicName]
	if itemList == nil {
		return 0
	} else {
		return itemList.Size()
	}
}
