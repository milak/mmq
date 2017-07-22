package service

import (
	"bytes"
	"encoding/json"
	"github.com/milak/mmqapi/conf"
	"github.com/milak/mmqapi/env"
	"github.com/milak/mmq/item"
	"github.com/milak/tools/event"
	"github.com/milak/tools/network"
	"github.com/google/uuid"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
	"fmt"
)

type instance struct {
	context *env.Context
}

func (this *instance) Get(w http.ResponseWriter, req *http.Request) {
	w.Header().Add("Access-Control-Allow-Origin", "*")
	req.ParseForm()
	callback := req.Form["callback"]
	w.WriteHeader(http.StatusOK)
	if callback != nil {
		w.Write([]byte(callback[0] + "("))
	}
	encoder := json.NewEncoder(w)
	encoder.Encode(this.context.Configuration.Instances)
	if callback != nil {
		w.Write([]byte(")"))
	}
}
func (this *instance) Delete(w http.ResponseWriter, req *http.Request) {
	w.Header().Add("Access-Control-Allow-Origin", "*")
	instanceName := req.URL.Path
	instanceName = instanceName[len("/API/instance/"):]
	removedInstance := this.context.Configuration.RemoveInstance(instanceName)
	if removedInstance == nil {
		_notFound(w)
	} else {
		event.Bus.FireEvent(&conf.InstanceRemoved{removedInstance})
		w.WriteHeader(http.StatusOK)
	}
}

type shutdown struct {
	context *env.Context
}

func (this *shutdown) Get(w http.ResponseWriter, req *http.Request) {
	w.WriteHeader(http.StatusOK)
	this.context.Running = false
	w.Write([]byte("server shutdown"))
	//http.DefaultServeMux.Shutdown()
}

type service struct {
	context *env.Context
}

func (this *service) Get(w http.ResponseWriter, req *http.Request) {
	w.Header().Add("Access-Control-Allow-Origin", "*")
	w.WriteHeader(http.StatusOK)
	encoder := json.NewEncoder(w)
	encoder.Encode(this.context.Configuration.Services)
}

type _log struct {
	context *env.Context
}

func (this *_log) Get(w http.ResponseWriter, req *http.Request) {
	file, err := os.Open("mmq.log")
	if err != nil {
		_notFound(w)
		this.context.Logger.Println("WARNING Unable to open log file", err)
	} else {
		w.WriteHeader(http.StatusOK)
		data := make([]byte, 100)
		count, err := file.Read(data)
		if err != nil {
			this.context.Logger.Println("WARNING Unable to open log file", err)
		} else {
			for count > 0 {
				w.Write(data[:count])
				count, err = file.Read(data)
			}
		}
		file.Close()
	}
}

type info struct {
	context *env.Context
	port string
}

func (this *info) Get(w http.ResponseWriter, req *http.Request) {
	w.Header().Add("Access-Control-Allow-Origin", "*")
	req.ParseForm()
	callback := req.Form["callback"]
	w.WriteHeader(http.StatusOK)
	if callback != nil {
		w.Write([]byte(callback[0] + "("))
	}
	encoder := json.NewEncoder(w)
	encoder.Encode(struct {
		Version string
		Host    string
		Port    string
		Name    string
		Groups  []string
	}{Version: this.context.Configuration.Version, Host: this.context.Host, Port: this.port, Name: this.context.Host + ":" + this.port, Groups: this.context.Configuration.Groups})
	if callback != nil {
		w.Write([]byte(")"))
	}
}

type _item struct {
	context *env.Context
	store *item.ItemStore
}

func (this *_item) Get(w http.ResponseWriter, req *http.Request) {
	this.context.Logger.Println("DEBUG entering item REST API", req)
	w.Header().Add("Access-Control-Allow-Origin", "*")
	id := req.URL.Path
	id = id[len("/API/item/"):]
	this.context.Logger.Println("DEBUG getting content of item ", id)
	item := this.store.GetItem(id)
	if item == nil {
		_notFound(w)
	} else {
		reader, err := this.store.GetContent(id, false)
		if err != nil {
			this.context.Logger.Println("ERROR i have not the content for a owned item ", id)
			w.WriteHeader(http.StatusInternalServerError)
		} else {
			_writeItem(w, item, reader)
		}
	}
}
func (this *_item) Put(w http.ResponseWriter, req *http.Request) {
	this.Post(w, req)
}
func (this *_item) Post(w http.ResponseWriter, req *http.Request) {
	//req.ParseForm()
	multipart := true
	erro := req.ParseMultipartForm(http.DefaultMaxHeaderBytes)
	if erro != nil {
		multipart = false
	}
	// Processing topic
	if len(req.Form["topic"]) == 0 {
		w.WriteHeader(http.StatusNotAcceptable)
		if len(req.Form["topic"]) == 0 {
			w.Write([]byte("Missing topic argument"))
		}
		return
	}
	topics := []string{}
	for _, topicName := range req.Form["topic"] {
		topics = append(topics, topicName)
	}
	// Processing value
	var content io.Reader
	if multipart {
		file, _, err := req.FormFile("value")
		content = file
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(err.Error()))
			return
		}
		defer file.Close()

	} else {
		if len(req.Form["value"]) == 0 {
			w.WriteHeader(http.StatusNotAcceptable)
			w.Write([]byte("Missing value argument"))
			return
		}
		content = bytes.NewBuffer([]byte(req.Form["value"][0]))
	}
	item := item.NewItem(topics)
	for i, key := range req.Form["property-name"] {
		value := req.Form["property-value"][i]
		item.AddProperty(key, value)
	}
	this.context.Logger.Println("DEBUG Adding item")
	err := this.store.Push(item, content)
	if err != nil {
		this.context.Logger.Println("DEBUG Failed to add", err)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
	} else {
		this.context.Logger.Println("DEBUG Item added")
		w.WriteHeader(http.StatusCreated)
	}
}
func _writeItem(w http.ResponseWriter, aItem *item.Item, aReader io.Reader) {
	w.Header().Add("id", aItem.ID)
	properties := "["
	for i, p := range aItem.Properties {
		if i != 0 {
			properties += ","
		}
		properties += "{\"name\" : \"" + p.Name + "\", \"value\" : \"" + p.Value + "\"}"
	}
	properties += "]"
	w.Header().Add("properties", properties)
	if aReader != nil {
		bytes := make([]byte, 2000)
		count, err := aReader.Read(bytes)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
		} else {
			w.WriteHeader(http.StatusOK)
			for count > 0 {
				w.Write(bytes[0:count])
				count, _ = aReader.Read(bytes)
			}
		}
	} else {
		w.WriteHeader(http.StatusNoContent)
	}
}

type topic struct {
	context *env.Context
	store *item.ItemStore
}

func (this *topic) Get(w http.ResponseWriter, req *http.Request) {
	w.Header().Add("Access-Control-Allow-Origin", "*")
	topicName := req.URL.Path
	topicName = topicName[len("/API/topic"):]
	if len(topicName) == 0 {
		w.Header().Add("Access-Control-Allow-Origin", "*")
		req.ParseForm()
		callback := req.Form["callback"]
		w.WriteHeader(http.StatusOK)
		if callback != nil {
			w.Write([]byte(callback[0] + "("))
		}
		encoder := json.NewEncoder(w)
		encoder.Encode(this.context.Configuration.Topics)
		if callback != nil {
			w.Write([]byte(")"))
		}
	} else if topicName == "/topics.opml" {
		rootUrl := "http://" + req.Host + "/"
		w.WriteHeader(http.StatusOK)
		w.Header().Set("content-type", "text/x-opml")
		w.Write([]byte("<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?>\n<opml version=\"2.0\">\n"))
		w.Write([]byte("<head>\n<title>MMQ topic.opml</title>\n"))
		w.Write([]byte("<dateCreated>"+time.Now().Format(env.DATE_FORMAT)+"</dateCreated>\n"))
		w.Write([]byte("<dateModified>"+time.Now().Format(env.DATE_FORMAT)+"</dateModified>\n"))
		w.Write([]byte("<ownerName>User</ownerName>\n"))
		w.Write([]byte("<ownerEmail>User@mailbox.com</ownerEmail>\n"))
		//w.Write([]byte("<expansionState></expansionState>\n"))
		//w.Write([]byte("<vertScrollState>1</vertScrollState>
		//<windowTop>61</windowTop>
		//<windowLeft>304</windowLeft>
		//<windowBottom>562</windowBottom>
		//<windowRight>842</windowRight>
		w.Write([]byte("</head>\n"))
		w.Write([]byte("<body>\n"))
		for _,topic := range this.context.Configuration.Topics {
			w.Write([]byte("<outline"))
			w.Write([]byte(" text=\""+topic.Name+"\""))
			w.Write([]byte(" description=\""+topic.Name+"\""))
			w.Write([]byte(" htmlUrl=\""+rootUrl+"\""))
			w.Write([]byte(" title=\"Topic : "+topic.Name+"\""))
			w.Write([]byte(" language=\"english\""))
			w.Write([]byte(" type=\"rss\"")) //w.Write([]byte(" type=\"atom\""))
			w.Write([]byte(" version=\"RSS2\"")) //w.Write([]byte(" version=\"ATOM1\""))
			w.Write([]byte(" xmlUrl=\""+rootUrl+"/API/topic/"+topic.Name+"/rss\""))//w.Write([]byte(" xmlUrl=\""+rootUrl+"/API/topic/"+topic.Name+"/atom\""))
			w.Write([]byte("/>"))
		}
		w.Write([]byte("</body>\n"))
		w.Write([]byte("</opml>"))
	} else if strings.HasSuffix(topicName, "/pop") {
		topicName = topicName[1 : len(topicName)-len("/pop")]
		item, reader, err := this.store.Pop(topicName)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(err.Error()))
		} else if item == nil {
			_notFound(w)
		} else {
			_writeItem(w, item, reader)
		}
	} else if strings.HasSuffix(topicName, "/list") {
		topicName = topicName[1 : len(topicName)-len("/list")]
		iterator, err := this.store.List(topicName)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("Error during list " + err.Error()))
			this.context.Logger.Println("ERROR items list", err)
			return
		}
		var displayableItems []DisplayableItem
		for iterator.HasNext() {
			item, _ := iterator.Next().(*item.Item)
			displayableItems = append(displayableItems, DisplayableItem{ID: item.ID, Age: item.GetAge(), Properties: item.Properties})
		}
		this.context.Logger.Println("DEBUG items list of "+topicName, displayableItems)
		w.WriteHeader(http.StatusOK)
		callback := req.Form["callback"]
		if callback != nil {
			w.Write([]byte(callback[0] + "("))
		}
		encoder := json.NewEncoder(w)
		encoder.Encode(displayableItems)
		if callback != nil {
			w.Write([]byte(")"))
		}
	} else if strings.HasSuffix(topicName, "/rss") {
		rootUrl := "http://" + req.Host + "/"
		topicName = topicName[1 : len(topicName)-len("/rss")]
		iterator, err := this.store.List(topicName)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("Error during list " + err.Error()))
			this.context.Logger.Println("ERROR items list", err)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Header().Set("content-type", "xml/rss")
		w.Write([]byte("<?xml version=\"1.0\"?>\n<rss version=\"2.0\">\n"))
		w.Write([]byte("<channel>\n"))
		w.Write([]byte("<title>Content of the Topic " + topicName + "</title>\n"))
		w.Write([]byte("<link>" + rootUrl + "API/topic/" + topicName + "/rss</link>\n"))
		w.Write([]byte("<description>Content of the Topic " + topicName + "</description>"))
		w.Write([]byte("<language>en-US</language>\n"))
		w.Write([]byte("<pubDate>" + time.Now().Format(env.DATE_FORMAT) + "</pubDate>\n"))
		w.Write([]byte("<lastBuildDate>" + time.Now().Format(env.DATE_FORMAT) + "</lastBuildDate>\n"))
		w.Write([]byte("<docs>https://github.com/milak/magicmq</docs>\n"))
		w.Write([]byte("<generator>Magic MQ " + this.context.Configuration.Version + "</generator>\n"))
		//w.Write([]byte("<managingEditor></managingEditor>"))
		//w.Write([]byte("<webMaster>webmaster@example.com</webMaster>"))
		index := 0
		for iterator.HasNext() {
			item, _ := iterator.Next().(*item.Item)
			w.Write([]byte("<item>\n"))
			w.Write([]byte("<title>Item #" + item.ID + "</title>\n"))
			w.Write([]byte("<link>" + rootUrl + "API/item/" + item.ID + "</link>\n"))
			description := "Item stored "
			topicList := ""
			for i, topic := range item.Topics {
				if i > 0 {
					topicList += ", "
				}
				topicList += topic
			}
			if len(item.Topics) > 1 {
				description += "in the topics : " + topicList
			} else {
				description += "in the topic : " + topicList
			}
			description += "\nSince " + item.CreationDate.Format(env.DATE_FORMAT)
			if len(item.Properties) > 0 {
				properties := ""
				for i, property := range item.Properties {
					if i > 0 {
						topicList += ", "
					}
					properties += property.Name + " = " + property.Value + "\n"
				}
				description += "Properties : \n" + properties
			}
			w.Write([]byte("<description>" + description + "</description>\n"))
			w.Write([]byte("<pubDate>" + item.CreationDate.Format(env.DATE_FORMAT) + "</pubDate>\n"))
			w.Write([]byte("<guid>" + item.ID + "</guid>\n"))
			w.Write([]byte("</item>\n"))
			index++
			if index == 10 {
				break
			}
		}
		w.Write([]byte("</channel>\n"))
		w.Write([]byte("</rss>\n"))
	} else if strings.HasSuffix(topicName, "/atom") {
		rootUrl := "http://" + req.Host + "/"
		topicName = topicName[1 : len(topicName)-len("/atom")]
		iterator, err := this.store.List(topicName)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("Error during list " + err.Error()))
			this.context.Logger.Println("ERROR items list", err)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Header().Set("content-type", "application/atom+xml")
		w.Write([]byte("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n"))
	    w.Write([]byte("<feed xmlns=\"http://www.w3.org/2005/Atom\">\n"))
		w.Write([]byte("<title>Content of the Topic " + topicName + "</title>\n"))
		w.Write([]byte("<link href=\""+ rootUrl + "API/topic/" + topicName + "/atom\"/>\n"))
		w.Write([]byte("<updated>"+time.Now().Format(env.DATE_FORMAT)+"</updated>"))
		w.Write([]byte("<author>\n<name>MMQ</name>\n</author>\n"))
		w.Write([]byte("<id>urn:uuid:"+uuid.New().String()+"</id>"))
		index := 0
		for iterator.HasNext() {
			item, _ := iterator.Next().(*item.Item)
			w.Write([]byte("<entry>\n"))
			w.Write([]byte("<title>Item #" + item.ID + "</title>\n"))
			w.Write([]byte("<link href=\"" + rootUrl + "API/item/" + item.ID + "\"/>\n"))
			w.Write([]byte("<id>urn:uuid:"+item.ID+"</id>"))
			w.Write([]byte("<updated>"+item.CreationDate.Format(env.DATE_FORMAT)+"</updated>"))
			description := "Item stored "
			topicList := ""
			for i, topic := range item.Topics {
				if i > 0 {
					topicList += ", "
				}
				topicList += topic
			}
			if len(item.Topics) > 1 {
				description += "in the topics : " + topicList
			} else {
				description += "in the topic : " + topicList
			}
			description += "\nSince " + item.CreationDate.Format(env.DATE_FORMAT)
			if len(item.Properties) > 0 {
				properties := ""
				for i, property := range item.Properties {
					if i > 0 {
						topicList += ", "
					}
					properties += property.Name + " = " + property.Value + "\n"
				}
				description += "Properties : \n" + properties
			}
			w.Write([]byte("<summary>" + description + "</summary>\n"))
			w.Write([]byte("</entry>\n"))
			index++
			if index == 10 {
				break
			}
		}
		w.Write([]byte("</feed>\n"))
	} else {
		topic := this.context.Configuration.GetTopic(topicName[1:])
		if topic == nil {
			_notFound(w)
		} else {
			req.ParseForm()
			w.WriteHeader(http.StatusOK)
			callback := req.Form["callback"]
			if callback != nil {
				w.Write([]byte(callback[0] + "("))
			}
			encoder := json.NewEncoder(w)
			encoder.Encode(topic)
			if callback != nil {
				w.Write([]byte(")"))
			}
		}
	}

}
func (this *topic) Delete(w http.ResponseWriter, req *http.Request) {
	w.Header().Add("Access-Control-Allow-Origin", "*")
	topicName := req.URL.Path
	topicName = topicName[len("/API/topic/"):]
	if !this.context.Configuration.RemoveTopic(topicName) {
		_notFound(w)
	} else {
		w.WriteHeader(http.StatusOK)
	}
}
func (this *topic) Post(w http.ResponseWriter, req *http.Request) {
	w.Header().Add("Access-Control-Allow-Origin", "*")
	req.ParseForm()
	name := req.Form["name"]
	if name == nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Missing name parameter"))
		return
	}
	if len(name) != 1 {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Wrong number of 'name' argument"))
		return
	}
	topic := this.context.Configuration.GetTopic(name[0])
	// TODO what about update ???
	if topic != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Topic allready exist"))
		return
	}
	topictypeArg := req.Form["type"]
	if topictypeArg == nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Missing type parameter"))
		return
	}
	topicType := topictypeArg[0]
	if topicType == conf.VIRTUAL {
		strategyArg := req.Form["strategy"]
		if strategyArg == nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("Missing strategy parameter"))
			return
		}
		strategy := strategyArg[0]
		if strategy != conf.ROUND_ROBIN && strategy != conf.ORDERED {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("Only " + conf.ROUND_ROBIN + " and " + conf.ORDERED + " values are accepted for topic strategy parameter"))
			return
		}
		topics := []string{}
		for _, subTopicName := range req.Form["topic"] {
			subTopic := this.context.Configuration.GetTopic(subTopicName)
			if subTopic == nil {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte("Sub topic not found " + subTopicName + ""))
				return
			}
			topics = append(topics, subTopicName)
		}
		topic = conf.NewVirtualTopic(name[0], strategy, topics)
	} else if topicType == conf.SIMPLE {
		topic = conf.NewTopic(name[0])
	} else {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Only " + conf.SIMPLE + " and " + conf.VIRTUAL + " values are accepted for topic type parameter"))
		return
	}
	for i, key := range req.Form["parameter-name"] {
		value := req.Form["parameter-value"][i]
		topic.AddParameter(key, value)
	}
}

type HttpRestService struct {
	context *env.Context
	port    string
	store   *item.ItemStore
}

func NewHttpRestService(aContext *env.Context, aStore *item.ItemStore) *HttpRestService {
	return &HttpRestService{context: aContext, store: aStore}
}
func _notFound(w http.ResponseWriter) {
	w.WriteHeader(http.StatusNotFound)
	w.Write([]byte("Sorry the page you requested is not found"))
}
func (this *HttpRestService) methodNotSupported(w http.ResponseWriter, aMethod string) {
	w.WriteHeader(http.StatusMethodNotAllowed)
	w.Write([]byte("Sorry " + string(http.StatusMethodNotAllowed) + " error : method '" + aMethod + "' not allowed"))
}

type DisplayableItem struct {
	ID         string
	Age        time.Duration
	Properties []item.Property
}

func (this *HttpRestService) Start() {
	var port *string = nil
	for s := range this.context.Configuration.Services {
		service := this.context.Configuration.Services[s]
		if !service.Active {
			continue
		}
		if service.Name == "ADMIN" {
			var root *string = nil
			for p := range service.Parameters {
				if service.Parameters[p].Name == "root" {
					root = &service.Parameters[p].Value
					break
				}
			}
			if root == nil {
				this.context.Logger.Println("WARNING Configuration error : missing root parameter for ADMIN service")
				panic("Configuration error : missing root parameter for ADMIN service")
			}
			if _, err := os.Stat(*root); os.IsNotExist(err) {
				this.context.Logger.Println("WARNING Configuration error : root parameter for ADMIN service doesn't match existing directory '" + (*root) + "'")
			} else {
				this.context.Logger.Println("INFO Starting ADMIN service with root '" + (*root) + "'")
				http.Handle("/", http.FileServer(http.Dir(*root)))
			}
		} else if service.Name == "REST" {
			for p := range service.Parameters {
				if service.Parameters[p].Name == "port" {
					port = &service.Parameters[p].Value
					break
				}
			}
			if port == nil {
				panic("Configuration error : missing port parameter for REST service")
			}

		}
	}
	if port != nil {
		this.port = *port
		objectMap := make(map[string]interface{})
		objectMap["topic"] = &topic{context : this.context, store : this.store}
		objectMap["item"] = &_item{context : this.context, store : this.store}
		objectMap["info"] = &info{context : this.context, port : this.port}
		objectMap["instance"] = &instance{context : this.context}
		objectMap["log"] = &_log{context : this.context}
		objectMap["service"] = &service{context : this.context}
		objectMap["shutdown"] = &shutdown{context : this.context}
		go network.Listen("/API",this.port,objectMap)
		fmt.Println("Starting REST API on port " + this.port)
		/*http.HandleFunc("/instance", this.instanceListListener)
		http.HandleFunc("/instance/", this.instanceListener)
		http.HandleFunc("/topic", this.topicListListener)
		http.HandleFunc("/topic/", this.topicListener)
		http.HandleFunc("/item", this.itemListener)
		http.HandleFunc("/item/", this.itemListener)
		go http.ListenAndServe(":"+this.port, nil)*/
	}
}
func (this *HttpRestService) GetName() string {
	return "REST"
}
func (this *HttpRestService) Stop() {
}
