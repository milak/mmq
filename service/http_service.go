package service

import (
	"bytes"
	"encoding/json"
	"github.com/milak/tools/event"
	"github.com/milak/tools/data"
	"io"
	"github.com/milak/mmqapi/conf"
	"github.com/milak/mmqapi/env"
	"github.com/milak/mmq/item"
	"net/http"
	"os"
	"strings"
	"time"
)
type HttpService struct {
	context *env.Context
	port    string
	store   *item.ItemStore
}

func NewHttpService(aContext *env.Context, aStore *item.ItemStore) *HttpService {
	return &HttpService{context: aContext, store: aStore}
}
func (this *HttpService) notFound(w http.ResponseWriter) {
	w.WriteHeader(http.StatusNotFound)
	w.Write([]byte("Sorry the page you requested is not found"))
}
func (this *HttpService) methodNotSupported(w http.ResponseWriter, aMethod string) {
	w.WriteHeader(http.StatusMethodNotAllowed)
	w.Write([]byte("Sorry " + string(http.StatusMethodNotAllowed) + " error : method '" + aMethod + "' not allowed"))
}
func (this *HttpService) infoListener(w http.ResponseWriter, req *http.Request) {
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
func (this *HttpService) topicListListener(w http.ResponseWriter, req *http.Request) {
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
}
func (this *HttpService) itemListener(w http.ResponseWriter, req *http.Request) {
	this.context.Logger.Println("DEBUG entering item REST API", req)
	w.Header().Add("Access-Control-Allow-Origin", "*")
	if req.Method == http.MethodPut || req.Method == http.MethodPost {
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
	} else if req.Method == http.MethodGet {
		id := req.URL.Path
		id = id[len("/item/"):]
		this.context.Logger.Println("DEBUG getting content of item ", id)
		item := this.store.GetItem(id)
		if item == nil {
			this.notFound(w)
		} else {
			reader, err := this.store.GetContent(id, false)
			if err != nil {
				this.context.Logger.Println("ERROR i have not the content for a owned item ", id)
				w.WriteHeader(http.StatusInternalServerError)
			} else {
				this.writeItem(w, item, reader)
			}
		}
	} else {
		this.context.Logger.Println("WARNING Method not supported ", req.Method)
		this.methodNotSupported(w, req.Method)
	}
}

type _DisplayableItem struct {
	ID         string
	Age        time.Duration
	Properties []data.Property
}

func (this *HttpService) writeItem(w http.ResponseWriter, aItem *item.Item, aReader io.Reader) {
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
func (this *HttpService) topicListener(w http.ResponseWriter, req *http.Request) {
	w.Header().Add("Access-Control-Allow-Origin", "*")
	topicName := req.URL.Path
	topicName = topicName[len("/topic/"):]
	if req.Method == http.MethodGet {
		if strings.HasSuffix(topicName, "/pop") {
			topicName = topicName[0 : len(topicName)-len("/pop")]
			item, reader, err := this.store.Pop(topicName)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte(err.Error()))
			} else if item == nil {
				this.notFound(w)
			} else {
				this.writeItem(w, item, reader)
			}
		} else if strings.HasSuffix(topicName, "/list") {
			topicName = topicName[0 : len(topicName)-len("/list")]
			iterator, err := this.store.List(topicName)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte("Error during list "+ err.Error()))
				this.context.Logger.Println("ERROR items list",err)
				return
			}
			var displayableItems []DisplayableItem
			for iterator.HasNext() {
				item,_ := iterator.Next().(*item.Item)
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
			topicName = topicName[0 : len(topicName)-len("/rss")]
			iterator, err := this.store.List(topicName)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte("Error during list "+ err.Error()))
				this.context.Logger.Println("ERROR items list",err)
				return
			}
			w.WriteHeader(http.StatusOK)
			w.Header().Set("content-type", "xml/rss")
			w.Write([]byte("<?xml version=\"1.0\"?>\n<rss version=\"2.0\">\n"))
			w.Write([]byte("<channel>\n"))
			w.Write([]byte("<title>Content of the Topic " + topicName + "</title>\n"))
			w.Write([]byte("<link>" + rootUrl + "topic/" + topicName + "/rss</link>\n"))
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
				item,_ := iterator.Next().(*item.Item)
				w.Write([]byte("<item>\n"))
				w.Write([]byte("<title>Item #" + item.ID + "</title>\n"))
				w.Write([]byte("<link>" + rootUrl + "item/" + item.ID + "</link>\n"))
				description := "Item stored "
				topicList := ""
				for i,topic := range item.Topics {
					if i > 0 {
						topicList += ", "
					}
					topicList += topic
				}
				if len(item.Topics) > 1 {
					description += "in the topics : "+topicList
				} else {
					description += "in the topic : " +topicList
				}
				description += " since "+item.CreationDate.Format(env.DATE_FORMAT)
				if len(item.Properties) > 0 {
					properties := ""
					for i,property := range item.Properties {
						if i > 0 {
							topicList += ", "
						}
						properties += property.Name + " = " + property.Value
					}
					description += " with properties : "+properties
				}
				w.Write([]byte("<description>"+description+"</description>\n"))
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

		} else {
			topic := this.context.Configuration.GetTopic(topicName)
			if topic == nil {
				this.notFound(w)
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
	} else if req.Method == http.MethodDelete {
		if !this.context.Configuration.RemoveTopic(topicName) {
			this.notFound(w)
		} else {
			w.WriteHeader(http.StatusOK)
		}
	} else if req.Method == http.MethodPost {
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
				w.Write([]byte("Only "+conf.ROUND_ROBIN+" and "+conf.ORDERED+" values are accepted for topic strategy parameter"))
				return
			}
			topics := []string{}
			for _, subTopicName := range req.Form["topic"] {
				subTopic := this.context.Configuration.GetTopic(subTopicName)
				if subTopic == nil {
					w.WriteHeader(http.StatusBadRequest)
					w.Write([]byte("Sub topic not found "+subTopicName+""))
					return
				}
				topics = append(topics,subTopicName)
			}
			topic = conf.NewVirtualTopic(name[0],strategy,topics)
		} else if topicType == conf.SIMPLE {
			topic = conf.NewTopic(name[0])
		} else {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("Only "+conf.SIMPLE+" and "+conf.VIRTUAL+" values are accepted for topic type parameter"))
			return
		}
		for i, key := range req.Form["parameter-name"] {
			value := req.Form["parameter-value"][i]
			topic.AddParameter(key, value)
		}
	} else {
		this.methodNotSupported(w, req.Method)
	}
}
func (this *HttpService) instanceListListener(w http.ResponseWriter, req *http.Request) {
	w.Header().Add("Access-Control-Allow-Origin", "*")
	w.WriteHeader(http.StatusOK)
	encoder := json.NewEncoder(w)
	encoder.Encode(this.context.Configuration.Instances)
}
func (this *HttpService) instanceListener(w http.ResponseWriter, req *http.Request) {
	w.Header().Add("Access-Control-Allow-Origin", "*")
	instanceName := req.URL.Path
	instanceName = instanceName[len("/instance/"):]
	if req.Method == http.MethodDelete {
		removedInstance := this.context.Configuration.RemoveInstance(instanceName)
		if removedInstance == nil {
			this.notFound(w)
		} else {
			event.Bus.FireEvent(&conf.InstanceRemoved{removedInstance})
			w.WriteHeader(http.StatusOK)
		}
	}
}
func (this *HttpService) serviceListListener(w http.ResponseWriter, req *http.Request) {
	w.Header().Add("Access-Control-Allow-Origin", "*")
	w.WriteHeader(http.StatusOK)
	encoder := json.NewEncoder(w)
	encoder.Encode(this.context.Configuration.Services)
}
func (this *HttpService) logListener(w http.ResponseWriter, req *http.Request) {
	file, err := os.Open("mmq.log")
	if err != nil {
		this.notFound(w)
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
func (this *HttpService) shutdownListener(w http.ResponseWriter, req *http.Request) {
	w.WriteHeader(http.StatusOK)
	this.context.Running = false
	//http.DefaultServeMux.Shutdown()
}
func (this *HttpService) Start() {
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
			http.HandleFunc("/instance", this.instanceListListener)
			http.HandleFunc("/instance/", this.instanceListener)
			http.HandleFunc("/topic", this.topicListListener)
			http.HandleFunc("/topic/", this.topicListener)
			http.HandleFunc("/item", this.itemListener)
			http.HandleFunc("/item/", this.itemListener)
			http.HandleFunc("/info", this.infoListener)
			http.HandleFunc("/log", this.logListener)
			http.HandleFunc("/service", this.serviceListListener)
			http.HandleFunc("/shutdown", this.shutdownListener)
		}
	}
	if port != nil {
		this.port = *port
		go http.ListenAndServe(":"+this.port, nil)
	}
}
func (this *HttpService) Stop() {
}
