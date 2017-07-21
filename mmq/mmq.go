package main 

import (
    "flag"
    "fmt"
    "os"
    "github.com/milak/mmq/env"
    "github.com/milak/mmq/conf"
    "github.com/milak/mmq/service"
    "github.com/milak/mmq/item"
    "github.com/milak/mmq/dist"
    "strings"
    "time"
    "plugin"
)

// The flag package provides a default help printer via -h switch
var versionFlag *bool = flag.Bool("v", false, "Print the version number.")
var linkOption *string = flag.String("l", "", "Link to a server to get the configuration.")
var configurationFileName *string = flag.String("f", "configuration.json", "The configuration file name")
func createServices(context *env.Context, store *item.ItemStore, pool *dist.InstancePool) []service.Service {
	result := []service.Service{}
	result = append(result,service.NewDistributedItemService(context,pool,store))
	result = append(result,service.NewHttpRestService(context,store))
	//result = append(result,service.NewHttpService(context,store))
	result = append(result,service.NewSyncService(context,pool))
	result = append(result,dist.NewListener(context,pool))
	result = append(result,service.NewAutoCleanService(context,store))
	return result
}
func startServices(services []service.Service){
	for _,service := range services {
		service.Start()
	}
}
func loadPlugins(){
	// Browse plugin directory
	pluginDirectory,err := os.Open("plugins")
	if err != nil {
		// no plugins directory
		return
	}
	info, err := pluginDirectory.Stat()
	if !info.IsDir() {
		// plugins is not a directory 
		return
	}
	files,err := pluginDirectory.Readdir(0)
	if err != nil {
		fmt.Println("Unable to browse plugins directory")
		return
	}
	for _,file := range files {
		p, err := plugin.Open(file.Name())
		if err != nil {
			fmt.Println("Unable to load plugin",file.Name(),":",err)
		}
	}
}
func main() {
	
	flag.Parse() // Scan the arguments list
	linkOptionAsString := (*linkOption)
	configuration,_ := conf.InitConfiguration(*configurationFileName)
    if linkOptionAsString != "" {
    	fmt.Println("Linking to master ",linkOptionAsString)
    	var host, port string
    	deuxPoints := strings.Index(linkOptionAsString, ":")
    	if deuxPoints != -1 {
    		host = linkOptionAsString[0:deuxPoints]
    		port = linkOptionAsString[deuxPoints+1:]
    	} else {
    		host = linkOptionAsString
    		port = conf.DEFAULT_SYNC_PORT
    	}
    	instance := conf.NewInstance(host,port)
    	configuration.AddInstance(instance)
    	service := configuration.GetServiceByName(conf.SERVICE_SYNC)
    	if service == nil {
	    	params := []conf.Parameter{*conf.NewParameter(conf.PARAMETER_PORT,conf.DEFAULT_SYNC_PORT)}
	    	service := conf.NewService(conf.SERVICE_SYNC,true,params)
	    	configuration.AddService(service)
    	} else {
    		service.Active = true
    	}
    }
    context := env.NewContext(configuration)
    fmt.Println("Starting MMQ on "+context.Host+"...")
    if *versionFlag {
        fmt.Println("Version:"/**, configuration.Version*/)
    }
    
	pool 	:= dist.NewInstancePool(context)  
    store 	:= item.NewStore(context)
    
    services := createServices(context,store,pool)
    startServices(services)
    fmt.Println("MMQ started")
    for context.Running {
    	time.Sleep(1000 * time.Millisecond)
    }
    fmt.Println("MMQ stopped")
}