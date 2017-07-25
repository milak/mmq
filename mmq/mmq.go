package main 

import (
    "flag"
    "fmt"
    "github.com/milak/mmqapi/env"
    "github.com/milak/mmqapi/conf"
    "github.com/milak/mmq/service"
    "github.com/milak/mmq/item"
    "github.com/milak/mmq/dist"
    "github.com/milak/tools/osgi"
    "strings"
    "time"
)
var framework *osgi.Framework
// The flag package provides a default help printer via -h switch
var versionFlag *bool = flag.Bool("v", false, "Print the version number.")
var linkOption *string = flag.String("l", "", "Link to a server to get the configuration.")
var configurationFileName *string = flag.String("f", "configuration.json", "The configuration file name")
func createServices(framework *osgi.Framework ,context *env.Context, store *item.ItemStore, pool *dist.InstancePool) {
	framework.RegisterService(service.NewDistributedItemService(context,pool,store))
	framework.RegisterService(service.NewHttpRestService(context,store))
	//result = append(result,service.NewHttpService(context,store))
	framework.RegisterService(service.NewSyncService(context,pool))
	framework.RegisterService(dist.NewListener(context,pool))
	framework.RegisterService(service.NewAutoCleanService(context,store))
}
func startServices(){
	for _,service := range framework.GetServices() {
		service.Start()
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
    framework = osgi.NewFramework("plugins",context.Logger)
    framework.Start()
    
	pool 	:= dist.NewInstancePool(context)  
    store 	:= item.NewStore(context)
    
    createServices(framework,context,store,pool)
    startServices(context.Services)
    fmt.Println("MMQ started")
    for context.Running {
    	time.Sleep(1000 * time.Millisecond)
    }
    fmt.Println("MMQ stopped")
}