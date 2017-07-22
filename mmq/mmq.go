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
func createServices(context *env.Context, store *item.ItemStore, pool *dist.InstancePool) {
	context.RegisterService(service.NewDistributedItemService(context,pool,store))
	context.RegisterService(service.NewHttpRestService(context,store))
	//result = append(result,service.NewHttpService(context,store))
	context.RegisterService(service.NewSyncService(context,pool))
	context.RegisterService(dist.NewListener(context,pool))
	context.RegisterService(result,service.NewAutoCleanService(context,store))
}
func startServices(services []env.Service){
	for _,service := range services {
		service.Start()
	}
}
func loadPlugins(context *env.Context){
	// Browse plugin directory
	pluginDirectory,err := os.Open("plugins")
	if err != nil {
		fmt.Println("No plugin directory")
		// no plugins directory
		return
	}
	fmt.Println("Loading plugins...")
	info, err := pluginDirectory.Stat()
	if !info.IsDir() {
		fmt.Println("Plugins directory is not a directory")
		return
	}
	files,err := pluginDirectory.Readdir(0)
	if err != nil {
		fmt.Println("Unable to browse plugins directory",err)
		return
	}
	for _,file := range files {
		fmt.Println("Loading plugin",file.Name(),"...")
		thePlugin, err := plugin.Open("plugins/"+file.Name())
		if err != nil {
			fmt.Println("Unable to load plugin",file.Name(),":",err)
		}
		function,err := thePlugin.Lookup("Init")
		if err != nil {
			function.(func(*env.Context))(context)
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
    loadPlugins(&context)
    
	pool 	:= dist.NewInstancePool(context)  
    store 	:= item.NewStore(context)
    
    createServices(context,store,pool)
    startServices(context.Services)
    fmt.Println("MMQ started")
    for context.Running {
    	time.Sleep(1000 * time.Millisecond)
    }
    fmt.Println("MMQ stopped")
}