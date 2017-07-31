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
    osgiservice "github.com/milak/tools/osgi/service"
    "github.com/milak/tools/network"
    "os"
    "strings"
    "time"
)
var framework *osgi.Framework
// The flag package provides a default help printer via -h switch
var versionFlag *bool = flag.Bool("v", false, "Print the version number.")
var linkOption *string = flag.String("l", "", "Link to a server to get the configuration.")
var configurationFileName *string = flag.String("f", "configuration.json", "The configuration file name")
func createServices(framework *osgi.Framework ,context *env.Context, store *item.ItemStore, pool *dist.InstancePool) {
	bundleContext := framework.GetBundleContext()
	
	s := service.NewDistributedItemService(pool,store)
	s.Start(bundleContext)
	s2 := service.NewHttpRestService(context,store)
	s2.Start(bundleContext)
	//result = append(result,service.NewHttpService(context,store))
	s3 := service.NewSyncService(pool)
	s3.Start(bundleContext)
	s4 := dist.NewListener(bundleContext,pool)
	s4.Start(bundleContext)
	s5 := service.NewAutoCleanService(store)
	s5.Start(bundleContext)
}
func startServices(){
	/*for _,service := range framework.GetServices() {
		service.Start()
	}*/
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
    framework = osgi.NewFramework("plugins")
    var logService osgiservice.LogService
    file, err := os.Create("mmq.log")
	if err != nil {
		logService = service.NewDefaultLogService()
		logger.Println("WARNING Unable to open file mmq.log")
	} else {
		logService = service.NewLogService(file, "",  log.Ldate | log.Ltime | log.Lshortfile)
	}
    framework.RegisterService("LogService",&logService)
    framework.SetProperty("configuration",configuration)
    host,_ := network.GetLocalIP()
    framework.SetProperty("Host",host)
    framework.SetProperty("InstanceName",context.InstanceName)
    framework.Start()
    
	pool 	:= dist.NewInstancePool(framework.GetBundleContext())  
    store 	:= item.NewStore(context)
    
    framework.RegisterService("StoreService",store)
    
    createServices(framework,context,store,pool)
    fmt.Println("MMQ started")
    for context.Running {
    	time.Sleep(1000 * time.Millisecond)
    }
    fmt.Println("MMQ stopped")
    framework.Stop()
}