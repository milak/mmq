package env

import (
	"log"
	"github.com/milak/mmq/conf"
	"github.com/milak/tools/network"
	"os"
)
const DATE_FORMAT string = "Mon, 02 Jan 2006 15:04:05 MST"
type Context struct {
	Running			bool
	Configuration 	*conf.Configuration
	Logger			*log.Logger
	Host			string
	InstanceName	string
}
func NewContext(conf *conf.Configuration) *Context {
	if conf == nil {
		panic("Configuration cannot be nil")
	}
	var logger *log.Logger
	file, err := os.Create("mmq.log")
	if err != nil {
		logger = log.New(os.Stdout, "",  log.Ldate | log.Ltime | log.Lshortfile)
		logger.Println("WARNING Unable to open file mmq.log")
	} else {
		logger = log.New(file, "",  log.Ldate | log.Ltime | log.Lshortfile)
	}
	host,_ := network.GetLocalIP()
	InstanceName := host
	for _,service := range conf.Services {
		if !service.Active {
			continue
		}
		if service.Name == "SYNC" {
			for _,p := range service.Parameters {
				if p.Name == "port" {
					InstanceName = host+":"+p.Value
					break;
				}
			}
			break
		}
	}
	return &Context{Running : true, Configuration : conf, Logger : logger, Host : host, InstanceName : InstanceName}
}