package dist
/**
 * This Service listens on SYNC PORT. Each call established emplies a call to Protocol class to process the exchange.
 */
import (
	"github.com/milak/tools/osgi"
	"github.com/milak/tools/osgi/service"
	"github.com/milak/mmqapi/conf"
	"log"
	"net"
)
type Listener struct {
	listener 	net.Listener					// a reference to the listener when doListen has been called
	context		osgi.BundleContext
	pool		*InstancePool
	logger		*log.Logger
	port		string
	running		bool
	protocol	*protocol
}
func NewListener(aContext osgi.BundleContext, aPool *InstancePool) *Listener {
	logServiceRef := aContext.GetService("LogService")
	logger := logServiceRef.Get().(*service.LogService).GetLogger()
	return &Listener{context : aContext, pool : aPool, running : true, protocol : NewProtocol(aContext,aPool), logger : logger}
}
func (this *Listener) Start(aBundleContext osgi.BundleContext){
	configuration := this.context.GetProperty("Configuration").(*conf.Configuration)
	for s := range configuration.Services {
		service := configuration.Services[s]
		if !service.Active {
			continue
		}
		if service.Name == "SYNC" {
			found := false
			this.logger.Println("INFO starting...")
			for p := range service.Parameters {
				if service.Parameters[p].Name == "port" {
					this.port = service.Parameters[p].Value
					this.running = true
					found = true
					go this.doListen(this.port)
					break
				}
			}
			if !found {
				this.logger.Panic("missing port parameter")
			}
		}
	}
}
func (this *Listener) GetVersion() string {
	return "1.0.0"
}
func (this *Listener) GetSymbolicName() string {
	return "listener"
}
/**
 * Listen remote Instances call
 * @param aPort : the listening port
 */
func (this *Listener) doListen (aPort string) {
	this.logger.Println("DEBUG listening on port",aPort,"...")
	var err error
	this.listener, err = net.Listen("tcp", ":"+aPort)
	if err != nil {
		this.logger.Println("DEBUG listening failed",err)
	} else {
		for this.running {
			conn, err := this.listener.Accept()
			if err != nil {
				this.logger.Println("WARNING Failed to listen",err)
			} else {
				this.logger.Println("INFO caught a call")
				/*instance,err := */this.protocol.handleConnection(conn)
				/*if err == nil {
					this.pool.newInstanceConnection(instance,&conn)
				}*/
			}
		}
	}
}
func (this *Listener) Stop() {
	this.running = false
	if this.listener != nil {
		this.listener.Close()
	}
}