package dist

import (
	"io"
	"net"
	"mmq/conf"
)

type connectionFactory interface {
	Build (*conf.Instance, *net.Conn) io.Closer
}