package dist

import (
	"io"
	"net"
	"github.com/milak/mmqapi/conf"
)

type connectionFactory interface {
	Build (*conf.Instance, *net.Conn) io.Closer
}