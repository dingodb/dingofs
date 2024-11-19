package basecmd

import (
	"context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
	"math"
	"sync"
	"time"
)

type ConnectionPool struct {
	connections map[string][]*grpc.ClientConn
	mux         sync.RWMutex
}

func NewConnectionPool() *ConnectionPool {
	return &ConnectionPool{
		connections: make(map[string][]*grpc.ClientConn),
	}
}

func (c *ConnectionPool) GetConnection(address string, timeout time.Duration) (*grpc.ClientConn, error) {
	c.mux.Lock()
	conns, ok := c.connections[address]
	size := len(conns)
	if ok && size > 0 {
		log.Printf("get connection ok,address[%s],size[%d]\n", address, size)
		conn := c.connections[address][0]
		c.connections[address] = c.connections[address][1:]
		c.mux.Unlock()
		return conn, nil
	}
	c.mux.Unlock()
	log.Printf("%s: start to dial", address)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	conn, err := grpc.DialContext(ctx, address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithInitialConnWindowSize(math.MaxInt32),
		grpc.WithInitialWindowSize(math.MaxInt32))
	if err != nil {
		log.Printf("%s: fail to dial", address)
		return nil, err
	}
	return conn, nil
}

func (c *ConnectionPool) Release(address string) {
	c.mux.Lock()
	defer c.mux.Unlock()
	delete(c.connections, address)
}
func (c *ConnectionPool) PutConnection(address string, conn *grpc.ClientConn) {
	c.mux.Lock()
	defer c.mux.Unlock()
	c.connections[address] = append(c.connections[address], conn)
}
