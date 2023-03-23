package pool

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"google.golang.org/grpc"
)

//GRPCPool pool info
type GRPCPool struct {
	Mu          sync.Mutex
	IdleTimeout time.Duration
	timeoutType TimeoutType
	conns       chan *GrpcIdleConn
	factory     func() (*grpc.ClientConn, error)
	close       func(*grpc.ClientConn) error
}

type GrpcIdleConn struct {
	Conn *grpc.ClientConn
	t    time.Time
}

//Get get from pool
func (c *GRPCPool) Get() (*GrpcIdleConn, error) {
	c.Mu.Lock()
	conns := c.conns
	c.Mu.Unlock()

	if conns == nil {
		return nil, errClosed
	}
	for {
		select {
		case wrapConn := <-conns:
			if wrapConn == nil || wrapConn.Conn == nil {
				return nil, errClosed
			}
			//判断是否超时，超时则丢弃
			if timeout := c.IdleTimeout; timeout > 0 {
				if wrapConn.t.Add(timeout).Before(time.Now()) {
					//丢弃并关闭该链接
					c.close(wrapConn.Conn)
					continue
				}
			}
			return wrapConn, nil
		default:
			conn, err := c.factory()
			if err != nil {
				return nil, err
			}
			return c.createGrpcIdleConn(conn), nil
		}
	}
}
func (c *GRPCPool) createGrpcIdleConn(conn *grpc.ClientConn) *GrpcIdleConn {
	t := time.Now()
	switch c.timeoutType {
	case IdleTimeoutType:
	case FixedTimeoutType: //create time advances random life cycle, avoid massive unusable Conn, alive: 1~1.5
		t = t.Add(-time.Millisecond * time.Duration(rand.Int63n(c.IdleTimeout.Milliseconds()/2)))
	}
	return &GrpcIdleConn{Conn: conn, t: t}
}

//Put put back to pool
func (c *GRPCPool) Put(conn *GrpcIdleConn) error {
	if conn == nil || conn.Conn == nil {
		return errRejected
	}

	c.Mu.Lock()
	defer c.Mu.Unlock()

	if c.conns == nil {
		return c.close(conn.Conn)
	}

	switch c.timeoutType {
	case IdleTimeoutType:
		conn.t = time.Now()
	case FixedTimeoutType:
	}

	select {
	case c.conns <- conn:
		return nil
	default:
		//连接池已满，直接关闭该链接
		return c.close(conn.Conn)
	}
}

//Close close pool
func (c *GRPCPool) Close() {
	c.Mu.Lock()
	conns := c.conns
	c.conns = nil
	c.factory = nil
	closeFun := c.close
	c.close = nil
	c.Mu.Unlock()

	if conns == nil {
		return
	}

	close(conns)
	for wrapConn := range conns {
		closeFun(wrapConn.Conn)
	}
}

//IdleCount idle connection count
func (c *GRPCPool) IdleCount() int {
	c.Mu.Lock()
	conns := c.conns
	c.Mu.Unlock()
	return len(conns)
}

//NewGRPCPool init grpc pool
func NewGRPCPool(o *Options, dialOptions ...grpc.DialOption) (*GRPCPool, error) {
	if err := o.validate(); err != nil {
		return nil, err
	}

	//init pool
	pool := &GRPCPool{
		conns: make(chan *GrpcIdleConn, o.MaxCap),
		factory: func() (*grpc.ClientConn, error) {
			target := o.nextTarget()
			if target == "" {
				return nil, errTargets
			}

			ctx, cancel := context.WithTimeout(context.Background(), o.DialTimeout)
			defer cancel()

			return grpc.DialContext(ctx, target, dialOptions...)
		},
		close:       func(v *grpc.ClientConn) error { return v.Close() },
		timeoutType: o.TimeoutType,
		IdleTimeout: o.IdleTimeout,
	}

	//danamic update targets
	o.update()

	//init make conns
	for i := 0; i < o.InitCap; i++ {
		conn, err := pool.factory()
		if err != nil {
			pool.Close()
			return nil, err
		}
		pool.conns <- pool.createGrpcIdleConn(conn)
	}

	return pool, nil
}
