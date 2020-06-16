package main

import (
	"go.etcd.io/etcd/clientv3"
	"time"
	"context"
	//"github.com/pkg/errors"
	"errors"
)

type DistributedLock struct {
	Config	clientv3.Config
	TTL		int64
	LeaseID clientv3.LeaseID
	Lease	clientv3.Lease
	Txn 	clientv3.Txn
	Key		string
	client  *clientv3.Client
	cancelKeepAlive context.CancelFunc
}

func (d *DistributedLock) Lock() (error) {
	d.Txn.If(clientv3.Compare(clientv3.CreateRevision(d.Key), "=", 0)).
		Then(clientv3.OpPut(d.Key, "", clientv3.WithLease(d.LeaseID))).
		Else()
	txnResp, err := d.Txn.Commit()
	if err != nil {
		return err
	}
	if !txnResp.Succeeded {
		return errors.New("lock fail")
	}
	return nil
}

func (d *DistributedLock) Unlock() (error) {
	d.cancelKeepAlive()
	_, err := d.Lease.Revoke(context.TODO(), d.LeaseID)
	return err
}

func NewDistributedLock(endPoints []string, dialTimeout float32, key string, ttl int64) (lock DistributedLock, err error) {
	lock = DistributedLock{
		Config: clientv3.Config{
			Endpoints: endPoints,
			DialTimeout:	time.Duration(dialTimeout)*time.Second,
		},
		Key:	key,
		TTL:	ttl,
	}

	lock.client, err = clientv3.New(lock.Config)
	if err != nil {
		return
	}

	// make lease
	ctx := context.TODO()
	lock.Lease = clientv3.NewLease(lock.client)
	leaseResp, err := lock.Lease.Grant(ctx, lock.TTL)
	if err != nil {
		return
	}
	lock.LeaseID = leaseResp.ID

	// make keepalive
	keepAliveCtx, cancelFunc := context.WithCancel(ctx)
	lock.cancelKeepAlive = cancelFunc
	_, err = lock.Lease.KeepAlive(keepAliveCtx, lock.LeaseID)
	if err != nil {
		return
	}

	// make txn
	lock.Txn = clientv3.NewKV(lock.client).Txn(ctx)
	return
}