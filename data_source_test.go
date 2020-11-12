package main

import (
	"testing"
	"time"
)

func TestCachedDataSource_CanReuseAndExpireGetNodeInfo(t *testing.T) {
	fds := new(fakeDataSource)
	ds := NewCachedDataSource(fds)
	cds := ds.(*cachedDataSource)
	cds.cacheTTL = time.Millisecond * 10

	if cds.cachedGetNodeInfo != nil || time.Now().Before(cds.cachedGetNodeInfoExpiresAt) {
		t.Fatal("No cache should be set after initialize")
	}

	// Read from data source
	cds.GetNodeInfo()
	if cds.cachedGetNodeInfo == nil {
		t.Fatal("Cache was not set after calling GetNodeInfo")
	}
	if cds.cachedGetNodeInfoExpiresAt.Before(time.Now()) {
		t.Fatal("Cache expiration time was not set after calling GetNodeInfo")
	}

	// Read from cache (update value before to verify the read is cached)
	fds.byteLag = 1337
	nodeInfo, _ := cds.GetNodeInfo()
	if nodeInfo != cds.cachedGetNodeInfo && nodeInfo.ByteLag != 0 {
		t.Fatal("Did not use the cached GetNodeInfo value when cached read was expected")
	}

	// Wait for cache to expire
	time.Sleep(cds.cacheTTL)
	if !cds.cachedGetNodeInfoExpiresAt.Before(time.Now()) {
		t.Fatal("Need to wait longer before cache will expire")
	}

	// Read from data source
	nodeInfo, _ = cds.GetNodeInfo()
	if cds.cachedGetNodeInfo.ByteLag != 1337 {
		t.Fatal("Cache was not successfully expired")
	}
}

func TestCachedDataSource_CanReuseAndExpireIsInRecovery(t *testing.T) {
	fds := new(fakeDataSource)
	ds := NewCachedDataSource(fds)
	cds := ds.(*cachedDataSource)
	cds.cacheTTL = time.Millisecond * 10

	if cds.cachedIsInRecovery == true || time.Now().Before(cds.cachedIsInRecoveryExpiresAt) {
		t.Fatal("No cache should be set after initialize")
	}

	// Read from data source
	cds.IsInRecovery()
	if cds.cachedIsInRecoveryExpiresAt.Before(time.Now()) {
		t.Fatal("Cache expiration time was not set after calling IsInRecovery")
	}

	// Read from cache (update value before to verify the read is cached)
	isInRecovery, _ := cds.IsInRecovery()
	if isInRecovery != cds.cachedIsInRecovery {
		t.Fatal("Did not use the cached IsInRecovery value when cached read was expected")
	}

	// Wait for cache to expire
	time.Sleep(cds.cacheTTL)
	if !cds.cachedIsInRecoveryExpiresAt.Before(time.Now()) {
		t.Fatal("Need to wait longer before cache will expire")
	}

	// Read from data source
	isInRecovery, _ = cds.IsInRecovery()
	if cds.cachedIsInRecoveryExpiresAt.Before(time.Now()) {
		t.Fatal("Cache expiration time was not set after calling IsInRecovery")
	}
}
