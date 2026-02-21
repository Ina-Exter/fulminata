package main

import (
	"sync"
)

type portsPool struct {
	sync.Mutex
	sem           chan struct{}
	openPorts     map[int]struct{}
	existingPorts map[int]struct{}
}

func newPortsPool(ports []int) *portsPool {
	openPorts := make(map[int]struct{}, len(ports))
	existingPorts := make(map[int]struct{}, len(ports))

	for _, p := range ports {
		openPorts[p] = struct{}{}
		existingPorts[p] = struct{}{}
	}

	return &portsPool{
		openPorts:     openPorts,
		sem:           make(chan struct{}, len(ports)),
		existingPorts: existingPorts,
	}
}

func (pm *portsPool) Take() int {
	pm.sem <- struct{}{}

	pm.Lock()

	defer pm.Unlock()

	for p := range pm.openPorts {
		delete(pm.openPorts, p)

		return p
	}

	return 0
}

func (pm *portsPool) Give(port int) {
	// do not give a new port
	if _, ok := pm.existingPorts[port]; !ok {
		return
	}

	pm.Lock()

	defer pm.Unlock()

	if _, ok := pm.openPorts[port]; ok {
		return
	}

	pm.openPorts[port] = struct{}{}

	<-pm.sem
}

func (pm *portsPool) Free() int {
	pm.Lock()

	defer pm.Unlock()

	return len(pm.openPorts)
}

func (pm *portsPool) Total() int {
	return len(pm.existingPorts)
}
