// MIT License
//
// Copyright (c) Microsoft Corporation. All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE

package controller

import (
	"fmt"
	ci "github.com/microsoft/frameworkcontroller/pkg/apis/frameworkcontroller/v1"
	//frameworkClient "github.com/microsoft/frameworkcontroller/pkg/client/clientset/versioned"
	frameworkInformer "github.com/microsoft/frameworkcontroller/pkg/client/informers/externalversions"
	frameworkLister "github.com/microsoft/frameworkcontroller/pkg/client/listers/frameworkcontroller/v1"
	"github.com/microsoft/frameworkcontroller/pkg/common"
	"github.com/microsoft/frameworkcontroller/pkg/util"
	//errorWrap "github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	core "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	//meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	//"k8s.io/apimachinery/pkg/types"
	//errorAgg "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	//kubeInformer "k8s.io/client-go/informers"
	//kubeClient "k8s.io/client-go/kubernetes"
	coreLister "k8s.io/client-go/listers/core/v1"
	//"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	//"k8s.io/client-go/util/retry"
	"k8s.io/client-go/util/workqueue"
	//"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type SkdResources struct {
	CPU    int64
	Memory int64
	GPU    int64
}

type SkdResourceRequirements struct {
	Limits   SkdResources
	Requests SkdResources
}

type SkdNode struct {
	Key              string
	ScheduleCategory string
	ScheduleZone     string
	Zone             *SkdZone
	HostIP           string
	Capacity         SkdResources
	Allocatable      SkdResources
	Allocated        SkdResources
	Free             SkdResources
	lockOnPods       *sync.RWMutex
	Pods             map[string]*SkdPod
	LastInformed     time.Time
}

type SkdPod struct {
	Key          string
	FrameworkKey string
	HostIP       string
	Node         *SkdNode
	Phase        core.PodPhase
	Condition    core.PodConditionType
	Resources    SkdResourceRequirements
	LastInformed time.Time
}

type SkdFramework struct {
	Key               string
	Name              string
	Namespace         string
	ScheduleCategory  string
	ScheduleZone      string
	ScheduleRemoteble string
	QueuingTimestamp  time.Time
	Zone              *SkdZone
	Resources         SkdResourceRequirements
	LastSync          time.Time
	Remote            struct {
		Host    string
		State   ci.RemoteState
		Request string
	}
}

type SkdZone struct {
	Key              string
	ScheduleCategory string
	ScheduleZone     string
	TotalCapacity    SkdResources
	TotalAllocated   SkdResources
	TotalFree        SkdResources
	TotalProvision   SkdResources
	lockOnNodes      *sync.RWMutex
	lockOnWaiting    *sync.RWMutex
	Nodes            map[string]*SkdNode
	fmWaiting        map[string]*SkdFramework
	LastInformed     time.Time
	Alive            bool
	lockOnResources  *sync.Mutex
	lockOnProvision  *sync.Mutex
}

type FrameworkScheduler struct {
	fmQueuing  map[string]*SkdFramework
	fmWaiting  map[string]*SkdFramework
	fmPending  map[string]*SkdFramework
	fmRemoting map[string]*SkdFramework

	podLister    coreLister.PodLister
	fLister      frameworkLister.FrameworkLister
	nodeInformer cache.SharedIndexInformer
	nodeLister   coreLister.NodeLister
	fQueue       workqueue.RateLimitingInterface

	lockOnQueuing  *sync.RWMutex
	lockOnWaiting  *sync.RWMutex
	lockOnPending  *sync.RWMutex
	lockOnRemoting *sync.RWMutex

	lockOnHostIP *sync.RWMutex
	lockOnNodes  *sync.RWMutex
	lockOnPods   *sync.RWMutex
	lockOnZones  *sync.RWMutex

	hostIP2node map[string]string
	nodes       map[string]string
	pods        map[string]string
	zones       map[string]*SkdZone
	zoneList    []*SkdZone

	lastModifiedZone      time.Time
	lastRefreshedZoneList time.Time
	oldestSyncTime        time.Time
}

type InterdomScheduler struct {
	mapfcs       map[string]*FrameworkController
	myfc         *FrameworkController
	fRemoteQueue workqueue.RateLimitingInterface
}

func NewInterdomScheduler(myfc *FrameworkController) *InterdomScheduler {
	fRemoteQueue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	is := &InterdomScheduler{
		mapfcs:       make(map[string]*FrameworkController),
		myfc:         myfc,
		fRemoteQueue: fRemoteQueue,
	}
	cConfigs := ci.NewRemoteConfigs()
	for _, cConfig := range cConfigs {
		kConfig := ci.BuildKubeConfig(cConfig)
		host := kConfig.Host
		if _, ok := is.mapfcs[host]; ok {
			common.LogLines("Duplicated host: %v", host)
			continue
		}
		kClient, fClient := util.CreateClients(kConfig)
		fListerInformer := frameworkInformer.NewSharedInformerFactory(fClient,
			0).Frameworkcontroller().V1().Frameworks()
		fInformer := fListerInformer.Informer()
		fLister := fListerInformer.Lister()
		fc := &FrameworkController{
			kConfig:   kConfig,
			cConfig:   cConfig,
			kClient:   kClient,
			fClient:   fClient,
			fInformer: fInformer,
			fLister:   fLister,
		}
		fInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				is.onRemoveFrameworkAdd(obj, fc)
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				is.onRemoveFrameworkUpdate(newObj, fc)
			},
			DeleteFunc: func(obj interface{}) {
				is.onRemoveFrameworkDelete(obj, fc)
			},
		})
		is.mapfcs[host] = fc
	}
	return is
}

func GetFrameworkKey(obj interface{}) string {
	key, err := util.GetKey(obj)
	if err != nil {
		log.Errorf("Failed to get key for obj %#v, skip to enqueue: %v", obj, err)
		return ""
	}

	_, _, err = util.SplitKey(key)
	if err != nil {
		log.Errorf("Got invalid key %v for obj %#v, skip to enqueue: %v", key, obj, err)
		return ""
	}

	return key
}

func (is *InterdomScheduler) onRemoveFrameworkAdd(obj interface{}, fc *FrameworkController) {
	key := GetFrameworkKey(obj)
	log.Infof("[%v] onRemoveFrameworkAdd: %v", key, fc.kConfig.Host)
}

func (is *InterdomScheduler) onRemoveFrameworkUpdate(obj interface{}, fc *FrameworkController) {
	key := GetFrameworkKey(obj)
	log.Infof("[%v] onRemoveFrameworkUpdate: %v", key, fc.kConfig.Host)
}

func (is *InterdomScheduler) onRemoveFrameworkDelete(obj interface{}, fc *FrameworkController) {
	key := GetFrameworkKey(obj)
	log.Infof("[%v] onRemoveFrameworkDelete: %v", key, fc.kConfig.Host)
}

func (is *InterdomScheduler) worker(fc *FrameworkController, id int32) {
}

func (is *InterdomScheduler) Run(stopCh <-chan struct{}) {
	defer is.fRemoteQueue.ShutDown()
	defer log.Errorf("InterdomScheduler Stopping " + ci.ComponentName)
	defer runtime.HandleCrash()

	for _, fc := range is.mapfcs {
		log.Infof("InterdomScheduler Recovering " + fc.kConfig.Host)
		util.PutCRD(
			fc.kConfig,
			ci.BuildFrameworkCRD(),
			fc.cConfig.CRDEstablishedCheckIntervalSec,
			fc.cConfig.CRDEstablishedCheckTimeoutSec)

		go fc.fInformer.Run(stopCh)
		if !cache.WaitForCacheSync(
			stopCh,
			fc.fInformer.HasSynced) {
			log.Errorf("Failed to WaitForCacheSync for %v", fc.kConfig.Host)
		}

		log.Infof("Running %v with %v workers for host %v",
			ci.ComponentName, *fc.cConfig.WorkerNumber, fc.kConfig.Host)

		for i := int32(0); i < *fc.cConfig.WorkerNumber; i++ {
			// id is dedicated for each iteration, while i is not.
			id := i
			go wait.Until(func() { is.worker(fc, id) }, time.Second, stopCh)
		}
	}

	<-stopCh
}

// obj could be *core.Pod or cache.DeletedFinalStateUnknown.
func (s *FrameworkScheduler) enqueuePodObj(obj interface{}, msg string) {
	key, err := util.GetKey(obj)
	if err != nil {
		log.Errorf("Failed to get key for pod %#v, skip to enqueue: %v", obj, err)
		return
	}
	s.enqueuePodKey(key)
	log.Infof("[%v]: enqueuePodObj: %v", key, msg)
}

// obj could be *core.Node or cache.DeletedFinalStateUnknown.
func (s *FrameworkScheduler) enqueueNodeObj(obj interface{}, msg string) {
	key, err := util.GetKey(obj)
	if err != nil {
		log.Errorf("Failed to get key for node %#v, skip to enqueue: %v", obj, err)
		return
	}
	s.enqueueNodeKey(key)
	log.Infof("[%v]: enqueueNodeObj: %v", key, msg)
}

func (s *FrameworkScheduler) enqueuePodKey(key string) {
	s.enqueueKey(ci.QueueKeyPrefixPod, key)
	log.Infof("[%v]: enqueuePodKey.", key)
}

func (s *FrameworkScheduler) enqueueNodeKey(key string) {
	s.enqueueKey(ci.QueueKeyPrefixNode, key)
	log.Infof("[%v]: enqueueNodeKey.", key)
}

func (s *FrameworkScheduler) enqueueZoneKey(key string) {
	s.enqueueKey(ci.QueueKeyPrefixZone, key)
	log.Infof("[%v]: enqueueZoneKey.", key)
}

func (s *FrameworkScheduler) enqueueKey(prefix string, key string) {
	s.fQueue.AddRateLimited(fmt.Sprintf("%v%v", prefix, key))
}

func (s *FrameworkScheduler) enqueueFrameworkKey(key string) {
	s.fQueue.AddRateLimited(key)
}

func (s *FrameworkScheduler) enqueueKeyAfter(key string, after int64) {
	duration := time.Duration(int64(time.Second) * after)
	s.fQueue.AddAfter(key, duration)
}

func HasPrefixPod(key interface{}) bool {
	return strings.HasPrefix(key.(string), ci.QueueKeyPrefixPod)
}

func HasPrefixNode(key interface{}) bool {
	return strings.HasPrefix(key.(string), ci.QueueKeyPrefixNode)
}

func HasPrefixZone(key interface{}) bool {
	return strings.HasPrefix(key.(string), ci.QueueKeyPrefixZone)
}

func GetPodKey(key interface{}) string {
	return key.(string)[len(ci.QueueKeyPrefixPod):]
}

func GetNodeKey(key interface{}) string {
	return key.(string)[len(ci.QueueKeyPrefixNode):]
}

func GetZoneKey(key interface{}) string {
	return key.(string)[len(ci.QueueKeyPrefixZone):]
}

func (s *FrameworkScheduler) lookupNodeKeyByPod(key string) (string, bool) {
	ReadLock(s.lockOnPods)
	defer ReadUnlock(s.lockOnPods)
	nodeKey, ok := s.pods[key]
	if ok {
		return nodeKey, true
	} else {
		return "", false
	}
}

func (s *FrameworkScheduler) lookupZoneKeyByNode(key string) (string, bool) {
	ReadLock(s.lockOnNodes)
	defer ReadUnlock(s.lockOnNodes)
	zoneKey, ok := s.nodes[key]
	if ok {
		return zoneKey, true
	} else {
		return "", false
	}
}

func (s *FrameworkScheduler) cleanupPodKey(key string) {
	WriteLock(s.lockOnPods)
	defer WriteUnlock(s.lockOnPods)
	delete(s.pods, key)
}

func (s *FrameworkScheduler) cleanupNodeKey(key string) {
	WriteLock(s.lockOnNodes)
	defer WriteUnlock(s.lockOnNodes)
	delete(s.nodes, key)
}

func (s *FrameworkScheduler) lookupPodByKey(key string) *SkdPod {
	nodeKey, ok := s.lookupNodeKeyByPod(key)
	if !ok {
		return nil
	}
	skdNode := s.lookupNodeByKey(nodeKey)
	if skdNode == nil {
		return nil
	}
	ReadLock(skdNode.lockOnPods)
	defer ReadUnlock(skdNode.lockOnPods)
	skdPod, exists := skdNode.Pods[key]
	if exists {
		return skdPod
	}
	return nil
}

func (s *FrameworkScheduler) lookupNodeByKey(key string) *SkdNode {
	zoneKey, ok := s.lookupZoneKeyByNode(key)
	if !ok {
		return nil
	}
	skdZone := s.lookupZoneByKey(zoneKey)
	if skdZone == nil {
		return nil
	}
	ReadLock(skdZone.lockOnNodes)
	defer ReadUnlock(skdZone.lockOnNodes)
	skdNode, exists := skdZone.Nodes[key]
	if exists {
		return skdNode
	}
	return nil
}

func (s *FrameworkScheduler) lookupZoneByKey(key string) *SkdZone {
	ReadLock(s.lockOnZones)
	defer ReadUnlock(s.lockOnZones)
	skdZone, exists := s.zones[key]
	if exists {
		return skdZone
	}
	return nil
}

func (s *FrameworkScheduler) addPodToNode(skdPod *SkdPod) error {
	nodeKey, ok := s.lookupNodeKeyByHostIP(skdPod.HostIP)
	if !ok {
		return fmt.Errorf("[%v] addPodToNode: Node with HostIP(=%v) is NOT found!",
			skdPod.Key, skdPod.HostIP)
	}
	skdNode := s.lookupNodeByKey(nodeKey)
	if skdNode == nil {
		return fmt.Errorf("[%v] addPodToNode: Node(=%v) does not exist!",
			skdPod.Key, nodeKey)
	}
	// Add skdPod to skdNode
	WriteLock(skdNode.lockOnPods)
	func() {
		defer WriteUnlock(skdNode.lockOnPods)
		skdNode.Pods[skdPod.Key] = skdPod
		skdPod.Node = skdNode
	}()
	// Add the map of {skdPod.Key -> skdNode.Key}
	WriteLock(s.lockOnPods)
	func() {
		defer WriteUnlock(s.lockOnPods)
		s.pods[skdPod.Key] = nodeKey
	}()
	// pods in node changed, notify the node
	s.enqueueNodeKey(nodeKey)
	return nil
}

func (s *FrameworkScheduler) addNodeToZone(skdNode *SkdNode) {
	zoneKey := ToZoneKey(skdNode.ScheduleCategory, skdNode.ScheduleZone)
	skdZone := s.lookupZoneByKey(zoneKey)
	if skdZone == nil {
		skdZone = s.CreateSkdZone(skdNode.ScheduleCategory, skdNode.ScheduleZone)
	}
	// Add skdNode to skdZone
	WriteLock(skdZone.lockOnNodes)
	func() {
		defer WriteUnlock(skdZone.lockOnNodes)
		skdZone.Nodes[skdNode.Key] = skdNode
		skdNode.Zone = skdZone
	}()
	// Add the map of {skdNode.Key -> skdZone.Key}
	WriteLock(s.lockOnNodes)
	func() {
		defer WriteUnlock(s.lockOnNodes)
		s.nodes[skdNode.Key] = zoneKey
	}()
	// nodes in zone changed, notify the zone
	s.enqueueZoneKey(zoneKey)
}

func (s *FrameworkScheduler) CreateSkdZone(category string, zone string) *SkdZone {
	zoneKey := ToZoneKey(category, zone)
	skdZone := &SkdZone{
		Key:              zoneKey,
		ScheduleCategory: category,
		ScheduleZone:     zone,
		TotalCapacity:    SkdResources{0, 0, 0},
		TotalAllocated:   SkdResources{0, 0, 0},
		TotalFree:        SkdResources{0, 0, 0},
		TotalProvision:   SkdResources{0, 0, 0},
		lockOnNodes:      new(sync.RWMutex),
		lockOnWaiting:    new(sync.RWMutex),
		Nodes:            make(map[string]*SkdNode),
		fmWaiting:        make(map[string]*SkdFramework),
		LastInformed:     time.Now(),
		Alive:            false,
		lockOnResources:  new(sync.Mutex),
		lockOnProvision:  new(sync.Mutex),
	}
	WriteLock(s.lockOnZones)
	defer WriteUnlock(s.lockOnZones)
	s.zones[zoneKey] = skdZone
	s.lastModifiedZone = time.Now()
	return skdZone
}

func (s *FrameworkScheduler) deletePod(skdPod *SkdPod) {
	skdNode := skdPod.Node
	if skdNode != nil {
		// Delete pod from node
		WriteLock(skdNode.lockOnPods)
		func() {
			defer WriteUnlock(skdNode.lockOnPods)
			skdPod.Node = nil
			delete(skdNode.Pods, skdPod.Key)
		}()
		// Pods in node are changed, notify the node
		s.enqueueNodeKey(skdNode.Key)
	}
	s.cleanupPodKey(skdPod.Key)
}

func (s *FrameworkScheduler) deleteNode(skdNode *SkdNode) {
	skdZone := skdNode.Zone
	if skdZone != nil {
		// Delete node from zone
		WriteLock(skdZone.lockOnNodes)
		func() {
			defer WriteUnlock(skdZone.lockOnNodes)
			skdNode.Zone = nil
			delete(skdZone.Nodes, skdNode.Key)
		}()
		// Nodes in zone are changed, notify the zone
		s.enqueueZoneKey(skdZone.Key)
	}
	s.cleanupNodeKey(skdNode.Key)
}

func (s *FrameworkScheduler) deleteZone(skdZone *SkdZone) {
	WriteLock(s.lockOnZones)
	defer WriteUnlock(s.lockOnZones)
	delete(s.zones, skdZone.Key)
	s.lastModifiedZone = time.Now()
}

func (s *FrameworkScheduler) deletePodByKey(key string) {
	skdPod := s.lookupPodByKey(key)
	if skdPod != nil {
		s.deletePod(skdPod)
	} else {
		s.cleanupPodKey(key)
	}
}

func (s *FrameworkScheduler) deleteNodeByKey(key string) {
	skdNode := s.lookupNodeByKey(key)
	if skdNode != nil {
		s.deleteNode(skdNode)
	} else {
		s.cleanupNodeKey(key)
	}
}

func (s *FrameworkScheduler) syncPod(key string) (returnedErr error) {
	startTime := time.Now()
	logPfx := fmt.Sprintf("[%v]: syncPod: ", key)
	log.Infof(logPfx + "Started")
	defer func() {
		if returnedErr != nil {
			// returnedErr is already prefixed with logPfx
			log.Warnf(returnedErr.Error())
			log.Warnf(logPfx +
				"Failed to due to Platform Transient Error. " +
				"Will enqueue it again after rate limited delay")
		}
		log.Infof(logPfx+"Completed: Duration %v", time.Since(startTime))
	}()

	namespace, name, err := util.SplitKey(key)
	if err != nil {
		// Unreachable
		panic(fmt.Errorf(logPfx+
			"Failed: Got invalid key from queue, but the queue should only contain "+
			"valid keys: %v", err))
	}

	pod, err := s.podLister.Pods(namespace).Get(name)
	if err != nil {
		if apiErrors.IsNotFound(err) {
			// GarbageCollectionController will handle the dependent object
			// deletion according to the ownerReferences.
			log.Infof(logPfx+"Skipped: Pod is not in local cache: %v", err)
			s.deletePodByKey(key)
			return nil
		} else {
			return fmt.Errorf(logPfx+"Failed: Pod is not in local cache: %v", err)
		}
	}
	if pod.DeletionTimestamp != nil {
		// Skip syncPod to avoid fighting with GarbageCollectionController,
		// because GarbageCollectionController may be deleting the dependent object.
		log.Infof(logPfx+"Skipped: Pod on node %v is to be deleted", key)
		s.deletePodByKey(key)
		return nil
	}
	skdPod := s.lookupPodByKey(key)
	if skdPod != nil {
		skdPod.LastInformed = time.Now()
		if skdPod.Phase != pod.Status.Phase {
			log.Infof(logPfx+"Skipped: Phase changes from %v to %v",
				skdPod.Phase, pod.Status.Phase)
		}
		skdPod.Phase = pod.Status.Phase
		if skdPod.HostIP != pod.Status.HostIP {
			skdPod.HostIP = pod.Status.HostIP
			// Unreachable!
			// HostIP should not be changed.
			log.Infof(logPfx+"Error: HostIP changed! %v -> %v",
				skdPod.HostIP, pod.Status.HostIP)
		}
		skdNode := skdPod.Node
		if skdNode != nil && skdNode.HostIP != skdPod.HostIP {
			log.Infof(logPfx+"Pod (%v@%v) is not on the Node(%v)!",
				skdPod.Key, skdPod.HostIP, skdNode.HostIP)
			s.deletePod(skdPod)
			err := s.addPodToNode(skdPod)
			if err != nil {
				log.Infof(logPfx+"Fail: %v", err)
				s.enqueuePodKey(skdPod.Key)
			}
		}
		return nil
	}
	if len(pod.Status.HostIP) == 0 {
		return nil
	}
	frameworkKey := GetPodFrameworkKey(pod)
	limits := GetPodResourceLimits(pod)
	requests := GetPodResourceRequests(pod)
	hostIP := pod.Status.HostIP
	phase := pod.Status.Phase
	skdPod = &SkdPod{
		Key:          key,
		FrameworkKey: frameworkKey,
		HostIP:       hostIP,
		Node:         nil,
		Phase:        phase,
		Resources: SkdResourceRequirements{
			Limits:   limits,
			Requests: requests,
		},
		LastInformed: time.Now(),
	}
	err = s.addPodToNode(skdPod)
	if err != nil {
		return fmt.Errorf(logPfx+"Fail: Add Pod to local cache! %v", err)
	}
	return nil
}

func ReadLock(lock *sync.RWMutex) {
	lock.RLock()
	//log.Infof("RLock %v", lock)
}

func ReadUnlock(lock *sync.RWMutex) {
	//log.Infof("RUnock %v", lock)
	lock.RUnlock()
}

func WriteLock(lock *sync.RWMutex) {
	//log.Infof("try WLock %v", lock)
	lock.Lock()
	//log.Infof("WLock %v", lock)
}

func WriteUnlock(lock *sync.RWMutex) {
	//log.Infof("WUnock %v", lock)
	lock.Unlock()
}

func (s *FrameworkScheduler) lookupNodeKeyByHostIP(hostIP string) (string, bool) {
	ReadLock(s.lockOnHostIP)
	defer ReadUnlock(s.lockOnHostIP)
	nodeKey, ok := s.hostIP2node[hostIP]
	if ok {
		return nodeKey, ok
	} else {
		return "", false
	}
}

func (s *FrameworkScheduler) setupHostIPtoNodeKey(hostIP string, nodeKey string) {
	log.Infof("try map HostIP(=%v) to Node(=%v)", hostIP, nodeKey)
	WriteLock(s.lockOnHostIP)
	defer WriteUnlock(s.lockOnHostIP)
	s.hostIP2node[hostIP] = nodeKey
	log.Infof("map HostIP(=%v) to Node(=%v)", hostIP, nodeKey)
}

func (s *FrameworkScheduler) cleanupHostIP(hostIP string) {
	WriteLock(s.lockOnHostIP)
	defer WriteUnlock(s.lockOnHostIP)
	delete(s.hostIP2node, hostIP)
}

func GetPodFrameworkName(kPod *core.Pod) string {
	if name, ok := kPod.Labels[ci.LabelKeyFrameworkName]; ok {
		return name
	}
	return ""
}

func GetPodFrameworkKey(kPod *core.Pod) string {
	fname := GetPodFrameworkName(kPod)
	if len(fname) > 0 {
		return fmt.Sprintf("%v/%v", kPod.Namespace, fname)
	}
	return ""
}

func GetCPUQuantity(resList core.ResourceList) int64 {
	if q, ok := resList[core.ResourceCPU]; ok {
		val := q.ScaledValue(-3)
		//log.Infof("CPU: %v (%v)", q, val)
		return val
	}
	return 0
}

func GetMemoryQuantity(resList core.ResourceList) int64 {
	if q, ok := resList[core.ResourceMemory]; ok {
		val := q.ScaledValue(0) / (1024 * 1024)
		//log.Infof("Memory: %v (%v)", q, val)
		return val
	}
	return 0
}

func GetGPUQuantity(resList core.ResourceList) int64 {
	if q, ok := resList[core.ResourceNvidiaGPU]; ok {
		val := q.ScaledValue(0)
		//log.Infof("GPU: %v (%v)", q, val)
		return val
	}
	return 0
}

func GetPodResourceLimits(kPod *core.Pod) (res SkdResources) {
	res = SkdResources{0, 0, 0}
	for _, c := range kPod.Spec.Containers {
		res.CPU += GetCPUQuantity(c.Resources.Limits)
		res.Memory += GetMemoryQuantity(c.Resources.Limits)
		res.GPU += GetGPUQuantity(c.Resources.Limits)
	}
	return res
}

func GetPodResourceRequests(kPod *core.Pod) (res SkdResources) {
	res = SkdResources{0, 0, 0}
	for _, c := range kPod.Spec.Containers {
		res.CPU += GetCPUQuantity(c.Resources.Requests)
		res.Memory += GetMemoryQuantity(c.Resources.Requests)
		res.GPU += GetGPUQuantity(c.Resources.Requests)
	}
	return res
}

func (s *FrameworkScheduler) syncNode(key string) (returnedErr error) {
	startTime := time.Now()
	logPfx := fmt.Sprintf("[%v]: syncNode: ", key)
	log.Infof(logPfx + "Started")
	defer func() {
		if returnedErr != nil {
			// returnedErr is already prefixed with logPfx
			log.Warnf(returnedErr.Error())
			log.Warnf(logPfx +
				"Failed to due to Platform Transient Error. " +
				"Will enqueue it again after rate limited delay")
		}
		log.Infof(logPfx+"Completed: Duration %v", time.Since(startTime))
	}()

	node, err := s.nodeLister.Get(key)
	if err != nil {
		if apiErrors.IsNotFound(err) {
			// GarbageCollectionController will handle the dependent object
			// deletion according to the ownerReferences.
			log.Infof(logPfx+
				"Skipped: Node cannot be found in local cache: %v", err)
			s.deleteNodeByKey(key)
			return nil
		} else {
			return fmt.Errorf(logPfx+
				"Failed: Node cannot be got from local cache: %v", err)
		}
	}
	if node.DeletionTimestamp != nil {
		// Skip syncFramework to avoid fighting with GarbageCollectionController,
		// because GarbageCollectionController may be deleting the dependent object.
		log.Infof(logPfx+
			"Skipped: Node cannot be found in local cache: %v", err)
		s.deleteNodeByKey(key)
		return nil
	}
	var oldZoneKey string = ""
	var zoneKey string = ""
	skdNode := s.lookupNodeByKey(key)
	if skdNode != nil {
		oldZoneKey = skdNode.Zone.Key
		category := GetNodeLabelValue(node,
			ci.LabelKeyScheduleCategory, ci.DefaultScheduleCategory)
		zone := GetNodeLabelValue(node, ci.LabelKeyScheduleZone, ci.DefaultScheduleZone)
		zoneKey = ToZoneKey(category, zone)
		if oldZoneKey != zoneKey {
			s.deleteNode(skdNode)
			skdNode.ScheduleCategory = category
			skdNode.ScheduleZone = zone
			s.addNodeToZone(skdNode)
		}
		skdNode.Capacity = GetNodeCapacity(node)
		skdNode.Allocatable = GetNodeAllocatable(node)
		hostIP := GetNodeHostIP(node)
		if skdNode.HostIP != hostIP {
			log.Infof(logPfx+"Unexpected: Node's Host IP changed, %v->%v on %v",
				skdNode.HostIP, hostIP, key)
			s.cleanupHostIP(skdNode.HostIP)
			skdNode.HostIP = hostIP
			s.setupHostIPtoNodeKey(hostIP, key)
		}
		skdNode.LastInformed = time.Now()
		log.Infof(logPfx+"OK: skdNode exists: %v", key)
	} else {
		category := GetNodeLabelValue(node,
			ci.LabelKeyScheduleCategory, ci.DefaultScheduleCategory)
		zone := GetNodeLabelValue(node, ci.LabelKeyScheduleZone, ci.DefaultScheduleZone)
		zoneKey = ToZoneKey(category, zone)
		capacity := GetNodeCapacity(node)
		allocatable := GetNodeAllocatable(node)
		hostIP := GetNodeHostIP(node)
		skdNode = &SkdNode{
			Key:              key,
			ScheduleCategory: category,
			ScheduleZone:     zone,
			Zone:             nil,
			HostIP:           hostIP,
			Capacity:         capacity,
			Allocatable:      allocatable,
			Allocated:        SkdResources{0, 0, 0},
			Free:             SkdResources{0, 0, 0},
			lockOnPods:       new(sync.RWMutex),
			Pods:             make(map[string]*SkdPod),
			LastInformed:     time.Now(),
		}
		s.setupHostIPtoNodeKey(hostIP, key)
		s.addNodeToZone(skdNode)
		log.Infof(logPfx+"Add new skdNode to local cache: %v", key)
	}

	// Calculate free resources
	skdNode.Allocated = SkdResources{0, 0, 0}
	ReadLock(skdNode.lockOnPods)
	func() {
		defer ReadUnlock(skdNode.lockOnPods)
		for _, skdPod := range skdNode.Pods {
			if skdPod.HostIP != skdNode.HostIP {
				continue
			}
			if skdPod.Resources.Limits.CPU > skdPod.Resources.Requests.CPU {
				skdNode.Allocated.CPU += skdPod.Resources.Limits.CPU
			} else {
				skdNode.Allocated.CPU += skdPod.Resources.Requests.CPU
			}
			if skdPod.Resources.Limits.Memory > skdPod.Resources.Requests.Memory {
				skdNode.Allocated.Memory += skdPod.Resources.Limits.Memory
			} else {
				skdNode.Allocated.Memory += skdPod.Resources.Requests.Memory
			}
			if skdPod.Resources.Limits.GPU > skdPod.Resources.Requests.GPU {
				skdNode.Allocated.GPU += skdPod.Resources.Limits.GPU
			} else {
				skdNode.Allocated.GPU += skdPod.Resources.Requests.GPU
			}
			if !strings.HasPrefix(skdPod.Key, "kube-system/") {
				log.Infof(logPfx+"lim=%v req=%v on %v",
					skdPod.Resources.Limits, skdPod.Resources.Requests, skdPod.Key)
			}
		}
	}()
	skdNode.Free.CPU = skdNode.Capacity.CPU - skdNode.Allocated.CPU
	skdNode.Free.Memory = skdNode.Capacity.Memory - skdNode.Allocated.Memory
	skdNode.Free.GPU = skdNode.Capacity.GPU - skdNode.Allocated.GPU
	log.Infof(logPfx+"capacity=%v allocated=%v on //%v",
		skdNode.Capacity, skdNode.Allocated, skdNode.Key)
	s.enqueueZoneKey(zoneKey)
	return nil
}

func EncodeKey(key string) string {
	key = strings.ReplaceAll(key, "%", "%25")
	key = strings.ReplaceAll(key, "/", "%2f")
	return key
}

func DecodeKey(key string) string {
	key = strings.ReplaceAll(key, "%2f", "/")
	key = strings.ReplaceAll(key, "%25", "%")
	return key
}

func ToZoneKey(category string, zone string) string {
	log.Infof("%v/%v", category, zone)
	category = EncodeKey(category)
	zone = EncodeKey(zone)
	log.Infof("%v/%v %v/%v", category, zone, DecodeKey(category), DecodeKey(zone))
	return fmt.Sprintf("%v/%v", category, zone)
}

func GetNodeHostIP(node *core.Node) string {
	for _, addr := range node.Status.Addresses {
		if addr.Type == core.NodeInternalIP {
			return addr.Address
		}
	}
	log.Infof("Node %v has no internal ip: %v", node.Status.Addresses)
	return ""
}

func GetNodeLabelValue(node *core.Node, label string, defaultValue string) string {
	if val, ok := node.Labels[label]; ok {
		return val
	}
	return defaultValue
}

func GetNodeCapacity(node *core.Node) SkdResources {
	return SkdResources{
		GetCPUQuantity(node.Status.Capacity),
		GetMemoryQuantity(node.Status.Capacity),
		GetGPUQuantity(node.Status.Capacity),
	}
}

func GetNodeAllocatable(node *core.Node) SkdResources {
	return SkdResources{
		GetCPUQuantity(node.Status.Allocatable),
		GetMemoryQuantity(node.Status.Allocatable),
		GetGPUQuantity(node.Status.Allocatable),
	}
}

func MaxInt64(a, b int64) int64 {
	if a > b {
		return a
	} else {
		return b
	}
}

func (s *FrameworkScheduler) syncZone(key string) (returnedErr error) {
	startTime := time.Now()
	logPfx := fmt.Sprintf("[%v]: syncZone: ", key)
	log.Infof(logPfx + "Started")
	defer func() {
		if returnedErr != nil {
			// returnedErr is already prefixed with logPfx
			log.Warnf(returnedErr.Error())
			log.Warnf(logPfx +
				"Failed to due to Platform Transient Error. " +
				"Will enqueue it again after rate limited delay")
		}
		log.Infof(logPfx+"Completed: Duration %v", time.Since(startTime))
	}()

	category, zone, err := util.SplitKey(key)

	if err != nil {
		// Unreachable
		panic(fmt.Errorf(logPfx+
			"Failed: Got invalid key from queue, but the queue should only contain "+
			"valid keys: %v", err))
	}

	category = DecodeKey(category)
	zone = DecodeKey(zone)
	skdZone := s.lookupZoneByKey(key)
	if skdZone == nil {
		skdZone = s.CreateSkdZone(category, zone)
	}

	skdZone.LastInformed = time.Now()
	skdZone.RefreshTotalCapacity()

	if !skdZone.Alive {
		return nil
	}

	pods := skdZone.listPodsOnZone()

	free := skdZone.TotalCapacity
	for _, skdPod := range pods {
		free.CPU -= MaxInt64(skdPod.Resources.Limits.CPU, skdPod.Resources.Requests.CPU)
		free.Memory -= MaxInt64(skdPod.Resources.Limits.Memory, skdPod.Resources.Requests.Memory)
		free.GPU -= MaxInt64(skdPod.Resources.Limits.GPU, skdPod.Resources.Requests.GPU)
	}
	skdZone.TotalFree = free

	for _, skdPod := range pods {
		s.doneForPending(skdPod.FrameworkKey)
	}

	var fnames sort.StringSlice = nil
	for _, skdPod := range pods {
		_, name, err := util.SplitKey(skdPod.FrameworkKey)
		if err == nil && name != "" {
			fnames = append(fnames, name)
		}
	}
	fnames.Sort()

	if skdZone.hasFreeProvision() {
		s.scheduleZoneForWaiting(skdZone)
	}
	if skdZone.hasFreeResources() {
		s.scheduleZoneForPending(skdZone)
	}

	log.Infof("[%v] syncZone: capa=%v", key, skdZone.TotalCapacity)
	log.Infof("[%v] syncZone: free=%v", key, skdZone.TotalFree)
	log.Infof("[%v] syncZone: prov=%v", key, skdZone.TotalProvision)
	log.Infof("[%v] syncZone: allc=%v", key, skdZone.TotalAllocated)

	LogFrameworks(fmt.Sprintf("[%v] schedule: Queuing=", key), s.fmQueuing, s.lockOnQueuing)
	LogFrameworks(fmt.Sprintf("[%v] schedule: Waiting=", key), s.fmWaiting, s.lockOnWaiting)
	LogFrameworks(fmt.Sprintf("[%v] syncZone: Waiting=", key), skdZone.fmWaiting, skdZone.lockOnWaiting)
	LogFrameworks(fmt.Sprintf("[%v] schedule: Pending=", key), s.fmPending, s.lockOnPending)
	log.Infof("[%v] schedule: Running={%v}", key, strings.Join(fnames, ","))

	return nil
}

func LogFrameworks(prefix string, fm map[string]*SkdFramework, lock *sync.RWMutex) {
	var fnames sort.StringSlice = nil
	ReadLock(lock)
	func() {
		defer ReadUnlock(lock)
		fnames = make(sort.StringSlice, 0, len(fm))
		for _, fwk := range fm {
			fnames = append(fnames, fwk.Name)
		}
	}()
	fnames.Sort()
	log.Infof("%v{%v}", prefix, strings.Join(fnames, ","))
}

func (s *FrameworkScheduler) scheduleForWaiting() {
	zoneList := s.refreshZoneList()
	for _, skdZone := range zoneList {
		if skdZone.Alive && skdZone.hasFreeProvision() {
			s.scheduleZoneForWaiting(skdZone)
		}
	}
}

func (s *FrameworkScheduler) scheduleCategoryForWaiting(category string) {
	zoneList := s.refreshZoneList()
	for _, skdZone := range zoneList {
		if TestScheduleCategory(category, skdZone.ScheduleCategory) {
			if skdZone.Alive && skdZone.hasFreeProvision() {
				s.scheduleZoneForWaiting(skdZone)
			}
		}
	}
}

func (s *FrameworkScheduler) scheduleForPending() {
	zoneList := s.refreshZoneList()
	for _, skdZone := range zoneList {
		if skdZone.Alive && skdZone.hasFreeResources() {
			s.scheduleZoneForPending(skdZone)
		}
	}
}

func (s *FrameworkScheduler) scheduleCategoryForPending(category string) {
	zoneList := s.refreshZoneList()
	for _, skdZone := range zoneList {
		if TestScheduleCategory(category, skdZone.ScheduleCategory) {
			if skdZone.Alive && skdZone.hasFreeResources() {
				s.scheduleZoneForPending(skdZone)
			}
		}
	}
}

func (s *FrameworkScheduler) scheduleNewFrameworkOnWaiting(fwk *SkdFramework, f *ci.Framework) {
	fwk.resyncFramework(f)
	s.deleteFrameworkFromWaiting(fwk.Key)
	skdZone := fwk.Zone
	defer s.enqueueZoneKey(skdZone.Key)
	if skdZone.hasFreeResources() {
		fwks := fwk.Zone.listFrameworksOnWaiting()
		for _, fk := range fwks {
			if fwk.QueuingTimestamp.After(fk.QueuingTimestamp) {
				skdZone.addFrameworkToWaiting(fwk)
				return
			}
		}
		if skdZone.allocResourcesForFramework(fwk) {
			s.addFrameworkToPending(fwk)
			skdZone.freeReservedResourcesOfFramework(fwk)
			s.scheduleZoneForWaiting(skdZone)
			return
		}
	}
	skdZone.addFrameworkToWaiting(fwk)
}

func (s *FrameworkScheduler) scheduleRemotableFramework(category string) []*SkdFramework {
	ReadLock(s.lockOnQueuing)
	WriteLock(s.lockOnRemoting)
	defer func() {
		WriteUnlock(s.lockOnRemoting)
		ReadUnlock(s.lockOnQueuing)
	}()
	var fwks []*SkdFramework
	for key, fwk := range s.fmQueuing {
		if fwk.ScheduleCategory == category &&
			fwk.ScheduleRemoteble == ci.RemoteEnabled &&
			fwk.Remote.State == ci.RemoteHibernated {
			fwks = append(fwks, fwk)
			s.fmRemoting[key] = fwk
			fwk.Remote.State = ci.RemoteWaking
		}
	}
	return fwks
}

func (z *SkdZone) RefreshTotalCapacity() {
	var keys []string
	totalCapacity := SkdResources{0, 0, 0}

	ReadLock(z.lockOnNodes)
	func() {
		defer ReadUnlock(z.lockOnNodes)
		keys = make([]string, 0, len(z.Nodes))
		for key, skdNode := range z.Nodes {
			keys = append(keys, key)
			totalCapacity.CPU += skdNode.Capacity.CPU
			totalCapacity.Memory += skdNode.Capacity.Memory
			totalCapacity.GPU += skdNode.Capacity.GPU
		}
	}()

	z.lockOnResources.Lock()
	z.lockOnProvision.Lock()
	func() {
		defer func() {
			z.lockOnProvision.Unlock()
			z.lockOnResources.Unlock()
		}()
		z.TotalCapacity = totalCapacity
	}()

	z.Alive = (totalCapacity.CPU > 0 && totalCapacity.Memory > 0)

	if z.Alive {
		log.Infof("[%v] syncZone: nodes={%v}", z.Key, strings.Join(keys, ","))
	}

}

func (z *SkdZone) hasFreeProvision() bool {
	provision := z.TotalProvision
	capacity := z.TotalCapacity
	if capacity.GPU < provision.GPU {
		return false
	}
	if capacity.CPU < provision.CPU {
		return false
	}
	if capacity.Memory < provision.Memory {
		return false
	}
	return true
}

func (z *SkdZone) hasFreeResources() bool {
	free := z.TotalFree
	allocated := z.TotalAllocated
	if free.GPU-allocated.GPU < 0 {
		return false
	}
	if free.CPU-allocated.CPU < 0 {
		return false
	}
	if free.Memory-allocated.Memory < 0 {
		return false
	}
	return true
}

func (z *SkdZone) reserveResourcesForFramework(fwk *SkdFramework) bool {
	requests := fwk.calculateReservedResources()
	z.lockOnProvision.Lock()
	defer z.lockOnProvision.Unlock()
	if z.hasFreeProvision() {
		z.TotalProvision.CPU += requests.CPU
		z.TotalProvision.Memory += requests.Memory
		z.TotalProvision.GPU += requests.GPU
		return true
	}
	return false
}

func (z *SkdZone) freeReservedResourcesOfFramework(fwk *SkdFramework) {
	requests := fwk.calculateReservedResources()
	z.lockOnProvision.Lock()
	defer z.lockOnProvision.Unlock()
	z.TotalProvision.CPU -= requests.CPU
	z.TotalProvision.Memory -= requests.Memory
	z.TotalProvision.GPU -= requests.GPU
}

func (z *SkdZone) allocResourcesForFramework(fwk *SkdFramework) bool {
	requests := fwk.calculateReservedResources()
	z.lockOnResources.Lock()
	defer z.lockOnResources.Unlock()
	if z.hasFreeResources() {
		z.TotalAllocated.CPU += requests.CPU
		z.TotalAllocated.Memory += requests.Memory
		z.TotalAllocated.GPU += requests.GPU
		return true
	}
	return false
}

func (z *SkdZone) freeResourcesOfFramework(fwk *SkdFramework) {
	requests := fwk.calculateReservedResources()
	z.lockOnResources.Lock()
	defer z.lockOnResources.Unlock()
	z.TotalAllocated.CPU -= requests.CPU
	z.TotalAllocated.Memory -= requests.Memory
	z.TotalAllocated.GPU -= requests.GPU
}

func (fwk *SkdFramework) calculateReservedResources() SkdResources {
	requests := fwk.Resources.Requests
	limits := fwk.Resources.Limits
	if limits.CPU > requests.CPU {
		requests.CPU += limits.CPU
	}
	if limits.Memory > requests.Memory {
		requests.Memory += limits.Memory
	}
	if limits.GPU > requests.GPU {
		requests.GPU += limits.GPU
	}
	return requests
}

func (s *FrameworkScheduler) addFrameworkToQueuing(fwk *SkdFramework) {
	WriteLock(s.lockOnQueuing)
	defer WriteUnlock(s.lockOnQueuing)
	s.fmQueuing[fwk.Key] = fwk
}

func (s *FrameworkScheduler) deleteFrameworkFromQueuing(key string) {
	WriteLock(s.lockOnQueuing)
	defer WriteUnlock(s.lockOnQueuing)
	delete(s.fmQueuing, key)
}

func (s *FrameworkScheduler) listFrameworksOnQueuing() []*SkdFramework {
	ReadLock(s.lockOnQueuing)
	defer ReadUnlock(s.lockOnQueuing)
	fwks := make([]*SkdFramework, 0, len(s.fmQueuing))
	for _, fwk := range s.fmQueuing {
		fwks = append(fwks, fwk)
	}
	return fwks
}

func (s *FrameworkScheduler) listFrameworksOfCategoryOnQueuing(category string) []*SkdFramework {
	ReadLock(s.lockOnQueuing)
	defer ReadUnlock(s.lockOnQueuing)
	fwks := make([]*SkdFramework, 0, len(s.fmQueuing))
	for _, fwk := range s.fmQueuing {
		if TestScheduleCategory(fwk.ScheduleCategory, category) {
			fwks = append(fwks, fwk)
		}
	}
	return fwks
}

func (s *FrameworkScheduler) addFrameworkToWaiting(fwk *SkdFramework) {
	WriteLock(s.lockOnWaiting)
	defer WriteUnlock(s.lockOnWaiting)
	s.fmWaiting[fwk.Key] = fwk
}

func (s *FrameworkScheduler) deleteFrameworkFromWaiting(key string) {
	WriteLock(s.lockOnWaiting)
	defer WriteUnlock(s.lockOnWaiting)
	delete(s.fmWaiting, key)
}

func (z *SkdZone) addFrameworkToWaiting(fwk *SkdFramework) {
	WriteLock(z.lockOnWaiting)
	defer WriteUnlock(z.lockOnWaiting)
	z.fmWaiting[fwk.Key] = fwk
}

func (z *SkdZone) deleteFrameworkFromWaiting(key string) {
	WriteLock(z.lockOnWaiting)
	defer WriteUnlock(z.lockOnWaiting)
	delete(z.fmWaiting, key)
}

func (z *SkdZone) listFrameworksOnWaiting() []*SkdFramework {
	ReadLock(z.lockOnWaiting)
	defer ReadUnlock(z.lockOnWaiting)
	fwks := make([]*SkdFramework, 0, len(z.fmWaiting))
	for _, fwk := range z.fmWaiting {
		fwks = append(fwks, fwk)
	}
	return fwks
}

func (s *FrameworkScheduler) addFrameworkToPending(fwk *SkdFramework) {
	WriteLock(s.lockOnPending)
	defer WriteUnlock(s.lockOnPending)
	s.fmPending[fwk.Key] = fwk
}

func (s *FrameworkScheduler) deleteFrameworkFromPending(key string) {
	WriteLock(s.lockOnPending)
	defer WriteUnlock(s.lockOnPending)
	delete(s.fmPending, key)
}

func (z *SkdZone) listNodesOnZone() []*SkdNode {
	ReadLock(z.lockOnNodes)
	defer ReadUnlock(z.lockOnNodes)
	nodes := make([]*SkdNode, 0, len(z.Nodes))
	for _, skdNode := range z.Nodes {
		nodes = append(nodes, skdNode)
	}
	return nodes
}

func (n *SkdNode) listPodsOnNode() []*SkdPod {
	ReadLock(n.lockOnPods)
	defer ReadUnlock(n.lockOnPods)
	pods := make([]*SkdPod, 0, len(n.Pods))
	for _, skdPod := range n.Pods {
		pods = append(pods, skdPod)
	}
	return pods
}

func (z *SkdZone) listPodsOnZone() []*SkdPod {
	var pods []*SkdPod
	nodes := z.listNodesOnZone()
	for _, skdNode := range nodes {
		ps := skdNode.listPodsOnNode()
		pods = append(pods, ps...)
	}
	return pods
}

func TestScheduleCategory(fwkCategory, category string) bool {
	if fwkCategory == ci.DefaultScheduleCategory {
		return true
	}
	if fwkCategory == category {
		return true
	}
	return false
}

func (s *FrameworkScheduler) resyncFrameworks() {
	type ListFunc func() []*SkdFramework
	var funcs []ListFunc
	fnc := func() []*SkdFramework {
		return s.listFrameworksOnQueuing()
	}
	funcs = append(funcs, fnc)
	for _, fnc := range funcs {
		fwks := fnc()
		now := time.Now()
		for _, fwk := range fwks {
			if now.Sub(fwk.LastSync) > ci.TimeoutOfFrameworkSync {
				f, err := s.fLister.Frameworks(fwk.Namespace).Get(fwk.Name)
				if err != nil {
					if apiErrors.IsNotFound(err) {
						s.enqueueFrameworkKey(fwk.Key)
					}
				} else if f.DeletionTimestamp != nil {
					s.enqueueFrameworkKey(fwk.Key)
				}
				fwk.QueuingTimestamp = GetFrameworkQueuingTimestamp(f)
				fwk.LastSync = now
			}
			now = time.Now()
			//log.Infof("resync: %v - %v", fwk.Key, fwk.QueuingTimestamp)
		}
	}
}

func (s *FrameworkScheduler) scheduleZoneForWaiting(skdZone *SkdZone) {
	defer s.enqueueZoneKey(skdZone.Key)
	for {
		var nextWaiting *SkdFramework = nil
		fwks := s.listFrameworksOfCategoryOnQueuing(skdZone.ScheduleCategory)
		for _, fwk := range fwks {
			if nextWaiting == nil {
				nextWaiting = fwk
			}
			if fwk.QueuingTimestamp.Before(nextWaiting.QueuingTimestamp) {
				nextWaiting = fwk
			}
		}
		if nextWaiting == nil {
			return
		}
		if skdZone.reserveResourcesForFramework(nextWaiting) {
			nextWaiting.Zone = skdZone
			s.addFrameworkToWaiting(nextWaiting)
			s.deleteFrameworkFromQueuing(nextWaiting.Key)
			s.enqueueFrameworkKey(nextWaiting.Key)
		} else {
			return
		}
	}
}

func (s *FrameworkScheduler) scheduleZoneForPending(skdZone *SkdZone) {
	defer s.enqueueZoneKey(skdZone.Key)
	for {
		var nextPending *SkdFramework = nil
		fwks := skdZone.listFrameworksOnWaiting()
		for _, fwk := range fwks {
			if nextPending == nil {
				nextPending = fwk
			}
			if fwk.QueuingTimestamp.Before(nextPending.QueuingTimestamp) {
				nextPending = fwk
			}
		}
		if nextPending == nil {
			return
		}
		if skdZone.allocResourcesForFramework(nextPending) {
			s.addFrameworkToPending(nextPending)
			skdZone.deleteFrameworkFromWaiting(nextPending.Key)
			skdZone.freeReservedResourcesOfFramework(nextPending)
			s.enqueueFrameworkKey(nextPending.Key)
			s.scheduleZoneForWaiting(skdZone)
		} else {
			return
		}
	}
}

func (s *FrameworkScheduler) syncFrameworkState(f *ci.Framework) bool {
	/*/ TODO: Preschedule for remote
	if s.syncRemoteState(f) {
		return true
	} /*/

	if f.Status.State == ci.FrameworkAttemptCreationQueuing {
		fwk, exists := s.checkForWaiting(f)
		if exists {
			f.TransitionFrameworkState(ci.FrameworkAttemptCreationWaiting)
			s.scheduleNewFrameworkOnWaiting(fwk, f)
		} else {
			fwk, exists = s.addToQueuing(f)
			if exists {
				s.scheduleForWaiting()
			} else {
				s.scheduleCategoryForWaiting(fwk.ScheduleCategory)
			}
			if fwk, exists = s.checkForWaiting(f); !exists {
				return true
			}
			f.TransitionFrameworkState(ci.FrameworkAttemptCreationWaiting)
			s.scheduleNewFrameworkOnWaiting(fwk, f)
		}
	}

	if f.Status.State == ci.FrameworkAttemptCreationWaiting {
		fwk, exists := s.checkForPending(f)
		if exists {
			f.TransitionFrameworkState(ci.FrameworkAttemptCreationPending)
			s.bindFrameworkToZone(fwk, f)
			return false
		} else {
			fwk, exists = s.lookupFrameworkOnWaiting(f.Key())
			if !exists {
				fwk, _ := s.addToQueuing(f)
				f.TransitionFrameworkState(ci.FrameworkAttemptCreationQueuing)
				s.scheduleCategoryForWaiting(fwk.ScheduleCategory)
				return true
			}
			fwk.resyncFramework(f)
			s.scheduleCategoryForPending(fwk.ScheduleCategory)
			if fwk, exists = s.checkForPending(f); exists {
				f.TransitionFrameworkState(ci.FrameworkAttemptCreationPending)
				s.bindFrameworkToZone(fwk, f)
				return false
			}
			return true
		}
	}
	return false
}

func GetFrameworkLabel(f *ci.Framework, key string) string {
	if value, ok := f.Labels[key]; ok {
		return value
	}
	return ""
}

func SetFrameworkLabel(f *ci.Framework, key string, value string) {
	if f.Labels == nil {
		f.Labels = make(map[string]string)
	}
	f.Labels[key] = value
}

func (fwk *SkdFramework) transitionRemoteState(newState ci.RemoteState) {
	fwk.Remote.State = newState
}

func (s *FrameworkScheduler) lookupRemotingFramework(key string) *SkdFramework {
	ReadLock(s.lockOnRemoting)
	defer ReadUnlock(s.lockOnRemoting)
	if fwk, ok := s.fmRemoting[key]; ok {
		return fwk
	}
	return nil
}

func (s *FrameworkScheduler) syncRemoteState(f *ci.Framework) bool {
	fwk := s.lookupRemotingFramework(f.Key())
	if fwk == nil {
		if f.Status.State == ci.FrameworkAttemptCreationRemoting {
			// TODO: Recovering from restart of framework controler
			f.TransitionFrameworkState(ci.FrameworkAttemptCreationQueuing)
		}
		return false
	}
	fwk.resyncFramework(f)
	defer func() {
		if fwk.Remote.State == ci.RemoteWaking {
			s.enqueueKeyAfter(fwk.Key, 1)
		} else if fwk.Remote.State == ci.RemoteRequestAllowed {
			s.enqueueKeyAfter(fwk.Key, 3)
		} else {
			s.enqueueKeyAfter(fwk.Key, 5)
		}
	}()
	if fwk.Remote.State == ci.RemoteWaking {
		SetFrameworkLabel(f, ci.LabelKeyScheduleRemotable, ci.RemoteEnabled)
		SetFrameworkLabel(f, ci.LabelKeyScheduleRemoted, ci.RemoteEmpty)
		SetFrameworkLabel(f, ci.LabelKeyRemoteRequest, ci.RemoteEmpty)
		SetFrameworkLabel(f, ci.LabelKeyRemoteResponse, ci.RemoteEmpty)
		fwk.transitionRemoteState(ci.RemoteRequestAllowed)
		return true
	} else if fwk.Remote.State == ci.RemoteRequestAllowed {
		request := GetFrameworkLabel(f, ci.LabelKeyRemoteRequest)
		if request != ci.RemoteEmpty {
			fwk.Remote.Host = request
			fwk.Remote.Request = request
			fwk.transitionRemoteState(ci.RemoteRequestRecieved)
			if f.Status.State == ci.FrameworkAttemptCreationQueuing {
				f.TransitionFrameworkState(ci.FrameworkAttemptCreationRemoting)
				s.deleteFrameworkFromQueuing(fwk.Key)
				return true
			}
		}
		if f.Status.State != ci.FrameworkAttemptCreationQueuing {
			SetFrameworkLabel(f, ci.LabelKeyScheduleRemoted, ci.RemoteDinied)
			fwk.transitionRemoteState(ci.RemoteRequestDenied)
		}
		return false
	} else if fwk.Remote.State == ci.RemoteRequestRecieved {
		return false
	} else if fwk.Remote.State == ci.RemoteRequestRecieved {
		if f.Status.State == ci.FrameworkAttemptCreationRemoting {
			if fwk.Zone != nil {
				if _, exists := s.checkForWaiting(f); exists {
					s.deleteFrameworkFromWaiting(fwk.Key)
				} else {
					fwk.Zone.deleteFrameworkFromWaiting(fwk.Key)
				}
				fwk.Zone.freeReservedResourcesOfFramework(fwk)
				fwk.Zone = nil
			}
			SetFrameworkLabel(f, ci.LabelKeyScheduleRemoted, fwk.Remote.Request)
			fwk.transitionRemoteState(ci.RemoteRequestAccepted)
			return true
		}
		panic(fmt.Sprintf("f.Status.State should be %v while fwk.Remote.State is %v!",
			ci.FrameworkAttemptCreationRemoting,
			ci.RemoteRequestRecieved))
		return false
	}
	return false
}

func GetFrameworkAnnotation(f *ci.Framework, key string, defaultVal string) string {
	if val, ok := f.Annotations[key]; ok {
		if val != "" {
			return val
		}
	}
	return defaultVal
}

func GetFrameworkSchedulePreemption(f *ci.Framework) int64 {
	anno := GetFrameworkAnnotation(f, ci.AnnotationKeySchedulePreemption, "0")
	preemption, err := strconv.ParseInt(anno, 10, 64)
	if err != nil {
		return 0
	}
	return preemption
}

func GetFrameworkScheduleCategory(f *ci.Framework) string {
	return GetFrameworkAnnotation(f, ci.AnnotationKeyScheduleCategory, ci.DefaultScheduleCategory)
}

func GetFrameworkScheduleZone(f *ci.Framework) string {
	return GetFrameworkAnnotation(f, ci.AnnotationKeyScheduleZone, ci.DefaultScheduleZone)
}

func GetFrameworkResources(f *ci.Framework) SkdResourceRequirements {
	resources := SkdResourceRequirements{
		Limits:   SkdResources{0, 0, 0},
		Requests: SkdResources{0, 0, 0},
	}
	for _, taskRole := range f.Spec.TaskRoles {
		tn := int64(taskRole.TaskNumber)
		pod := taskRole.Task.Pod
		containers := append(pod.Spec.InitContainers, pod.Spec.Containers...)
		for _, container := range containers {
			lcpu := GetCPUQuantity(container.Resources.Limits)
			lmemory := GetMemoryQuantity(container.Resources.Limits)
			lgpu := GetGPUQuantity(container.Resources.Limits)
			rcpu := GetCPUQuantity(container.Resources.Requests)
			rmemory := GetMemoryQuantity(container.Resources.Requests)
			rgpu := GetGPUQuantity(container.Resources.Requests)
			// We assume best effort pod holding 1000MHz CPU and 512MB Memory
			if lcpu == 0 && rcpu == 0 {
				lcpu = 1000
			}
			if lmemory == 0 && rmemory == 0 {
				lmemory = 512
			}
			resources.Limits.CPU += lcpu * tn
			resources.Limits.Memory += lmemory * tn
			resources.Limits.GPU += lgpu * tn
			resources.Requests.CPU += rcpu * tn
			resources.Requests.Memory += rmemory * tn
			resources.Requests.GPU += rgpu * tn
		}
	}
	return resources
}

// Only frameworks of Queuing state will do checkForWaiting
func (s *FrameworkScheduler) checkForWaiting(f *ci.Framework) (*SkdFramework, bool) {
	key := f.Key()
	ReadLock(s.lockOnWaiting)
	defer ReadUnlock(s.lockOnWaiting)
	fwk, ok := s.fmWaiting[key]
	return fwk, ok
}

func GetFrameworkQueuingTimestamp(f *ci.Framework) time.Time {
	preemption := time.Duration(GetFrameworkSchedulePreemption(f)) * time.Second
	queuingTimestamp := f.CreationTimestamp.Time.Add(-preemption)
	return queuingTimestamp
}

func NewSdkFramework(f *ci.Framework) *SkdFramework {
	category := GetFrameworkScheduleCategory(f)
	zone := GetFrameworkScheduleZone(f)
	queuingTimestamp := GetFrameworkQueuingTimestamp(f)
	resources := GetFrameworkResources(f)
	fwk := &SkdFramework{
		Key:              f.Key(),
		Name:             f.Name,
		Namespace:        f.Namespace,
		ScheduleCategory: category,
		ScheduleZone:     zone,
		QueuingTimestamp: queuingTimestamp,
		Zone:             nil,
		Resources:        resources,
		LastSync:         time.Now(),
	}
	log.Infof("NewSdkFramework %v %v/%v", f.Name, category, zone)
	return fwk
}

func (fwk *SkdFramework) resyncFramework(f *ci.Framework) {
	// changing of preemption while changes QueuingTimestamp
	fwk.QueuingTimestamp = GetFrameworkQueuingTimestamp(f)
	// TODO: if resources will change ...
	fwk.LastSync = time.Now()
}

func (s *FrameworkScheduler) addToQueuing(f *ci.Framework) (*SkdFramework, bool) {
	key := f.Key()
	fwk, ok := s.fmQueuing[key]
	if ok {
		fwk.resyncFramework(f)
	} else {
		fwk = NewSdkFramework(f)
		s.addFrameworkToQueuing(fwk)
		log.Infof("scheduler: Add new framework %v to Queuing", key)
	}
	return fwk, ok
}

// Only frameworks in Waitting state will checkForPending
func (s *FrameworkScheduler) checkForPending(f *ci.Framework) (*SkdFramework, bool) {
	key := f.Key()
	ReadLock(s.lockOnPending)
	defer ReadUnlock(s.lockOnPending)
	fwk, ok := s.fmPending[key]
	return fwk, ok
}

// Frameworks in Pending state will be removed from fmPending
func (s *FrameworkScheduler) doneForPending(key string) {
	WriteLock(s.lockOnPending)
	defer WriteUnlock(s.lockOnPending)
	if fwk, ok := s.fmPending[key]; ok {
		fwk.Zone.freeResourcesOfFramework(fwk)
		delete(s.fmPending, key)
	}
}

func (s *FrameworkScheduler) bindFrameworkToZone(fwk *SkdFramework, f *ci.Framework) {
	// f.Annotations[ci.AnnotationKeyScheduleCategory] == skdZone.ScheduleCategory
	//SetFrameworkAnnotation(f, ci.AnnotationKeyScheduleZone, fwk.Zone.ScheduleZone)
	if f.Annotations == nil {
		f.Annotations = make(map[string]string)
	}
	f.Annotations[ci.AnnotationKeyScheduleZone] = fwk.Zone.ScheduleZone
	s.enqueueZoneKey(fwk.Zone.Key)
}

func (s *FrameworkScheduler) bindPodToZone(pod *core.Pod, f *ci.Framework) {
	category := GetFrameworkScheduleCategory(f)
	zone := GetFrameworkScheduleZone(f)
	if pod.Spec.NodeSelector == nil {
		pod.Spec.NodeSelector = make(map[string]string)
	}
	if category != ci.DefaultScheduleCategory {
		pod.Spec.NodeSelector[ci.LabelKeyScheduleCategory] = category
	}
	if zone != ci.DefaultScheduleZone {
		pod.Spec.NodeSelector[ci.LabelKeyScheduleZone] = zone
	}
}

func (s *FrameworkScheduler) lookupFrameworkOnWaiting(key string) (fwk *SkdFramework, ok bool) {
	ReadLock(s.lockOnWaiting)
	func() {
		defer ReadUnlock(s.lockOnWaiting)
		fwk, ok = s.fmWaiting[key]
	}()
	if ok {
		fwk.Zone.addFrameworkToWaiting(fwk)
		return fwk, ok
	}
	zoneList := s.refreshZoneList()
	for _, skdZone := range zoneList {
		ReadLock(skdZone.lockOnWaiting)
		func() {
			defer ReadUnlock(skdZone.lockOnWaiting)
			fwk, ok = skdZone.fmWaiting[key]
		}()
		if ok {
			return fwk, ok
		}
	}
	return fwk, ok
}

func (s *FrameworkScheduler) refreshZoneList() []*SkdZone {
	if s.lastRefreshedZoneList.After(s.lastModifiedZone) {
		return s.zoneList
	}
	ReadLock(s.lockOnZones)
	func() {
		defer ReadUnlock(s.lockOnZones)
		list := make([]*SkdZone, 0, len(s.zones))
		for _, skdZone := range s.zones {
			list = append(list, skdZone)
		}
		s.zoneList = list
	}()
	s.lastRefreshedZoneList = time.Now()
	return s.zoneList
}

func (s *FrameworkScheduler) onFrameworkDeleted(key string) {
	var fwk *SkdFramework
	var ok bool
	WriteLock(s.lockOnQueuing)
	func() {
		defer WriteUnlock(s.lockOnQueuing)
		if fwk, ok = s.fmQueuing[key]; ok {
			delete(s.fmQueuing, key)
		}
	}()
	if ok {
		return
	}
	WriteLock(s.lockOnWaiting)
	func() {
		defer WriteUnlock(s.lockOnWaiting)
		if fwk, ok = s.fmWaiting[key]; ok {
			delete(s.fmWaiting, key)
		}
	}()
	if ok {
		fwk.Zone.freeReservedResourcesOfFramework(fwk)
		return
	}
	zoneList := s.refreshZoneList()
	for _, skdZone := range zoneList {
		WriteLock(skdZone.lockOnWaiting)
		func() {
			defer WriteUnlock(skdZone.lockOnWaiting)
			if fwk, ok = skdZone.fmWaiting[key]; ok {
				delete(skdZone.fmWaiting, key)
			}
		}()
		if ok {
			fwk.Zone.freeReservedResourcesOfFramework(fwk)
			return
		}
	}
	s.doneForPending(key)
}
