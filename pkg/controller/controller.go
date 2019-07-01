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
	frameworkClient "github.com/microsoft/frameworkcontroller/pkg/client/clientset/versioned"
	frameworkInformer "github.com/microsoft/frameworkcontroller/pkg/client/informers/externalversions"
	frameworkLister "github.com/microsoft/frameworkcontroller/pkg/client/listers/frameworkcontroller/v1"
	"github.com/microsoft/frameworkcontroller/pkg/common"
	"github.com/microsoft/frameworkcontroller/pkg/util"
	errorWrap "github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	core "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	errorAgg "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	kubeInformer "k8s.io/client-go/informers"
	kubeClient "k8s.io/client-go/kubernetes"
	coreLister "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/retry"
	"k8s.io/client-go/util/workqueue"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// FrameworkController maintains the lifecycle for all Frameworks in the cluster.
// It is the engine to transition the Framework.Status and other Framework related
// objects to satisfy the Framework.Spec eventually.
type FrameworkController struct {
	kConfig *rest.Config
	cConfig *ci.Config

	// Client is used to write remote objects in ApiServer.
	// Remote objects are up-to-date and is writable.
	//
	// To read objects, it is better to use Lister instead of Client, since the
	// Lister is cached and the cache is the ground truth of other managed objects.
	//
	// Write remote objects cannot immediately change the local cached ground truth,
	// so, it is just a hint to drive the ground truth changes, and a complete write
	// should wait until the local cached objects reflect the write.
	//
	// Client already has retry policy to retry for most transient failures.
	// Client write failure does not mean the write does not succeed on remote, the
	// failure may be due to the success response is just failed to deliver to the
	// Client.
	kClient kubeClient.Interface
	fClient frameworkClient.Interface

	// Informer is used to sync remote objects to local cached objects, and deliver
	// events of object changes.
	//
	// The event delivery is level driven, not edge driven.
	// For example, the Informer may not deliver any event if a create is immediately
	// followed by a delete.
	cmInformer  cache.SharedIndexInformer
	podInformer cache.SharedIndexInformer
	fInformer   cache.SharedIndexInformer

	// Lister is used to read local cached objects in Informer.
	// Local cached objects may be outdated and is not writable.
	//
	// Outdated means current local cached objects may not reflect previous Client
	// remote writes.
	// For example, in previous round of syncFramework, Client created a Pod on
	// remote, however, in current round of syncFramework, the Pod may not appear
	// in the local cache, i.e. the local cached Pod is outdated.
	//
	// The local cached Framework.Status may be also outdated, so we take the
	// expected Framework.Status instead of the local cached one as the ground
	// truth of Framework.Status.
	//
	// The events of object changes are aligned with local cache, so we take the
	// local cached object instead of the remote one as the ground truth of
	// other managed objects except for the Framework.Status.
	// The outdated other managed object can be avoided by sync it only after the
	// remote write is also reflected in the local cache.
	cmLister  coreLister.ConfigMapLister
	podLister coreLister.PodLister
	fLister   frameworkLister.FrameworkLister

	// Queue is used to decouple items delivery and processing, i.e. control
	// how items are scheduled and distributed to process.
	// The items may come from Informer's events, or Controller's events, etc.
	//
	// It is not strictly FIFO because its Add method will only enqueue an item
	// if it is not already in the queue, i.e. the queue is deduplicated.
	// In fact, it is a FIFO pending set combined with a processing set instead of
	// a standard queue, i.e. a strict FIFO data structure.
	// So, even if we only allow to start a single worker, we cannot ensure all items
	// in the queue will be processed in FIFO order.
	// Finally, in any case, processing later enqueued item should not depend on the
	// result of processing previous enqueued item.
	//
	// However, it can be used to provide a processing lock for every different items
	// in the queue, i.e. the same item will not be processed concurrently, even in
	// the face of multiple concurrent workers.
	// Note, different items may still be processed concurrently in the face of
	// multiple concurrent workers. So, processing different items should modify
	// different objects to avoid additional concurrency control.
	//
	// Given above queue behaviors, we can choose to enqueue what kind of items:
	// 1. Framework Key
	//    Support multiple concurrent workers, but processing is coarse grained.
	//    Good at managing many small scale Frameworks.
	//    More idiomatic and easy to implement.
	// 2. All Managed Object Keys, such as Framework Key, Pod Key, etc
	//    Only support single worker, but processing is fine grained.
	//    Good at managing few large scale Frameworks.
	// 3. Events, such as [Pod p is added to Framework f]
	//    Only support single worker, and processing is fine grained.
	//    Good at managing few large scale Frameworks.
	// 4. Objects, such as Framework Object
	//    Only support single worker.
	//    Compared with local cached objects, the dequeued objects may be outdated.
	//    Internally, item will be used as map key, so objects means low performance.
	// Finally, we choose choice 1, so it is a Framework Key Queue.
	//
	// Processing is coarse grained:
	// Framework Key as item cannot differentiate Framework events, even for Add,
	// Update and Delete Framework event.
	// Besides, the dequeued item may be outdated compared the local cached one.
	// So, we can coarsen Add, Update and Delete event as a single Update event,
	// enqueue the Framework Key, and until the Framework Key is dequeued and started
	// to process, we refine the Update event to Add, Update or Delete event.
	//
	// Framework Key in queue should be valid, i.e. it can be SplitKey successfully.
	//
	// Enqueue a Framework Key means schedule a syncFramework for the Framework,
	// no matter the Framework's objects changed or not.
	//
	// Methods:
	// Add:
	//   Only keep the earliest item to dequeue:
	//   The item will only be enqueued if it is not already in the queue.
	// AddAfter:
	//   Only keep the earliest item to Add:
	//   The item may be Added before the duration elapsed, such as the same item
	//   is AddedAfter later with an earlier duration.
	fQueue workqueue.RateLimitingInterface

	// GlobalScheduler
	scheduler *GlobalScheduler

	// fExpectedStatusInfos is used to store the expected Framework.Status info for
	// all Frameworks.
	// See ExpectedFrameworkStatusInfo.
	//
	// Framework Key -> The expected Framework.Status info
	fExpectedStatusInfos map[string]*ExpectedFrameworkStatusInfo
}

type ExpectedFrameworkStatusInfo struct {
	// The expected Framework.Status.
	// It is the ground truth Framework.Status that the remote and the local cached
	// Framework.Status are expected to be.
	//
	// It is used to sync against the local cached Framework.Spec and the local
	// cached other related objects, and it helps to ensure the Framework.Status is
	// Monotonically Exposed.
	// Note, the local cached Framework.Status may be outdated compared with the
	// remote one, so without the it, the local cached Framework.Status is not
	// enough to ensure the Framework.Status is Monotonically Exposed.
	// See FrameworkStatus.
	status *ci.FrameworkStatus

	// Whether the expected Framework.Status is the same as the remote one.
	// It helps to ensure the expected Framework.Status is persisted before sync.
	remoteSynced bool
}

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
	lockOfPods       *sync.RWMutex
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
	Key              string
	Name             string
	Namespace        string
	ScheduleCategory string
	ScheduleZone     string
	QueuingTimestamp time.Time
	ZoneKey          string
	Zone             *SkdZone
	Resources        SkdResourceRequirements
	LastSync         time.Time
}

type SkdZone struct {
	Key              string
	ScheduleCategory string
	ScheduleZone     string
	TotalCapacity    SkdResources
	TotalAllocated   SkdResources
	TotalFree        SkdResources
	TotalProvision   SkdResources
	lockOfNodes      *sync.RWMutex
	lockOfWaiting    *sync.RWMutex
	Nodes            map[string]*SkdNode
	fmWaiting        map[string]*SkdFramework
	LastInformed     time.Time
}

type GlobalScheduler struct {
	fmQueuing map[string]*SkdFramework
	fmWaiting map[string]*SkdFramework
	fmPending map[string]*SkdFramework

	podLister    coreLister.PodLister
	fLister      frameworkLister.FrameworkLister
	nodeInformer cache.SharedIndexInformer
	nodeLister   coreLister.NodeLister
	fQueue       workqueue.RateLimitingInterface

	lockOfQueuing *sync.RWMutex
	lockOfWaiting *sync.RWMutex
	lockOfPending *sync.RWMutex

	lockOfHostIP *sync.RWMutex
	lockOfNodes  *sync.RWMutex
	lockOfPods   *sync.RWMutex
	lockOfZones  *sync.RWMutex

	hostIP2node map[string]string
	nodes       map[string]string
	pods        map[string]string
	zones       map[string]*SkdZone
	zoneList    []*SkdZone

	lastModifiedZone      time.Time
	lastRefreshedZoneList time.Time
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

func NewFrameworkController() *FrameworkController {
	log.Infof("Initializing " + ci.ComponentName)

	cConfig := ci.NewConfig()
	common.LogLines("With Config: \n%v", common.ToYaml(cConfig))
	kConfig := ci.BuildKubeConfig(cConfig)

	kClient, fClient := util.CreateClients(kConfig)

	// Informer resync will periodically replay the event of all objects stored in its cache.
	// However, by design, Informer and Controller should not miss any event.
	// So, we should disable resync to avoid hiding missing event bugs inside Controller.
	//
	// TODO: Add AttemptCreating state after SharedInformer supports IncludeUninitialized.
	// So that we can move the object initialization time out of the
	// ObjectLocalCacheCreationTimeoutSec, to reduce the expectation timeout false alarm
	// rate when Pod is specified with Initializers.
	// See https://github.com/kubernetes/kubernetes/pull/51247
	cmListerInformer := kubeInformer.NewSharedInformerFactory(kClient, 0).Core().V1().ConfigMaps()
	podListerInformer := kubeInformer.NewSharedInformerFactory(kClient, 0).Core().V1().Pods()
	fListerInformer := frameworkInformer.NewSharedInformerFactory(fClient, 0).Frameworkcontroller().V1().Frameworks()
	nodeListerInformer := kubeInformer.NewSharedInformerFactory(kClient, 0).Core().V1().Nodes()
	cmInformer := cmListerInformer.Informer()
	podInformer := podListerInformer.Informer()
	fInformer := fListerInformer.Informer()
	nodeInformer := nodeListerInformer.Informer()
	cmLister := cmListerInformer.Lister()
	podLister := podListerInformer.Lister()
	fLister := fListerInformer.Lister()
	nodeLister := nodeListerInformer.Lister()

	// Using DefaultControllerRateLimiter to rate limit on both particular items and overall items.
	fQueue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())

	scheduler := &GlobalScheduler{
		fmQueuing: make(map[string]*SkdFramework),
		fmWaiting: make(map[string]*SkdFramework),
		fmPending: make(map[string]*SkdFramework),

		podLister:    podLister,
		fLister:      fLister,
		nodeInformer: nodeInformer,
		nodeLister:   nodeLister,
		fQueue:       fQueue,

		lockOfQueuing: new(sync.RWMutex),
		lockOfWaiting: new(sync.RWMutex),
		lockOfPending: new(sync.RWMutex),

		lockOfHostIP: new(sync.RWMutex),
		lockOfPods:   new(sync.RWMutex),
		lockOfNodes:  new(sync.RWMutex),
		lockOfZones:  new(sync.RWMutex),

		hostIP2node: make(map[string]string),
		nodes:       make(map[string]string),
		pods:        make(map[string]string),
		zones:       make(map[string]*SkdZone),
		zoneList:    make([]*SkdZone, 0),

		lastModifiedZone:      time.Now(),
		lastRefreshedZoneList: time.Now(),
	}

	c := &FrameworkController{
		kConfig:              kConfig,
		cConfig:              cConfig,
		kClient:              kClient,
		fClient:              fClient,
		cmInformer:           cmInformer,
		podInformer:          podInformer,
		fInformer:            fInformer,
		cmLister:             cmLister,
		podLister:            podLister,
		fLister:              fLister,
		fQueue:               fQueue,
		scheduler:            scheduler,
		fExpectedStatusInfos: map[string]*ExpectedFrameworkStatusInfo{},
	}

	fInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			c.enqueueFrameworkObj(obj, "Framework Added")
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			// FrameworkController only cares about Framework.Spec update
			oldF := oldObj.(*ci.Framework)
			newF := newObj.(*ci.Framework)
			if !reflect.DeepEqual(oldF.Spec, newF.Spec) {
				c.enqueueFrameworkObj(newObj, "Framework.Spec Updated")
			}
		},
		DeleteFunc: func(obj interface{}) {
			c.enqueueFrameworkObj(obj, "Framework Deleted")
		},
	})

	cmInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			c.enqueueFrameworkConfigMapObj(obj, "Framework ConfigMap Added")
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			c.enqueueFrameworkConfigMapObj(newObj, "Framework ConfigMap Updated")
		},
		DeleteFunc: func(obj interface{}) {
			c.enqueueFrameworkConfigMapObj(obj, "Framework ConfigMap Deleted")
		},
	})

	podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			c.enqueueFrameworkPodObj(obj, "Framework Pod Added")
			scheduler.enqueuePodObj(obj, "(******) Pod Added")
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			c.enqueueFrameworkPodObj(newObj, "Framework Pod Updated")
			scheduler.enqueuePodObj(newObj, "(******) Pod Updated")
		},
		DeleteFunc: func(obj interface{}) {
			c.enqueueFrameworkPodObj(obj, "Framework Pod Deleted")
			scheduler.enqueuePodObj(obj, "(******) Pod Deleted")
		},
	})

	nodeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			scheduler.enqueueNodeObj(obj, "(******) Node Added")
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			scheduler.enqueueNodeObj(newObj, "(******) Node Updated")
		},
		DeleteFunc: func(obj interface{}) {
			scheduler.enqueueNodeObj(obj, "(******) Node Deleted")
		},
	})

	return c
}

// obj could be *ci.Framework or cache.DeletedFinalStateUnknown.
func (c *FrameworkController) enqueueFrameworkObj(obj interface{}, msg string) {
	key, err := util.GetKey(obj)
	if err != nil {
		log.Errorf("Failed to get key for obj %#v, skip to enqueue: %v", obj, err)
		return
	}

	_, _, err = util.SplitKey(key)
	if err != nil {
		log.Errorf("Got invalid key %v for obj %#v, skip to enqueue: %v", key, obj, err)
		return
	}

	c.fQueue.Add(key)
	log.Infof("[%v]: enqueueFrameworkObj: %v", key, msg)
}

// obj could be *core.ConfigMap or cache.DeletedFinalStateUnknown.
func (c *FrameworkController) enqueueFrameworkConfigMapObj(obj interface{}, msg string) {
	if cm := util.ToConfigMap(obj); cm != nil {
		if f := c.getConfigMapOwner(cm); f != nil {
			c.enqueueFrameworkObj(f, msg+": "+cm.Name)
		}
	}
}

// obj could be *core.Pod or cache.DeletedFinalStateUnknown.
func (c *FrameworkController) enqueueFrameworkPodObj(obj interface{}, msg string) {
	if pod := util.ToPod(obj); pod != nil {
		if cm := c.getPodOwner(pod); cm != nil {
			c.enqueueFrameworkConfigMapObj(cm, msg+": "+pod.Name)
		}
	}
}

// obj could be *core.Pod or cache.DeletedFinalStateUnknown.
func (s *GlobalScheduler) enqueuePodObj(obj interface{}, msg string) {
	key, err := util.GetKey(obj)
	if err != nil {
		log.Errorf("Failed to get key for pod %#v, skip to enqueue: %v", obj, err)
		return
	}
	s.enqueuePodKey(key)
	log.Infof("[%v]: enqueuePodObj: %v", key, msg)
}

// obj could be *core.Node or cache.DeletedFinalStateUnknown.
func (s *GlobalScheduler) enqueueNodeObj(obj interface{}, msg string) {
	key, err := util.GetKey(obj)
	if err != nil {
		log.Errorf("Failed to get key for node %#v, skip to enqueue: %v", obj, err)
		return
	}
	s.enqueueNodeKey(key)
	log.Infof("[%v]: enqueueNodeObj: %v", key, msg)
}

func (s *GlobalScheduler) enqueuePodKey(key string) {
	s.enqueueKey(ci.QueueKeyPrefixPod, key)
	log.Infof("[%v]: enqueuePodKey.", key)
}

func (s *GlobalScheduler) enqueueNodeKey(key string) {
	s.enqueueKey(ci.QueueKeyPrefixNode, key)
	log.Infof("[%v]: enqueueNodeKey.", key)
}

func (s *GlobalScheduler) enqueueZoneKey(key string) {
	s.enqueueKey(ci.QueueKeyPrefixZone, key)
	log.Infof("[%v]: enqueueZoneKey.", key)
}

func (s *GlobalScheduler) enqueueKey(prefix string, key string) {
	s.fQueue.AddRateLimited(fmt.Sprintf("%v%v", prefix, key))
}

func (s *GlobalScheduler) enqueueFrameworkKey(key string) {
	s.fQueue.AddRateLimited(key)
}

func (c *FrameworkController) getConfigMapOwner(cm *core.ConfigMap) *ci.Framework {
	cmOwner := meta.GetControllerOf(cm)
	if cmOwner == nil {
		return nil
	}

	if cmOwner.Kind != ci.FrameworkKind {
		return nil
	}

	f, err := c.fLister.Frameworks(cm.Namespace).Get(cmOwner.Name)
	if err != nil {
		if !apiErrors.IsNotFound(err) {
			log.Errorf(
				"[%v]: ConfigMapOwner %#v cannot be got from local cache: %v",
				cm.Namespace+"/"+cm.Name, *cmOwner, err)
		}
		return nil
	}

	if f.UID != cmOwner.UID {
		// GarbageCollectionController will handle the dependent object
		// deletion according to the ownerReferences.
		return nil
	}

	return f
}

func (c *FrameworkController) getPodOwner(pod *core.Pod) *core.ConfigMap {
	podOwner := meta.GetControllerOf(pod)
	if podOwner == nil {
		return nil
	}

	if podOwner.Kind != ci.ConfigMapKind {
		return nil
	}

	cm, err := c.cmLister.ConfigMaps(pod.Namespace).Get(podOwner.Name)
	if err != nil {
		if !apiErrors.IsNotFound(err) {
			log.Errorf(
				"[%v]: PodOwner %#v cannot be got from local cache: %v",
				pod.Namespace+"/"+pod.Name, *podOwner, err)
		}
		return nil
	}

	if cm.UID != podOwner.UID {
		// GarbageCollectionController will handle the dependent object
		// deletion according to the ownerReferences.
		return nil
	}

	return cm
}

func (c *FrameworkController) Run(stopCh <-chan struct{}) {
	/**/
	defer c.fQueue.ShutDown()
	defer log.Errorf("Stopping " + ci.ComponentName)
	defer runtime.HandleCrash()

	log.Infof("Recovering " + ci.ComponentName)
	util.PutCRD(
		c.kConfig,
		ci.BuildFrameworkCRD(),
		c.cConfig.CRDEstablishedCheckIntervalSec,
		c.cConfig.CRDEstablishedCheckTimeoutSec)

	go c.fInformer.Run(stopCh)
	go c.cmInformer.Run(stopCh)
	go c.podInformer.Run(stopCh)
	go c.scheduler.nodeInformer.Run(stopCh)
	if !cache.WaitForCacheSync(
		stopCh,
		c.fInformer.HasSynced,
		c.cmInformer.HasSynced,
		c.podInformer.HasSynced,
		c.scheduler.nodeInformer.HasSynced) {
		panic("Failed to WaitForCacheSync")
	}

	log.Infof("Running %v with %v workers",
		ci.ComponentName, *c.cConfig.WorkerNumber)

	for i := int32(0); i < *c.cConfig.WorkerNumber; i++ {
		// id is dedicated for each iteration, while i is not.
		id := i
		go wait.Until(func() { c.worker(id) }, time.Second, stopCh)
	}
	/**/
	//	go NewInterdomScheduler(c).Run(stopCh)

	<-stopCh
}

func (c *FrameworkController) worker(id int32) {
	defer log.Errorf("Stopping worker-%v", id)
	log.Infof("Running worker-%v", id)

	for c.processNextWorkItem(id) {
	}
}

func (c *FrameworkController) processNextWorkItem(id int32) bool {
	// Blocked to get an item which is different from the current processing items.
	key, quit := c.fQueue.Get()
	if quit {
		return false
	}
	log.Infof("[%v]: Assigned to worker-%v", key, id)

	// Remove the item from the current processing items to unblock getting the
	// same item again.
	defer c.fQueue.Done(key)

	var err error
	if HasPrefixPod(key) {
		err = c.scheduler.syncPod(GetPodKey(key))
	} else if HasPrefixNode(key) {
		err = c.scheduler.syncNode(GetNodeKey(key))
	} else if HasPrefixZone(key) {
		err = c.scheduler.syncZone(GetZoneKey(key))
	} else {
		err = c.syncFramework(key.(string))
	}
	if err == nil {
		// Reset the rate limit counters of the item in the queue, such as NumRequeues,
		// because we have synced it successfully.
		c.fQueue.Forget(key)
	} else {
		c.fQueue.AddRateLimited(key)
	}

	return true
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

func (s *GlobalScheduler) lookupNodeKeyByPod(key string) (string, bool) {
	ReadLock(s.lockOfPods)
	defer ReadUnlock(s.lockOfPods)
	nodeKey, ok := s.pods[key]
	if ok {
		return nodeKey, true
	} else {
		return "", false
	}
}

func (s *GlobalScheduler) lookupZoneKeyByNode(key string) (string, bool) {
	ReadLock(s.lockOfNodes)
	defer ReadUnlock(s.lockOfNodes)
	zoneKey, ok := s.nodes[key]
	if ok {
		return zoneKey, true
	} else {
		return "", false
	}
}

func (s *GlobalScheduler) cleanupPodKey(key string) {
	WriteLock(s.lockOfPods)
	defer WriteUnlock(s.lockOfPods)
	delete(s.pods, key)
}

func (s *GlobalScheduler) cleanupNodeKey(key string) {
	WriteLock(s.lockOfNodes)
	defer WriteUnlock(s.lockOfNodes)
	delete(s.nodes, key)
}

func (s *GlobalScheduler) lookupPodByKey(key string) *SkdPod {
	nodeKey, ok := s.lookupNodeKeyByPod(key)
	if !ok {
		return nil
	}
	skdNode := s.lookupNodeByKey(nodeKey)
	if skdNode == nil {
		return nil
	}
	ReadLock(skdNode.lockOfPods)
	defer ReadUnlock(skdNode.lockOfPods)
	skdPod, exists := skdNode.Pods[key]
	if exists {
		return skdPod
	}
	return nil
}

func (s *GlobalScheduler) lookupNodeByKey(key string) *SkdNode {
	zoneKey, ok := s.lookupZoneKeyByNode(key)
	if !ok {
		return nil
	}
	skdZone := s.lookupZoneByKey(zoneKey)
	if skdZone == nil {
		return nil
	}
	ReadLock(skdZone.lockOfNodes)
	defer ReadUnlock(skdZone.lockOfNodes)
	skdNode, exists := skdZone.Nodes[key]
	if exists {
		return skdNode
	}
	return nil
}

func (s *GlobalScheduler) lookupZoneByKey(key string) *SkdZone {
	ReadLock(s.lockOfZones)
	defer ReadUnlock(s.lockOfZones)
	skdZone, exists := s.zones[key]
	if exists {
		return skdZone
	}
	return nil
}

func (s *GlobalScheduler) addPodToNode(skdPod *SkdPod) error {
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
	WriteLock(skdNode.lockOfPods)
	func() {
		defer WriteUnlock(skdNode.lockOfPods)
		skdNode.Pods[skdPod.Key] = skdPod
		skdPod.Node = skdNode
	}()
	// Add the map of {skdPod.Key -> skdNode.Key}
	WriteLock(s.lockOfPods)
	func() {
		defer WriteUnlock(s.lockOfPods)
		s.pods[skdPod.Key] = nodeKey
	}()
	// pods in node changed, notify the node
	s.enqueueNodeKey(nodeKey)
	// Check the framework of the pod and free resources alloced to the framework.
	// Resources are now alloced by pods of framework.
	// TODO: pods on multiple nodes might be of the same framework
	s.doneForPending(skdPod.FrameworkKey)
	return nil
}

func (s *GlobalScheduler) addNodeToZone(skdNode *SkdNode) {
	zoneKey := ToZoneKey(skdNode.ScheduleCategory, skdNode.ScheduleZone)
	skdZone := s.lookupZoneByKey(zoneKey)
	if skdZone == nil {
		skdZone = s.CreateSkdZone(skdNode.ScheduleCategory, skdNode.ScheduleZone)
	}
	// Add skdNode to skdZone
	WriteLock(skdZone.lockOfNodes)
	func() {
		defer WriteUnlock(skdZone.lockOfNodes)
		skdZone.Nodes[skdNode.Key] = skdNode
		skdNode.Zone = skdZone
	}()
	// Add the map of {skdNode.Key -> skdZone.Key}
	WriteLock(s.lockOfNodes)
	func() {
		defer WriteUnlock(s.lockOfNodes)
		s.nodes[skdNode.Key] = zoneKey
	}()
	// nodes in zone changed, notify the zone
	s.enqueueZoneKey(zoneKey)
}

func (s *GlobalScheduler) CreateSkdZone(category string, zone string) *SkdZone {
	zoneKey := ToZoneKey(category, zone)
	skdZone := &SkdZone{
		Key:              zoneKey,
		ScheduleCategory: category,
		ScheduleZone:     zone,
		TotalCapacity:    SkdResources{0, 0, 0},
		TotalAllocated:   SkdResources{0, 0, 0},
		TotalFree:        SkdResources{0, 0, 0},
		TotalProvision:   SkdResources{0, 0, 0},
		lockOfNodes:      new(sync.RWMutex),
		lockOfWaiting:    new(sync.RWMutex),
		Nodes:            make(map[string]*SkdNode),
		fmWaiting:        make(map[string]*SkdFramework),
		LastInformed:     time.Now(),
	}
	WriteLock(s.lockOfZones)
	defer WriteUnlock(s.lockOfZones)
	s.zones[zoneKey] = skdZone
	return skdZone
}

func (s *GlobalScheduler) deletePod(skdPod *SkdPod) {
	skdNode := skdPod.Node
	if skdNode != nil {
		// Delete pod from node
		WriteLock(skdNode.lockOfPods)
		func() {
			defer WriteUnlock(skdNode.lockOfPods)
			skdPod.Node = nil
			delete(skdNode.Pods, skdPod.Key)
		}()
		// Pods in node are changed, notify the node
		s.enqueueNodeKey(skdNode.Key)
	}
	s.cleanupPodKey(skdPod.Key)
}

func (s *GlobalScheduler) deleteNode(skdNode *SkdNode) {
	skdZone := skdNode.Zone
	if skdZone != nil {
		// Delete node from zone
		WriteLock(skdZone.lockOfNodes)
		func() {
			defer WriteUnlock(skdZone.lockOfNodes)
			skdNode.Zone = nil
			delete(skdZone.Nodes, skdNode.Key)
		}()
		// Nodes in zone are changed, notify the zone
		s.enqueueZoneKey(skdZone.Key)
	}
	s.cleanupNodeKey(skdNode.Key)
}

func (s *GlobalScheduler) deleteZone(skdZone *SkdZone) {
	WriteLock(s.lockOfZones)
	defer WriteUnlock(s.lockOfZones)
	delete(s.zones, skdZone.Key)
}

func (s *GlobalScheduler) deletePodByKey(key string) {
	skdPod := s.lookupPodByKey(key)
	if skdPod != nil {
		s.deletePod(skdPod)
	} else {
		s.cleanupPodKey(key)
	}
}

func (s *GlobalScheduler) deleteNodeByKey(key string) {
	skdNode := s.lookupNodeByKey(key)
	if skdNode != nil {
		s.deleteNode(skdNode)
	} else {
		s.cleanupNodeKey(key)
	}
}

func (s *GlobalScheduler) syncPod(key string) (returnedErr error) {
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

func (s *GlobalScheduler) lookupNodeKeyByHostIP(hostIP string) (string, bool) {
	ReadLock(s.lockOfHostIP)
	defer ReadUnlock(s.lockOfHostIP)
	nodeKey, ok := s.hostIP2node[hostIP]
	if ok {
		return nodeKey, ok
	} else {
		return "", false
	}
}

func (s *GlobalScheduler) setupHostIPtoNodeKey(hostIP string, nodeKey string) {
	log.Infof("try map HostIP(=%v) to Node(=%v)", hostIP, nodeKey)
	WriteLock(s.lockOfHostIP)
	defer WriteUnlock(s.lockOfHostIP)
	s.hostIP2node[hostIP] = nodeKey
	log.Infof("map HostIP(=%v) to Node(=%v)", hostIP, nodeKey)
}

func (s *GlobalScheduler) cleanupHostIP(hostIP string) {
	WriteLock(s.lockOfHostIP)
	defer WriteUnlock(s.lockOfHostIP)
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

func (s *GlobalScheduler) syncNode(key string) (returnedErr error) {
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
			lockOfPods:       new(sync.RWMutex),
			Pods:             make(map[string]*SkdPod),
			LastInformed:     time.Now(),
		}
		s.setupHostIPtoNodeKey(hostIP, key)
		s.addNodeToZone(skdNode)
		log.Infof(logPfx+"Add new skdNode to local cache: %v", key)
	}

	// Calculate free resources
	skdNode.Allocated = SkdResources{0, 0, 0}
	ReadLock(skdNode.lockOfPods)
	func() {
		defer ReadUnlock(skdNode.lockOfPods)
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

func (s *GlobalScheduler) syncZone(key string) (returnedErr error) {
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

	skdZone.RefreshTotalCapacityAndFree()

	if skdZone.HasFreeProvision() {
		fwk := s.ScheduleWaitingForZone(skdZone)
		if fwk != nil {
			if fwk.Zone == skdZone {
				s.ScheduleWaitingToZone(fwk, skdZone)
				log.Infof(logPfx+"ScheduleWaitingToZone(%v)", fwk.Key)
			} else {
				log.Infof(logPfx+"ScheduleWaitingForZone(%v)", fwk.Key)
				s.enqueueFrameworkKey(fwk.Key)
			}
		} else {
			fwk = s.ScheduleQueuingFramework(skdZone.ScheduleCategory)
			if fwk != nil {
				log.Infof(logPfx+"ScheduleQueuingFramework: %v -> %v", 
											fwk.Key, skdZone.ScheduleCategory)
				fwk.ZoneKey = skdZone.Key
				s.enqueueFrameworkKey(fwk.Key)
			}
		}
	}

	if skdZone.HasFreeResources() {
		fwk := s.ScheduleWaitingFramework(skdZone)
		if fwk != nil {
			log.Infof(logPfx+"ScheduleWaitingFramework(%v)", fwk.Key)
			s.enqueueFrameworkKey(fwk.Key)
		}
	}

	log.Infof("[%v] syncZone: capa=%v", key, skdZone.TotalCapacity)
	log.Infof("[%v] syncZone: free=%v", key, skdZone.TotalFree)
	log.Infof("[%v] syncZone: prov=%v", key, skdZone.TotalProvision)
	log.Infof("[%v] syncZone: allc=%v", key, skdZone.TotalAllocated)

	LogFrameworks(fmt.Sprintf("[%v] schedule: Queuing=", key), s.fmQueuing, s.lockOfQueuing)
	LogFrameworks(fmt.Sprintf("[%v] schedule: Waiting=", key), s.fmWaiting, s.lockOfWaiting)
	LogFrameworks(fmt.Sprintf("[%v] syncZone: Waiting=", key), skdZone.fmWaiting, skdZone.lockOfWaiting)
	LogFrameworks(fmt.Sprintf("[%v] schedule: Pending=", key), s.fmPending, s.lockOfPending)

	s.lastModifiedZone = time.Now()

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

func (z *SkdZone) RefreshTotalCapacityAndFree() {
	var keys []string
	totalCapacity := SkdResources{0, 0, 0}
	totalFree := SkdResources{0, 0, 0}

	ReadLock(z.lockOfNodes)
	func() {
		defer ReadUnlock(z.lockOfNodes)
		keys = make([]string, 0, len(z.Nodes))
		for key, skdNode := range z.Nodes {
			keys = append(keys, key)
			totalCapacity.CPU += skdNode.Capacity.CPU
			totalCapacity.Memory += skdNode.Capacity.Memory
			totalCapacity.GPU += skdNode.Capacity.GPU
			totalFree.CPU += skdNode.Free.CPU
			totalFree.Memory += skdNode.Free.Memory
			totalFree.GPU += skdNode.Free.GPU
		}
	}()

	z.TotalCapacity = totalCapacity
	z.TotalFree = totalFree

	log.Infof("[%v] syncZone: nodes={%v}", z.Key, strings.Join(keys, ","))

}

func (z *SkdZone) HasFreeProvision() bool {
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

func (z *SkdZone) HasFreeResources() bool {
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

func (z *SkdZone) ReserveResourcesForFramework(fwk *SkdFramework) {
	provision := z.TotalProvision
	requests := fwk.CalculateReservedResources()
	provision.CPU += requests.CPU
	provision.Memory += requests.Memory
	provision.GPU += requests.GPU
	z.TotalProvision = provision
}

func (z *SkdZone) FreeReservedResourcesOfFramework(fwk *SkdFramework) {
	provision := z.TotalProvision
	requests := fwk.CalculateReservedResources()
	provision.CPU -= requests.CPU
	provision.Memory -= requests.Memory
	provision.GPU -= requests.GPU
	z.TotalProvision = provision
}

func (z *SkdZone) AllocResourcesForFramework(fwk *SkdFramework) {
	allocated := z.TotalAllocated
	requests := fwk.CalculateReservedResources()
	allocated.CPU += requests.CPU
	allocated.Memory += requests.Memory
	allocated.GPU += requests.GPU
	z.TotalAllocated = allocated
}

func (z *SkdZone) FreeResourcesOfFramework(fwk *SkdFramework) {
	allocated := z.TotalAllocated
	requests := fwk.CalculateReservedResources()
	allocated.CPU -= requests.CPU
	allocated.Memory -= requests.Memory
	allocated.GPU -= requests.GPU
	z.TotalAllocated = allocated
}

func (fwk *SkdFramework) CalculateReservedResources() SkdResources {
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

func (s *GlobalScheduler) ScheduleWaitingForZone(skdZone *SkdZone) *SkdFramework {
	ReadLock(s.lockOfWaiting)
	defer ReadUnlock(s.lockOfWaiting)
	for _, fwk := range s.fmWaiting {
		if fwk.Zone == skdZone {
			return fwk
		}
		if fwk.ZoneKey == skdZone.Key {
			return fwk
		}
	}
	return nil
}

func (s *GlobalScheduler) ScheduleWaitingToZone(fwk *SkdFramework, skdZone *SkdZone) {
	WriteLock(s.lockOfWaiting)
	func() {
		defer WriteUnlock(s.lockOfWaiting)
		delete(s.fmWaiting, fwk.Key)
	}()
	WriteLock(skdZone.lockOfWaiting)
	func() {
		defer WriteUnlock(skdZone.lockOfWaiting)
		skdZone.fmWaiting[fwk.Key] = fwk
		skdZone.ReserveResourcesForFramework(fwk)
	}()
}

func (s *GlobalScheduler) AddFrameworkToQueuing(fwk *SkdFramework) {
	WriteLock(s.lockOfQueuing)
	defer WriteUnlock(s.lockOfQueuing)
	s.fmQueuing[fwk.Key] = fwk
}

func (s *GlobalScheduler) DeleteFrameworkFromQueuing(key string) {
	WriteLock(s.lockOfQueuing)
	defer WriteUnlock(s.lockOfQueuing)
	delete(s.fmQueuing, key)
}

func (s *GlobalScheduler) AddFrameworkToWaiting(fwk *SkdFramework) {
	WriteLock(s.lockOfWaiting)
	defer WriteUnlock(s.lockOfWaiting)
	s.fmWaiting[fwk.Key] = fwk
}

func (s *GlobalScheduler) DeleteFrameworkFromWaiting(key string) {
	WriteLock(s.lockOfWaiting)
	defer WriteUnlock(s.lockOfWaiting)
	delete(s.fmWaiting, key)
}

func (z *SkdZone) AddFrameworkToWaiting(fwk *SkdFramework) {
	WriteLock(z.lockOfWaiting)
	defer WriteUnlock(z.lockOfWaiting)
	z.fmWaiting[fwk.Key] = fwk
}

func (z *SkdZone) DeleteFrameworkFromWaiting(key string) {
	WriteLock(z.lockOfWaiting)
	defer WriteUnlock(z.lockOfWaiting)
	delete(z.fmWaiting, key)
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

func (s *GlobalScheduler) ScheduleQueuingFramework(scheduleCategory string) *SkdFramework {
	var nextWaiting *SkdFramework = nil
	fwks := make([]*SkdFramework, 0)
	ReadLock(s.lockOfQueuing)
	func() {
		defer ReadUnlock(s.lockOfQueuing)
		for _, fwk := range s.fmQueuing {
			fwks = append(fwks, fwk)
		}
	}()
	now := time.Now()
	for _, fwk := range fwks {
		if !TestScheduleCategory(fwk.ScheduleCategory, scheduleCategory) {
			continue
		}
		if now.Sub(fwk.LastSync) > ci.TimeoutOfFrameworkSync {
			f, err := s.fLister.Frameworks(fwk.Namespace).Get(fwk.Name)
			if err != nil {
				if apiErrors.IsNotFound(err) {
					// framework does not exist, delete it from fmQueuing
					s.DeleteFrameworkFromQueuing(fwk.Key)
				}
				continue
			} else if f.DeletionTimestamp != nil {
				// framework is being deleted, so delete it from fmQueuing
				s.DeleteFrameworkFromQueuing(fwk.Key)
				continue
			}
			fwk.QueuingTimestamp = GetFrameworkQueuingTimestamp(f)
			fwk.LastSync = now
			log.Infof("SQueuing: Sync %v - %v", fwk.Key, fwk.QueuingTimestamp)
		}
		if nextWaiting == nil {
			nextWaiting = fwk
		}
		if fwk.QueuingTimestamp.Before(nextWaiting.QueuingTimestamp) {
			nextWaiting = fwk
		}
	}
	if nextWaiting != nil {
		s.AddFrameworkToWaiting(nextWaiting)
		s.DeleteFrameworkFromQueuing(nextWaiting.Key)
	}
	return nextWaiting
}

func (s *GlobalScheduler) ScheduleWaitingFramework(skdZone *SkdZone) *SkdFramework {
	var nextPending *SkdFramework = nil
	fwks := make([]*SkdFramework, 0)
	ReadLock(skdZone.lockOfWaiting)
	func() {
		defer ReadUnlock(skdZone.lockOfWaiting)
		for _, fwk := range skdZone.fmWaiting {
			fwks = append(fwks, fwk)
		}
	}()
	now := time.Now()
	for _, fwk := range fwks {
		if now.Sub(fwk.LastSync) > ci.TimeoutOfFrameworkSync {
			f, err := s.fLister.Frameworks(fwk.Namespace).Get(fwk.Name)
			if err != nil {
				if apiErrors.IsNotFound(err) {
					// framework does not exist, delete it from fmQueuing
					skdZone.DeleteFrameworkFromWaiting(fwk.Key)
				}
				continue
			} else if f.DeletionTimestamp != nil {
				// framework is being deleted, so delete it from fmQueuing
				skdZone.DeleteFrameworkFromWaiting(fwk.Key)
			}
			fwk.QueuingTimestamp = GetFrameworkQueuingTimestamp(f)
			fwk.LastSync = now
			log.Infof("SWaiting: Sync %v - %v", fwk.Key, fwk.QueuingTimestamp)
		}
		if nextPending == nil {
			nextPending = fwk
		} else if fwk.QueuingTimestamp.Before(nextPending.QueuingTimestamp) {
			nextPending = fwk
		}
	}
	if nextPending != nil {
		WriteLock(s.lockOfPending)
		func() {
			defer WriteUnlock(s.lockOfPending)
			s.fmPending[nextPending.Key] = nextPending
			skdZone.AllocResourcesForFramework(nextPending)
		}()
		WriteLock(skdZone.lockOfWaiting)
		func() {
			defer WriteUnlock(skdZone.lockOfWaiting)
			delete(skdZone.fmWaiting, nextPending.Key)
			skdZone.FreeReservedResourcesOfFramework(nextPending)
		}()
	}
	return nextPending
}

// It should not be invoked concurrently with the same key.
//
// Return error only for Platform Transient Error, so that the key
// can be enqueued again after rate limited delay.
// For Platform Permanent Error, it should be delivered by panic.
// For Framework Error, it should be delivered into Framework.Status.
func (c *FrameworkController) syncFramework(key string) (returnedErr error) {
	startTime := time.Now()
	logPfx := fmt.Sprintf("[%v]: syncFramework: ", key)
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

	localF, err := c.fLister.Frameworks(namespace).Get(name)
	if err != nil {
		if apiErrors.IsNotFound(err) {
			// GarbageCollectionController will handle the dependent object
			// deletion according to the ownerReferences.
			log.Infof(logPfx+
				"Skipped: Framework cannot be found in local cache: %v", err)
			c.deleteExpectedFrameworkStatusInfo(key)
			return nil
		} else {
			return fmt.Errorf(logPfx+
				"Failed: Framework cannot be got from local cache: %v", err)
		}
	} else {
		if localF.DeletionTimestamp != nil {
			// Skip syncFramework to avoid fighting with GarbageCollectionController,
			// because GarbageCollectionController may be deleting the dependent object.
			log.Infof(logPfx+
				"Skipped: Framework is deleting: Will be deleted at %v",
				localF.DeletionTimestamp)
			return nil
		} else {
			f := localF.DeepCopy()
			// From now on, f is a writable copy of the original local cached one, and
			// it may be different from the original one.

			expected, exists := c.getExpectedFrameworkStatusInfo(f.Key())
			if !exists {
				if f.Status != nil {
					// Recover f related things, since it is the first time we see it and
					// its Status is not nil.
					c.recoverFrameworkWorkItems(f)
				}

				// f.Status must be the same as the remote one, since it is the first
				// time we see it.
				c.updateExpectedFrameworkStatusInfo(f.Key(), f.Status, true)
			} else {
				// f.Status may be outdated, so override it with the expected one, to
				// ensure the Framework.Status is Monotonically Exposed.
				f.Status = expected.status

				// Ensure the expected Framework.Status is the same as the remote one
				// before sync.
				if !expected.remoteSynced {
					updateErr := c.updateRemoteFrameworkStatus(f)
					if updateErr != nil {
						return updateErr
					}
					c.updateExpectedFrameworkStatusInfo(f.Key(), f.Status, true)
				}
			}

			// At this point, f.Status is the same as the expected and remote
			// Framework.Status, so it is ready to sync against f.Spec and other
			// related objects.
			errs := []error{}
			remoteF := f.DeepCopy()

			syncErr := c.syncFrameworkStatus(f)
			errs = append(errs, syncErr)

			if !reflect.DeepEqual(remoteF.Status, f.Status) {
				// Always update the expected and remote Framework.Status even if sync
				// error, since f.Status should never be corrupted due to any Platform
				// Transient Error, so no need to rollback to the one before sync, and
				// no need to DeepCopy between f.Status and the expected one.
				updateErr := c.updateRemoteFrameworkStatus(f)
				errs = append(errs, updateErr)

				c.updateExpectedFrameworkStatusInfo(f.Key(), f.Status, updateErr == nil)
			} else {
				log.Infof(logPfx +
					"Skip to update the expected and remote Framework.Status since " +
					"they are unchanged")
			}

			return errorAgg.NewAggregate(errs)
		}
	}
}

// No need to recover the non-AddAfter items, because the Informer has already
// delivered the Add events for all recovered Frameworks which caused all
// Frameworks will be enqueued to sync.
func (c *FrameworkController) recoverFrameworkWorkItems(f *ci.Framework) {
	logPfx := fmt.Sprintf("[%v]: recoverFrameworkWorkItems: ", f.Key())
	log.Infof(logPfx + "Started")
	defer func() { log.Infof(logPfx + "Completed") }()

	if f.Status == nil {
		return
	}

	c.recoverTimeoutChecks(f)
}

func (c *FrameworkController) recoverTimeoutChecks(f *ci.Framework) {
	// If a check is already timeout, the timeout will be handled by the following
	// sync after the recover, so no need to enqueue it again.
	c.enqueueFrameworkAttemptCreationTimeoutCheck(f, true)
	c.enqueueFrameworkRetryDelayTimeoutCheck(f, true)
	for _, taskRoleStatus := range f.TaskRoleStatuses() {
		for _, taskStatus := range taskRoleStatus.TaskStatuses {
			taskRoleName := taskRoleStatus.Name
			taskIndex := taskStatus.Index
			c.enqueueTaskAttemptCreationTimeoutCheck(f, taskRoleName, taskIndex, true)
			c.enqueueTaskRetryDelayTimeoutCheck(f, taskRoleName, taskIndex, true)
		}
	}
}

func (c *FrameworkController) enqueueFrameworkAttemptCreationTimeoutCheck(
	f *ci.Framework, failIfTimeout bool) bool {
	if f.Status.State != ci.FrameworkAttemptCreationRequested {
		return false
	}

	leftDuration := common.CurrentLeftDuration(
		f.Status.TransitionTime,
		c.cConfig.ObjectLocalCacheCreationTimeoutSec)
	if common.IsTimeout(leftDuration) && failIfTimeout {
		return false
	}

	c.fQueue.AddAfter(f.Key(), leftDuration)
	log.Infof("[%v]: enqueueFrameworkAttemptCreationTimeoutCheck after %v",
		f.Key(), leftDuration)
	return true
}

func (c *FrameworkController) enqueueTaskAttemptCreationTimeoutCheck(
	f *ci.Framework, taskRoleName string, taskIndex int32,
	failIfTimeout bool) bool {
	taskStatus := f.TaskStatus(taskRoleName, taskIndex)
	if taskStatus.State != ci.TaskAttemptCreationRequested {
		return false
	}

	leftDuration := common.CurrentLeftDuration(
		taskStatus.TransitionTime,
		c.cConfig.ObjectLocalCacheCreationTimeoutSec)
	if common.IsTimeout(leftDuration) && failIfTimeout {
		return false
	}

	c.fQueue.AddAfter(f.Key(), leftDuration)
	log.Infof("[%v][%v][%v]: enqueueTaskAttemptCreationTimeoutCheck after %v",
		f.Key(), taskRoleName, taskIndex, leftDuration)
	return true
}

func (c *FrameworkController) enqueueFrameworkRetryDelayTimeoutCheck(
	f *ci.Framework, failIfTimeout bool) bool {
	if f.Status.State != ci.FrameworkAttemptCompleted {
		return false
	}

	leftDuration := common.CurrentLeftDuration(
		f.Status.TransitionTime,
		f.Status.RetryPolicyStatus.RetryDelaySec)
	if common.IsTimeout(leftDuration) && failIfTimeout {
		return false
	}

	c.fQueue.AddAfter(f.Key(), leftDuration)
	log.Infof("[%v]: enqueueFrameworkRetryDelayTimeoutCheck after %v",
		f.Key(), leftDuration)
	return true
}

func (c *FrameworkController) enqueueTaskRetryDelayTimeoutCheck(
	f *ci.Framework, taskRoleName string, taskIndex int32,
	failIfTimeout bool) bool {
	taskStatus := f.TaskStatus(taskRoleName, taskIndex)
	if taskStatus.State != ci.TaskAttemptCompleted {
		return false
	}

	leftDuration := common.CurrentLeftDuration(
		taskStatus.TransitionTime,
		taskStatus.RetryPolicyStatus.RetryDelaySec)
	if common.IsTimeout(leftDuration) && failIfTimeout {
		return false
	}

	c.fQueue.AddAfter(f.Key(), leftDuration)
	log.Infof("[%v][%v][%v]: enqueueTaskRetryDelayTimeoutCheck after %v",
		f.Key(), taskRoleName, taskIndex, leftDuration)
	return true
}

func (c *FrameworkController) enqueueFramework(f *ci.Framework, msg string) {
	c.fQueue.Add(f.Key())
	log.Infof("[%v]: enqueueFramework: %v", f.Key(), msg)
}

func (c *FrameworkController) syncFrameworkStatus(f *ci.Framework) error {
	logPfx := fmt.Sprintf("[%v]: syncFrameworkStatus: ", f.Key())
	log.Infof(logPfx + "Started")
	defer func() { log.Infof(logPfx + "Completed") }()

	if f.Status == nil {
		f.Status = f.NewFrameworkStatus()
	} else {
		// TODO: Support Framework.Spec Update
	}

	return c.syncFrameworkState(f)
}

func (c *FrameworkController) syncFrameworkState(f *ci.Framework) error {
	logPfx := fmt.Sprintf("[%v]: syncFrameworkState: ", f.Key())
	log.Infof(logPfx + "Started")
	defer func() { log.Infof(logPfx + "Completed") }()

	if f.Status.State == ci.FrameworkCompleted {
		log.Infof(logPfx + "Skipped: Framework is already completed")
		return nil
	}

	// Get the ground truth readonly cm
	cm, err := c.getOrCleanupConfigMap(f)
	if err != nil {
		return err
	}

	// Totally reconstruct FrameworkState in case Framework.Status is failed to
	// persist due to FrameworkController restart.
	if cm == nil {
		if f.ConfigMapUID() == nil {
			if f.Status.State != ci.FrameworkAttemptCreationWaiting &&
				f.Status.State != ci.FrameworkAttemptCreationQueuing {
				f.TransitionFrameworkState(ci.FrameworkAttemptCreationPending)
			}
		} else {
			// Avoid sync with outdated object:
			// cm is remote creation requested but not found in the local cache.
			if f.Status.State == ci.FrameworkAttemptCreationRequested {
				if c.enqueueFrameworkAttemptCreationTimeoutCheck(f, true) {
					log.Infof(logPfx +
						"Waiting ConfigMap to appear in the local cache or timeout")
					return nil
				}

				diag := fmt.Sprintf(
					"ConfigMap does not appear in the local cache within timeout %v, "+
						"so consider it was deleted and force delete it",
					common.SecToDuration(c.cConfig.ObjectLocalCacheCreationTimeoutSec))
				log.Warnf(logPfx + diag)

				// Ensure cm is deleted in remote to avoid managed cm leak after
				// FrameworkCompleted.
				err := c.deleteConfigMap(f, *f.ConfigMapUID())
				if err != nil {
					return err
				}

				f.Status.AttemptStatus.CompletionStatus =
					ci.CompletionCodeConfigMapCreationTimeout.NewCompletionStatus(diag)
			}

			if f.Status.State != ci.FrameworkAttemptCompleted {
				if f.Status.AttemptStatus.CompletionStatus == nil {
					diag := fmt.Sprintf("ConfigMap was deleted by others")
					log.Warnf(logPfx + diag)
					f.Status.AttemptStatus.CompletionStatus =
						ci.CompletionCodeConfigMapExternalDeleted.NewCompletionStatus(diag)
				}

				f.Status.AttemptStatus.CompletionTime = common.PtrNow()
				f.TransitionFrameworkState(ci.FrameworkAttemptCompleted)
				log.Infof(logPfx+
					"FrameworkAttemptInstance %v is completed with CompletionStatus: %v",
					*f.FrameworkAttemptInstanceUID(),
					f.Status.AttemptStatus.CompletionStatus)
			}
		}
	} else {
		if cm.DeletionTimestamp == nil {
			if f.Status.State == ci.FrameworkAttemptDeletionPending {
				// The CompletionStatus has been persisted, so it is safe to delete the
				// cm now.
				err := c.deleteConfigMap(f, *f.ConfigMapUID())
				if err != nil {
					return err
				}
				f.TransitionFrameworkState(ci.FrameworkAttemptDeletionRequested)
			}

			// Avoid sync with outdated object:
			// cm is remote deletion requested but not deleting or deleted in the local
			// cache.
			if f.Status.State == ci.FrameworkAttemptDeletionRequested {
				// The deletion requested object will never appear again with the same UID,
				// so always just wait.
				log.Infof(logPfx +
					"Waiting ConfigMap to disappearing or disappear in the local cache")
				return nil
			}

			if f.Status.State != ci.FrameworkAttemptPreparing &&
				f.Status.State != ci.FrameworkAttemptRunning {
				f.TransitionFrameworkState(ci.FrameworkAttemptPreparing)
			}
		} else {
			f.TransitionFrameworkState(ci.FrameworkAttemptDeleting)
			log.Infof(logPfx + "Waiting ConfigMap to be deleted")
			return nil
		}
	}
	// At this point, f.Status.State must be in:
	// {FrameworkAttemptCreationPending, FrameworkAttemptCompleted,
	// FrameworkAttemptPreparing, FrameworkAttemptRunning}

	if f.Status.State == ci.FrameworkAttemptCompleted {
		// attemptToRetryFramework
		retryDecision := f.Spec.RetryPolicy.ShouldRetry(
			f.Status.RetryPolicyStatus,
			f.Status.AttemptStatus.CompletionStatus.Type,
			*c.cConfig.FrameworkMinRetryDelaySecForTransientConflictFailed,
			*c.cConfig.FrameworkMaxRetryDelaySecForTransientConflictFailed)

		if f.Status.RetryPolicyStatus.RetryDelaySec == nil {
			// RetryFramework is not yet scheduled, so need to be decided.
			if retryDecision.ShouldRetry {
				// scheduleToRetryFramework
				log.Infof(logPfx+
					"Will retry Framework with new FrameworkAttempt: RetryDecision: %v",
					retryDecision)

				f.Status.RetryPolicyStatus.RetryDelaySec = &retryDecision.DelaySec
			} else {
				// completeFramework
				log.Infof(logPfx+
					"Will complete Framework: RetryDecision: %v",
					retryDecision)

				f.Status.CompletionTime = common.PtrNow()
				f.TransitionFrameworkState(ci.FrameworkCompleted)
				// Delete the framework from fmPending for safty
				c.scheduler.doneForPending(f.Key())
				return nil
			}
		}

		if f.Status.RetryPolicyStatus.RetryDelaySec != nil {
			// RetryFramework is already scheduled, so just need to check timeout.
			if c.enqueueFrameworkRetryDelayTimeoutCheck(f, true) {
				log.Infof(logPfx + "Waiting Framework to retry after delay")
				return nil
			}

			// retryFramework
			log.Infof(logPfx + "Retry Framework")
			f.Status.RetryPolicyStatus.TotalRetriedCount++
			if retryDecision.IsAccountable {
				f.Status.RetryPolicyStatus.AccountableRetriedCount++
			}
			f.Status.RetryPolicyStatus.RetryDelaySec = nil
			f.Status.AttemptStatus = f.NewFrameworkAttemptStatus(
				f.Status.RetryPolicyStatus.TotalRetriedCount)
			f.TransitionFrameworkState(ci.FrameworkAttemptCreationPending)
		}
	}
	// At this point, f.Status.State must be in:
	// {FrameworkAttemptCreationPending, FrameworkAttemptPreparing,
	// FrameworkAttemptRunning, FrameworkAttemptCreationWaiting/Queuing}

	if f.Status.State == ci.FrameworkAttemptCreationQueuing {
		if c.scheduler.checkForWaiting(f) {
			f.TransitionFrameworkState(ci.FrameworkAttemptCreationWaiting)
			log.Infof(logPfx+"Changing %v from Queuing to Waiting", f.Key())
		} else {
			if c.scheduler.addToQueuing(f) {
				log.Infof(logPfx+"Add framework %v to Queuing", f.Key())
			}
			c.scheduler.enqueueZonesForQueuing()
			return nil
		}
	}

	if f.Status.State == ci.FrameworkAttemptCreationWaiting {
		if c.scheduler.checkForPending(f) {
			f.TransitionFrameworkState(ci.FrameworkAttemptCreationPending)
		} else {
			c.scheduler.addToWaiting(f)
			log.Infof(logPfx+"Add framework %v to Waiting", f.Key())
			c.scheduler.enqueueZonesForWaiting()
			return nil
		}
	}

	// At this point, f.Status.State must be in:
	// {FrameworkAttemptCreationPending, FrameworkAttemptPreparing,
	// FrameworkAttemptRunning}

	if f.Status.State == ci.FrameworkAttemptCreationPending {
		// createFrameworkAttempt
		cm, err := c.createConfigMap(f)
		if err != nil {
			return err
		}

		f.Status.AttemptStatus.ConfigMapUID = &cm.UID
		f.Status.AttemptStatus.InstanceUID = ci.GetFrameworkAttemptInstanceUID(
			f.FrameworkAttemptID(), f.ConfigMapUID())
		f.TransitionFrameworkState(ci.FrameworkAttemptCreationRequested)

		// Informer may not deliver any event if a create is immediately followed by
		// a delete, so manually enqueue a sync to check the cm existence after the
		// timeout.
		c.enqueueFrameworkAttemptCreationTimeoutCheck(f, false)

		// The ground truth cm is the local cached one instead of the remote one,
		// so need to wait before continue the sync.
		log.Infof(logPfx +
			"Waiting ConfigMap to appear in the local cache or timeout")
		return nil
	}
	// At this point, f.Status.State must be in:
	// {FrameworkAttemptPreparing, FrameworkAttemptRunning}

	if f.Status.State == ci.FrameworkAttemptPreparing ||
		f.Status.State == ci.FrameworkAttemptRunning {
		cancelled, err := c.syncTaskRoleStatuses(f, cm)

		if !cancelled {
			if !f.IsAnyTaskRunning() {
				f.TransitionFrameworkState(ci.FrameworkAttemptPreparing)
			} else {
				f.TransitionFrameworkState(ci.FrameworkAttemptRunning)
			}
		}

		return err
	} else {
		// Unreachable
		panic(fmt.Errorf(logPfx+
			"Failed: At this point, FrameworkState should be in {%v, %v} instead of %v",
			ci.FrameworkAttemptPreparing, ci.FrameworkAttemptRunning, f.Status.State))
	}
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
func (s *GlobalScheduler) checkForWaiting(f *ci.Framework) bool {
	key := f.Key()
	ReadLock(s.lockOfWaiting)
	defer ReadUnlock(s.lockOfWaiting)
	_, ok := s.fmWaiting[key]
	return ok
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
		ZoneKey:          "",
		Zone:             nil,
		Resources:        resources,
		LastSync:         time.Now(),
	}
	log.Infof("NewSdkFramework %v %v/%v", f.Name, category, zone)
	return fwk
}

func (s *GlobalScheduler) addToQueuing(f *ci.Framework) bool {
	key := f.Key()
	fwk, ok := s.fmQueuing[key]
	if !ok {
		fwk := NewSdkFramework(f)
		s.AddFrameworkToQueuing(fwk)
		return true
	}
	// changing of preemption while changes QueuingTimestamp
	fwk.QueuingTimestamp = GetFrameworkQueuingTimestamp(f)
	// TODO: if resources will change ...
	return false
}

// Only frameworks in Waitting state will checkForPending
func (s *GlobalScheduler) checkForPending(f *ci.Framework) bool {
	key := f.Key()
	ReadLock(s.lockOfPending)
	defer ReadUnlock(s.lockOfPending)
	_, ok := s.fmPending[key]
	return ok
}

// Frameworks in Pending state will be removed from fmPending
func (s *GlobalScheduler) doneForPending(key string) {
	WriteLock(s.lockOfPending)
	defer WriteUnlock(s.lockOfPending)
	if fwk, ok := s.fmPending[key]; ok {
		fwk.Zone.FreeResourcesOfFramework(fwk)
		delete(s.fmPending, key)
	}
}

func (s *GlobalScheduler) addToWaiting(f *ci.Framework) error {
	key := f.Key()
	fwk, ok := s.lookupWaitingFramework(key)
	if !ok {
		fwk := NewSdkFramework(f)
		s.AddFrameworkToWaiting(fwk)
		return nil
	}

	// changing of preemption while changes QueuingTimestamp
	fwk.QueuingTimestamp = GetFrameworkQueuingTimestamp(f)
	// TODO: if resources will change ...

	// Assign framework to zone only when framework in Waiting state
	if fwk.Zone == nil && len(fwk.ZoneKey) > 0 {
		fwk.Zone = s.lookupZoneByKey(fwk.ZoneKey)
		if fwk.Zone != nil {
			s.bindFrameworkToZone(f, fwk)
		} else {
			// Unreachable
			log.Warnf("s.lookupZoneByKey(%v) return nil", fwk.ZoneKey)
		}
		s.enqueueZoneKey(fwk.ZoneKey)
	}
	return nil
}

func (s *GlobalScheduler) enqueueZonesForQueuing() {
	zoneList := s.RefreshZoneList()
	for _, skdZone := range zoneList {
		if skdZone.HasFreeProvision() {
			s.enqueueZoneKey(skdZone.Key)
		}
	}
}

func (s *GlobalScheduler) enqueueZonesForWaiting() {
	zoneList := s.RefreshZoneList()
	for _, skdZone := range zoneList {
		if skdZone.HasFreeResources() {
			s.enqueueZoneKey(skdZone.Key)
		}
	}
}

func (s *GlobalScheduler) bindFrameworkToZone(f *ci.Framework, fwk *SkdFramework) {
	// f.Annotations[ci.AnnotationKeyScheduleCategory] == skdZone.ScheduleCategory
	if f.Annotations == nil {
		f.Annotations = make(map[string]string)
	}
	f.Annotations[ci.AnnotationKeyScheduleZone] = fwk.Zone.ScheduleZone
}

func (s *GlobalScheduler) bindPodToZone(pod *core.Pod, f *ci.Framework) {
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

func (s *GlobalScheduler) lookupWaitingFramework(key string) (fwk *SkdFramework, ok bool) {
	ReadLock(s.lockOfWaiting)
	func() {
		defer ReadUnlock(s.lockOfWaiting)
		fwk, ok = s.fmWaiting[key]
	}()
	if ok {
		return fwk, ok
	}
	zoneList := s.RefreshZoneList()
	for _, skdZone := range zoneList {
		ReadLock(skdZone.lockOfWaiting)
		func() {
			defer ReadUnlock(skdZone.lockOfWaiting)
			fwk, ok = skdZone.fmWaiting[key]
		}()
		if ok {
			return fwk, ok
		}
	}
	return fwk, ok
}

func (s *GlobalScheduler) RefreshZoneList() []*SkdZone {
	if s.lastRefreshedZoneList.After(s.lastModifiedZone) {
		return s.zoneList
	}
	ReadLock(s.lockOfZones)
	func() {
		defer ReadUnlock(s.lockOfZones)
		list := make([]*SkdZone, 0, len(s.zones))
		for _, skdZone := range s.zones {
			list = append(list, skdZone)
		}
		s.zoneList = list
	}()
	s.lastRefreshedZoneList = time.Now().Add(ci.TimeoutOfRefreshZoneList)
	return s.zoneList
}

// Get Framework's current ConfigMap object, if not found, then clean up existing
// controlled ConfigMap if any.
// Returned cm is either managed or nil, if it is the managed cm, it is not
// writable and may be outdated even if no error.
// Clean up instead of recovery is because the ConfigMapUID is always the ground
// truth.
func (c *FrameworkController) getOrCleanupConfigMap(
	f *ci.Framework) (*core.ConfigMap, error) {
	cm, err := c.cmLister.ConfigMaps(f.Namespace).Get(f.ConfigMapName())
	if err != nil {
		if apiErrors.IsNotFound(err) {
			return nil, nil
		} else {
			return nil, fmt.Errorf(
				"[%v]: ConfigMap %v cannot be got from local cache: %v",
				f.Key(), f.ConfigMapName(), err)
		}
	}

	if f.ConfigMapUID() == nil || *f.ConfigMapUID() != cm.UID {
		// cm is the unmanaged
		if meta.IsControlledBy(cm, f) {
			// The managed ConfigMap becomes unmanaged if and only if Framework.Status
			// is failed to persist due to FrameworkController restart or create fails
			// but succeeds on remote, so clean up the ConfigMap to avoid unmanaged cm
			// leak.
			return nil, c.deleteConfigMap(f, cm.UID)
		} else {
			return nil, fmt.Errorf(
				"[%v]: ConfigMap %v naming conflicts with others: "+
					"Existing ConfigMap %v with DeletionTimestamp %v is not "+
					"controlled by current Framework %v, %v",
				f.Key(), f.ConfigMapName(),
				cm.UID, cm.DeletionTimestamp, f.Name, f.UID)
		}
	} else {
		// cm is the managed
		return cm, nil
	}
}

// Using UID to ensure we delete the right object.
func (c *FrameworkController) deleteConfigMap(
	f *ci.Framework, cmUID types.UID) error {
	cmName := f.ConfigMapName()
	err := c.kClient.CoreV1().ConfigMaps(f.Namespace).Delete(cmName,
		&meta.DeleteOptions{Preconditions: &meta.Preconditions{UID: &cmUID}})
	if err != nil && !apiErrors.IsNotFound(err) {
		return fmt.Errorf("[%v]: Failed to delete ConfigMap %v, %v: %v",
			f.Key(), cmName, cmUID, err)
	} else {
		log.Infof("[%v]: Succeeded to delete ConfigMap %v, %v",
			f.Key(), cmName, cmUID)
		return nil
	}
}

func (c *FrameworkController) createConfigMap(
	f *ci.Framework) (*core.ConfigMap, error) {
	cm := f.NewConfigMap()
	remoteCM, err := c.kClient.CoreV1().ConfigMaps(f.Namespace).Create(cm)
	if err != nil {
		return nil, fmt.Errorf("[%v]: Failed to create ConfigMap %v: %v",
			f.Key(), cm.Name, err)
	} else {
		log.Infof("[%v]: Succeeded to create ConfigMap %v",
			f.Key(), cm.Name)
		return remoteCM, nil
	}
}

func (c *FrameworkController) syncTaskRoleStatuses(
	f *ci.Framework, cm *core.ConfigMap) (syncFrameworkCancelled bool, err error) {
	logPfx := fmt.Sprintf("[%v]: syncTaskRoleStatuses: ", f.Key())
	log.Infof(logPfx + "Started")
	defer func() { log.Infof(logPfx + "Completed") }()

	errs := []error{}
	for _, taskRoleStatus := range f.TaskRoleStatuses() {
		log.Infof("[%v][%v]: syncTaskRoleStatus", f.Key(), taskRoleStatus.Name)
		for _, taskStatus := range taskRoleStatus.TaskStatuses {
			cancelled, err := c.syncTaskState(f, cm, taskRoleStatus.Name, taskStatus.Index)
			if err != nil {
				errs = append(errs, err)
			}

			if cancelled {
				log.Infof(
					"[%v][%v][%v]: syncFramework is cancelled",
					f.Key(), taskRoleStatus.Name, taskStatus.Index)
				return true, errorAgg.NewAggregate(errs)
			}

			if err != nil {
				// The Tasks in the TaskRole have the same Spec except for the PodName,
				// so in most cases, same Platform Transient Error will return.
				log.Warnf(
					"[%v][%v][%v]: Failed to sync Task, "+
						"skip to sync the Tasks behind it in the TaskRole: %v",
					f.Key(), taskRoleStatus.Name, taskStatus.Index, err)
				break
			}
		}
	}

	return false, errorAgg.NewAggregate(errs)
}

func (c *FrameworkController) syncTaskState(
	f *ci.Framework, cm *core.ConfigMap,
	taskRoleName string, taskIndex int32) (syncFrameworkCancelled bool, err error) {
	logPfx := fmt.Sprintf("[%v][%v][%v]: syncTaskState: ",
		f.Key(), taskRoleName, taskIndex)
	log.Infof(logPfx + "Started")
	defer func() { log.Infof(logPfx + "Completed") }()

	taskRoleSpec := f.TaskRoleSpec(taskRoleName)
	taskSpec := taskRoleSpec.Task
	taskRoleStatus := f.TaskRoleStatus(taskRoleName)
	taskStatus := f.TaskStatus(taskRoleName, taskIndex)

	if taskStatus.State == ci.TaskCompleted {
		// The TaskCompleted should not trigger FrameworkAttemptDeletionPending, so
		// it is safe to skip the attemptToCompleteFrameworkAttempt.
		// Otherwise, given it is impossible that the TaskCompleted is persisted
		// but the FrameworkAttemptDeletionPending is not persisted, the TaskCompleted
		// should have already triggered and persisted FrameworkAttemptDeletionPending
		// in previous sync, so current sync should have already been skipped but not.
		log.Infof(logPfx + "Skipped: Task is already completed")
		return false, nil
	}

	// Get the ground truth readonly pod
	pod, err := c.getOrCleanupPod(f, cm, taskRoleName, taskIndex)
	if err != nil {
		return false, err
	}

	// Totally reconstruct TaskState in case Framework.Status is failed to persist
	// due to FrameworkController restart.
	if pod == nil {
		if taskStatus.PodUID() == nil {
			f.TransitionTaskState(taskRoleName, taskIndex, ci.TaskAttemptCreationPending)
		} else {
			// Avoid sync with outdated object:
			// pod is remote creation requested but not found in the local cache.
			if taskStatus.State == ci.TaskAttemptCreationRequested {
				if c.enqueueTaskAttemptCreationTimeoutCheck(f, taskRoleName, taskIndex, true) {
					log.Infof(logPfx +
						"Waiting Pod to appear in the local cache or timeout")
					return false, nil
				}

				diag := fmt.Sprintf(
					"Pod does not appear in the local cache within timeout %v, "+
						"so consider it was deleted and force delete it",
					common.SecToDuration(c.cConfig.ObjectLocalCacheCreationTimeoutSec))
				log.Warnf(logPfx + diag)

				// Ensure pod is deleted in remote to avoid managed pod leak after
				// TaskCompleted.
				err := c.deletePod(f, taskRoleName, taskIndex, *taskStatus.PodUID())
				if err != nil {
					return false, err
				}

				taskStatus.AttemptStatus.CompletionStatus =
					ci.CompletionCodePodCreationTimeout.NewCompletionStatus(diag)
			}

			if taskStatus.State != ci.TaskAttemptCompleted {
				if taskStatus.AttemptStatus.CompletionStatus == nil {
					diag := fmt.Sprintf("Pod was deleted by others")
					log.Warnf(logPfx + diag)
					taskStatus.AttemptStatus.CompletionStatus =
						ci.CompletionCodePodExternalDeleted.NewCompletionStatus(diag)
				}

				taskStatus.AttemptStatus.CompletionTime = common.PtrNow()
				f.TransitionTaskState(taskRoleName, taskIndex, ci.TaskAttemptCompleted)
				log.Infof(logPfx+
					"TaskAttemptInstance %v is completed with CompletionStatus: %v",
					*taskStatus.TaskAttemptInstanceUID(),
					taskStatus.AttemptStatus.CompletionStatus)
			}
		}
	} else {
		if pod.DeletionTimestamp == nil {
			if taskStatus.State == ci.TaskAttemptDeletionPending {
				// The CompletionStatus has been persisted, so it is safe to delete the
				// pod now.
				err := c.deletePod(f, taskRoleName, taskIndex, *taskStatus.PodUID())
				if err != nil {
					return false, err
				}
				f.TransitionTaskState(taskRoleName, taskIndex, ci.TaskAttemptDeletionRequested)
			}

			// Avoid sync with outdated object:
			// pod is remote deletion requested but not deleting or deleted in the local
			// cache.
			if taskStatus.State == ci.TaskAttemptDeletionRequested {
				// The deletion requested object will never appear again with the same UID,
				// so always just wait.
				log.Infof(logPfx +
					"Waiting Pod to disappearing or disappear in the local cache")
				return false, nil
			}

			// Possibly due to the NodeController has not heard from the kubelet who
			// manages the Pod for more than node-monitor-grace-period but less than
			// pod-eviction-timeout.
			// And after pod-eviction-timeout, the Pod will be marked as deleting, but
			// it will only be automatically deleted after the kubelet comes back and
			// kills the Pod.
			if pod.Status.Phase == core.PodUnknown {
				log.Infof(logPfx+
					"Waiting Pod to be deleted or deleting or transitioned from %v",
					pod.Status.Phase)
				return false, nil
			}

			// Below Pod fields may be available even when PodPending, such as the Pod
			// has been bound to a Node, but one or more Containers has not been started.
			taskStatus.AttemptStatus.PodIP = &pod.Status.PodIP
			taskStatus.AttemptStatus.PodHostIP = &pod.Status.HostIP

			if pod.Status.Phase == core.PodPending {
				f.TransitionTaskState(taskRoleName, taskIndex, ci.TaskAttemptPreparing)
				return false, nil
			} else if pod.Status.Phase == core.PodRunning {
				f.TransitionTaskState(taskRoleName, taskIndex, ci.TaskAttemptRunning)
				return false, nil
			} else if pod.Status.Phase == core.PodSucceeded {
				diag := fmt.Sprintf("Pod succeeded")
				log.Infof(logPfx + diag)
				c.completeTaskAttempt(f, taskRoleName, taskIndex,
					ci.CompletionCodeSucceeded.NewCompletionStatus(diag))
				return false, nil
			} else if pod.Status.Phase == core.PodFailed {
				// All Container names in a Pod must be different, so we can still identify
				// a Container even after the InitContainers is merged with the AppContainers.
				allContainerStatuses := append(
					pod.Status.InitContainerStatuses,
					pod.Status.ContainerStatuses...)

				lastContainerExitCode := common.NilInt32()
				lastContainerCompletionTime := time.Time{}
				allContainerDiags := []string{}
				for _, containerStatus := range allContainerStatuses {
					terminated := containerStatus.State.Terminated
					if terminated != nil && terminated.ExitCode != 0 {
						allContainerDiags = append(allContainerDiags, fmt.Sprintf(
							"[Container %v, ExitCode: %v, Reason: %v, Message: %v]",
							containerStatus.Name, terminated.ExitCode, terminated.Reason,
							terminated.Message))

						if lastContainerExitCode == nil ||
							lastContainerCompletionTime.Before(terminated.FinishedAt.Time) {
							lastContainerExitCode = &terminated.ExitCode
							lastContainerCompletionTime = terminated.FinishedAt.Time
						}
					}
				}

				if lastContainerExitCode == nil {
					diag := fmt.Sprintf(
						"Pod failed without any non-zero container exit code, maybe " +
							"stopped by the system")
					log.Warnf(logPfx + diag)
					c.completeTaskAttempt(f, taskRoleName, taskIndex,
						ci.CompletionCodePodFailedWithoutFailedContainer.NewCompletionStatus(diag))
				} else {
					diag := fmt.Sprintf(
						"Pod failed with non-zero container exit code: %v",
						strings.Join(allContainerDiags, ", "))
					log.Infof(logPfx + diag)
					if strings.Contains(diag, string(ci.ReasonOOMKilled)) {
						c.completeTaskAttempt(f, taskRoleName, taskIndex,
							ci.CompletionCodeContainerOOMKilled.NewCompletionStatus(diag))
					} else {
						c.completeTaskAttempt(f, taskRoleName, taskIndex,
							ci.CompletionCode(*lastContainerExitCode).NewCompletionStatus(diag))
					}
				}
				return false, nil
			} else {
				return false, fmt.Errorf(logPfx+
					"Failed: Got unrecognized Pod Phase: %v", pod.Status.Phase)
			}
		} else {
			f.TransitionTaskState(taskRoleName, taskIndex, ci.TaskAttemptDeleting)
			log.Infof(logPfx + "Waiting Pod to be deleted")
			return false, nil
		}
	}
	// At this point, taskStatus.State must be in:
	// {TaskAttemptCreationPending, TaskAttemptCompleted}

	if taskStatus.State == ci.TaskAttemptCompleted {
		// attemptToRetryTask
		retryDecision := taskSpec.RetryPolicy.ShouldRetry(
			taskStatus.RetryPolicyStatus,
			taskStatus.AttemptStatus.CompletionStatus.Type,
			0, 0)

		if taskStatus.RetryPolicyStatus.RetryDelaySec == nil {
			// RetryTask is not yet scheduled, so need to be decided.
			if retryDecision.ShouldRetry {
				// scheduleToRetryTask
				log.Infof(logPfx+
					"Will retry Task with new TaskAttempt: RetryDecision: %v",
					retryDecision)

				taskStatus.RetryPolicyStatus.RetryDelaySec = &retryDecision.DelaySec
			} else {
				// completeTask
				log.Infof(logPfx+
					"Will complete Task: RetryDecision: %v",
					retryDecision)

				taskStatus.CompletionTime = common.PtrNow()
				f.TransitionTaskState(taskRoleName, taskIndex, ci.TaskCompleted)
			}
		}

		if taskStatus.RetryPolicyStatus.RetryDelaySec != nil {
			// RetryTask is already scheduled, so just need to check timeout.
			if c.enqueueTaskRetryDelayTimeoutCheck(f, taskRoleName, taskIndex, true) {
				log.Infof(logPfx + "Waiting Task to retry after delay")
				return false, nil
			}

			// retryTask
			log.Infof(logPfx + "Retry Task")
			taskStatus.RetryPolicyStatus.TotalRetriedCount++
			if retryDecision.IsAccountable {
				taskStatus.RetryPolicyStatus.AccountableRetriedCount++
			}
			taskStatus.RetryPolicyStatus.RetryDelaySec = nil
			taskStatus.AttemptStatus = f.NewTaskAttemptStatus(
				taskRoleName, taskIndex, taskStatus.RetryPolicyStatus.TotalRetriedCount)
			f.TransitionTaskState(taskRoleName, taskIndex, ci.TaskAttemptCreationPending)
		}
	}
	// At this point, taskStatus.State must be in:
	// {TaskCompleted, TaskAttemptCreationPending}

	if taskStatus.State == ci.TaskCompleted {
		// attemptToCompleteFrameworkAttempt
		completionPolicy := taskRoleSpec.FrameworkAttemptCompletionPolicy
		minFailedTaskCount := completionPolicy.MinFailedTaskCount
		minSucceededTaskCount := completionPolicy.MinSucceededTaskCount

		if taskStatus.IsFailed() && minFailedTaskCount != ci.UnlimitedValue {
			failedTaskCount := taskRoleStatus.GetTaskCount((*ci.TaskStatus).IsFailed)
			if failedTaskCount >= minFailedTaskCount {
				diag := fmt.Sprintf(
					"FailedTaskCount %v has reached MinFailedTaskCount %v in TaskRole [%v]: "+
						"Triggered by Task [%v][%v]: Diagnostics: %v",
					failedTaskCount, minFailedTaskCount, taskRoleName,
					taskRoleName, taskIndex, taskStatus.AttemptStatus.CompletionStatus.Diagnostics)
				log.Infof(logPfx + diag)
				c.completeFrameworkAttempt(f,
					taskStatus.AttemptStatus.CompletionStatus.Code.NewCompletionStatus(diag))
				return true, nil
			}
		}

		if taskStatus.IsSucceeded() && minSucceededTaskCount != ci.UnlimitedValue {
			succeededTaskCount := taskRoleStatus.GetTaskCount((*ci.TaskStatus).IsSucceeded)
			if succeededTaskCount >= minSucceededTaskCount {
				diag := fmt.Sprintf(
					"SucceededTaskCount %v has reached MinSucceededTaskCount %v in TaskRole [%v]: "+
						"Triggered by Task [%v][%v]: Diagnostics: %v",
					succeededTaskCount, minSucceededTaskCount, taskRoleName,
					taskRoleName, taskIndex, taskStatus.AttemptStatus.CompletionStatus.Diagnostics)
				log.Infof(logPfx + diag)
				c.completeFrameworkAttempt(f,
					ci.CompletionCodeSucceeded.NewCompletionStatus(diag))
				return true, nil
			}
		}

		if f.AreAllTasksCompleted() {
			totalTaskCount := f.GetTaskCount(nil)
			failedTaskCount := f.GetTaskCount((*ci.TaskStatus).IsFailed)
			diag := fmt.Sprintf(
				"All Tasks are completed and no user specified conditions in "+
					"FrameworkAttemptCompletionPolicy have ever been triggered: "+
					"TotalTaskCount: %v, FailedTaskCount: %v: "+
					"Triggered by Task [%v][%v]: Diagnostics: %v",
				totalTaskCount, failedTaskCount,
				taskRoleName, taskIndex, taskStatus.AttemptStatus.CompletionStatus.Diagnostics)
			log.Infof(logPfx + diag)
			c.completeFrameworkAttempt(f,
				ci.CompletionCodeSucceeded.NewCompletionStatus(diag))
			return true, nil
		}

		return false, nil
	}
	// At this point, taskStatus.State must be in:
	// {TaskAttemptCreationPending}

	if taskStatus.State == ci.TaskAttemptCreationPending {
		// createTaskAttempt
		pod, err := c.createPod(f, cm, taskRoleName, taskIndex)
		if err != nil {
			apiErr := errorWrap.Cause(err)
			if apiErrors.IsInvalid(apiErr) {
				// Should be Framework Error instead of Platform Transient Error.
				// Directly complete the FrameworkAttempt, since we should not complete
				// a TaskAttempt without an associated Pod in any case.
				diag := fmt.Sprintf(
					"Pod Spec is invalid in TaskRole [%v]: "+
						"Triggered by Task [%v][%v]: Diagnostics: %v",
					taskRoleName, taskRoleName, taskIndex, apiErr)
				log.Infof(logPfx + diag)
				c.completeFrameworkAttempt(f,
					ci.CompletionCodePodSpecInvalid.NewCompletionStatus(diag))
				return true, nil
			} else {
				return false, err
			}
		}

		taskStatus.AttemptStatus.PodUID = &pod.UID
		taskStatus.AttemptStatus.InstanceUID = ci.GetTaskAttemptInstanceUID(
			taskStatus.TaskAttemptID(), taskStatus.PodUID())
		f.TransitionTaskState(taskRoleName, taskIndex, ci.TaskAttemptCreationRequested)

		// Informer may not deliver any event if a create is immediately followed by
		// a delete, so manually enqueue a sync to check the pod existence after the
		// timeout.
		c.enqueueTaskAttemptCreationTimeoutCheck(f, taskRoleName, taskIndex, false)

		// The ground truth pod is the local cached one instead of the remote one,
		// so need to wait before continue the sync.
		log.Infof(logPfx +
			"Waiting Pod to appear in the local cache or timeout")
		return false, nil
	}
	// At this point, taskStatus.State must be in:
	// {}

	// Unreachable
	panic(fmt.Errorf(logPfx+
		"Failed: At this point, TaskState should be in {} instead of %v",
		taskStatus.State))
}

// Get Task's current Pod object, if not found, then clean up existing
// controlled Pod if any.
// Returned pod is either managed or nil, if it is the managed pod, it is not
// writable and may be outdated even if no error.
// Clean up instead of recovery is because the PodUID is always the ground truth.
func (c *FrameworkController) getOrCleanupPod(
	f *ci.Framework, cm *core.ConfigMap,
	taskRoleName string, taskIndex int32) (*core.Pod, error) {
	taskStatus := f.TaskStatus(taskRoleName, taskIndex)
	pod, err := c.podLister.Pods(f.Namespace).Get(taskStatus.PodName())
	if err != nil {
		if apiErrors.IsNotFound(err) {
			return nil, nil
		} else {
			return nil, fmt.Errorf(
				"[%v][%v][%v]: Pod %v cannot be got from local cache: %v",
				f.Key(), taskRoleName, taskIndex, taskStatus.PodName(), err)
		}
	}

	if taskStatus.PodUID() == nil || *taskStatus.PodUID() != pod.UID {
		// pod is the unmanaged
		if meta.IsControlledBy(pod, cm) {
			// The managed Pod becomes unmanaged if and only if Framework.Status
			// is failed to persist due to FrameworkController restart or create fails
			// but succeeds on remote, so clean up the Pod to avoid unmanaged pod leak.
			return nil, c.deletePod(f, taskRoleName, taskIndex, pod.UID)
		} else {
			return nil, fmt.Errorf(
				"[%v][%v][%v]: Pod %v naming conflicts with others: "+
					"Existing Pod %v with DeletionTimestamp %v is not "+
					"controlled by current ConfigMap %v, %v",
				f.Key(), taskRoleName, taskIndex, taskStatus.PodName(),
				pod.UID, pod.DeletionTimestamp, cm.Name, cm.UID)
		}
	} else {
		// pod is the managed
		return pod, nil
	}
}

// Using UID to ensure we delete the right object.
func (c *FrameworkController) deletePod(
	f *ci.Framework, taskRoleName string, taskIndex int32,
	podUID types.UID) error {
	taskStatus := f.TaskStatus(taskRoleName, taskIndex)
	podName := taskStatus.PodName()
	err := c.kClient.CoreV1().Pods(f.Namespace).Delete(podName,
		&meta.DeleteOptions{Preconditions: &meta.Preconditions{UID: &podUID}})
	if err != nil && !apiErrors.IsNotFound(err) {
		return fmt.Errorf("[%v][%v][%v]: Failed to delete Pod %v, %v: %v",
			f.Key(), taskRoleName, taskIndex, podName, podUID, err)
	} else {
		log.Infof("[%v][%v][%v]: Succeeded to delete Pod %v, %v",
			f.Key(), taskRoleName, taskIndex, podName, podUID)
		return nil
	}
}

func (c *FrameworkController) createPod(
	f *ci.Framework, cm *core.ConfigMap,
	taskRoleName string, taskIndex int32) (*core.Pod, error) {
	pod := f.NewPod(cm, taskRoleName, taskIndex)
	c.scheduler.bindPodToZone(pod, f)
	remotePod, err := c.kClient.CoreV1().Pods(f.Namespace).Create(pod)
	if err != nil {
		return nil, errorWrap.Wrapf(err,
			"[%v]: Failed to create Pod %v", f.Key(), pod.Name)
	} else {
		log.Infof("[%v]: Succeeded to create Pod: %v", f.Key(), pod.Name)
		return remotePod, nil
	}
}

func (c *FrameworkController) completeTaskAttempt(
	f *ci.Framework, taskRoleName string, taskIndex int32,
	completionStatus *ci.CompletionStatus) {
	logPfx := fmt.Sprintf("[%v][%v][%v]: completeTaskAttempt: ",
		f.Key(), taskRoleName, taskIndex)

	taskStatus := f.TaskStatus(taskRoleName, taskIndex)
	taskStatus.AttemptStatus.CompletionStatus = completionStatus
	f.TransitionTaskState(taskRoleName, taskIndex, ci.TaskAttemptDeletionPending)

	// To ensure the CompletionStatus is persisted before deleting the pod,
	// we need to wait until next sync to delete the pod, so manually enqueue
	// a sync.
	c.enqueueFramework(f, "TaskAttemptDeletionPending")
	log.Infof(logPfx + "Waiting TaskAttempt CompletionStatus to be persisted")
}

func (c *FrameworkController) completeFrameworkAttempt(
	f *ci.Framework, completionStatus *ci.CompletionStatus) {
	logPfx := fmt.Sprintf("[%v]: completeFrameworkAttempt: ", f.Key())

	f.Status.AttemptStatus.CompletionStatus = completionStatus
	f.TransitionFrameworkState(ci.FrameworkAttemptDeletionPending)

	// To ensure the CompletionStatus is persisted before deleting the cm,
	// we need to wait until next sync to delete the cm, so manually enqueue
	// a sync.
	c.enqueueFramework(f, "FrameworkAttemptDeletionPending")
	log.Infof(logPfx + "Waiting FrameworkAttempt CompletionStatus to be persisted")
}

func (c *FrameworkController) updateRemoteFrameworkStatus(f *ci.Framework) error {
	logPfx := fmt.Sprintf("[%v]: updateRemoteFrameworkStatus: ", f.Key())
	log.Infof(logPfx + "Started")
	defer func() { log.Infof(logPfx + "Completed") }()

	updateF := f
	updateErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		updateF.Status = f.Status
		_, updateErr := c.fClient.FrameworkcontrollerV1().Frameworks(updateF.Namespace).Update(updateF)
		if updateErr == nil {
			return nil
		}

		// Try to resolve conflict by patching more recent object.
		localF, getErr := c.fLister.Frameworks(updateF.Namespace).Get(updateF.Name)
		if getErr != nil {
			if apiErrors.IsNotFound(getErr) {
				return fmt.Errorf("Framework cannot be found in local cache: %v", getErr)
			} else {
				log.Warnf(logPfx+"Framework cannot be got from local cache: %v", getErr)
			}
		} else {
			// Only resolve conflict for the same object to avoid updating another
			// object of the same name.
			if f.UID != localF.UID {
				return fmt.Errorf(
					"Framework UID mismatch: Current UID %v, Local Cached UID %v",
					f.UID, localF.UID)
			} else {
				updateF = localF.DeepCopy()
			}
		}

		return updateErr
	})

	if updateErr != nil {
		// Will still be requeued and retried after rate limited delay.
		return fmt.Errorf(logPfx+"Failed: %v", updateErr)
	} else {
		return nil
	}
}

func (c *FrameworkController) getExpectedFrameworkStatusInfo(key string) (
	*ExpectedFrameworkStatusInfo, bool) {
	value, exists := c.fExpectedStatusInfos[key]
	return value, exists
}

func (c *FrameworkController) deleteExpectedFrameworkStatusInfo(key string) {
	log.Infof("[%v]: deleteExpectedFrameworkStatusInfo: ", key)
	delete(c.fExpectedStatusInfos, key)
	// Cleanup Queuing, Waiting, and Pending framework in scheduler
	c.scheduler.DeleteFrameworkFromQueuing(key)
	c.scheduler.DeleteFrameworkFromWaiting(key)
	c.scheduler.doneForPending(key)
}

func (c *FrameworkController) updateExpectedFrameworkStatusInfo(key string,
	status *ci.FrameworkStatus, remoteSynced bool) {
	log.Infof("[%v]: updateExpectedFrameworkStatusInfo", key)
	c.fExpectedStatusInfos[key] = &ExpectedFrameworkStatusInfo{
		status:       status,
		remoteSynced: remoteSynced,
	}
}
