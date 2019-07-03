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
	//frameworkLister "github.com/microsoft/frameworkcontroller/pkg/client/listers/frameworkcontroller/v1"
	"github.com/microsoft/frameworkcontroller/pkg/common"
	"github.com/microsoft/frameworkcontroller/pkg/util"
	//errorWrap "github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	//core "k8s.io/api/core/v1"
	//apiErrors "k8s.io/apimachinery/pkg/api/errors"
	//meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	//"k8s.io/apimachinery/pkg/types"
	//errorAgg "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	//kubeInformer "k8s.io/client-go/informers"
	//kubeClient "k8s.io/client-go/kubernetes"
	//coreLister "k8s.io/client-go/listers/core/v1"
	//"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	//"k8s.io/client-go/util/retry"
	"k8s.io/client-go/util/workqueue"
	//"reflect"
	//"sort"
	//"strconv"
	//"strings"
	//"sync"
	"time"
)

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
