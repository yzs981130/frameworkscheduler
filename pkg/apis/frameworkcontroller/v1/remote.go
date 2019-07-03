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

package v1

import ()

///////////////////////////////////////////////////////////////////////////////////////
// Remote Constants
///////////////////////////////////////////////////////////////////////////////////////
const (
	RemoteEnabled  string = "true"
	RemoteDisabled string = "false"
	RemoteEmpty    string = ""
	RemoteDinied   string = "dinied"

	AnnotationKeyScheduleRemotable string = "openi.cn/schedule-remotable"

	LabelKeyScheduleRemotable string = AnnotationKeyScheduleRemotable
	LabelKeyScheduleRemoted   string = "openi.cn/schedule-remoted"
	LabelKeyRemoteRequest     string = "openi.cn/remote-request"
	LabelKeyRemoteResponse    string = "openi.cn/remote-response"
)

type RemoteState string

const (
	// [InitialState]
	// -> RemoteWaking
	RemoteHibernated RemoteState = ""

	// Ready to start remote scheduling
	// -> RemoteRequestAllowed
	RemoteWaking RemoteState = "Waking"

	// Allow Remote Site to send request for the framework
	// [StartState]
	// -> RemoteRequestRecieved
	// -> RemoteHibernated
	RemoteRequestAllowed RemoteState = "RequestAllowed"

	// A request for the framework has been recieved
	// -> RemoteRequestAccepted
	// -> RemoteHibernated
	RemoteRequestRecieved RemoteState = "RequestRecieved"

	// Accept a request for the framework
	// -> RemoteFrameworkCloning
	// -> RemoteFrameworkFailed
	// -> RemoteFrameworkCompleted
	RemoteRequestAccepted RemoteState = "RequestAccepted"
	RemoteRequestDenied   RemoteState = "RequestDenied"
	RemoteRequestCanceled RemoteState = "RequestCanceled"

	// Remote site is cloning the framework
	// -> RemoteFrameworkFailed
	// -> RemoteFrameworkCompleted
	RemoteFrameworkCloning RemoteState = "FrameworkCloning"

	RemoteFrameworkFailed RemoteState = "FrameworkFailed"

	RemoteFrameworkCompleted RemoteState = "FrameworkCompleted"
)
