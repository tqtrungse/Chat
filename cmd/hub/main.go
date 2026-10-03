/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import "xxx/cmd/hub/boostrap"

func main() {
	boostrap.Run()

	//logger := log.NewLogger(nil)
	//framePool := pool.NewFramePool(200)
	//workerPool := pool.NewWorkerPool(
	//	"worker-pool",
	//	10*runtime.GOMAXPROCS(0),
	//	pool.Config{
	//		ScaleThreshold: 1,
	//	},
	//)
	//workerPool.SetPanicHandler(func(ctx context.Context, r any) {
	//	logger.Panic(
	//		log.TraceID(0),
	//		"panic in pool",
	//		zap.String("pool_name", workerPool.Name()),
	//		zap.Any("panic_reason", r),
	//		zap.Stack("stack_trace"),
	//	)
	//})
	//
	//inactiveSessions := infraSession.NewInactiveSessions(5)
	//secureChannel := infraCrypto.NewAesGcmChannel(framePool)
	//
	//connActivator := usecase.NewConnActivator(
	//	workerPool,
	//	inactiveSessions,
	//	secureChannel,
	//)
	//
	//connRouter := usecase.NewConnListener(
	//	connActivator,
	//	nil,
	//)
	//
	//server := tcp.NewServer(
	//	&tcp.Config{
	//		Addr:            "tcp://:9000",
	//		Multicore:       true,
	//		NumEventLoop:    runtime.GOMAXPROCS(0),
	//		TCPKeepAlive:    30 * time.Second,
	//		TCPKeepInterval: 5 * time.Second,
	//		TCPKeepCount:    3,
	//	},
	//	connRouter,
	//)
	//
	//err := server.Run()
	//if err != nil {
	//	panic(err)
	//}
}
