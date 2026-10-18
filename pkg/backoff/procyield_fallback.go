//go:build !amd64 && !arm64

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

package backoff

// procYield is a fallback for architectures that don't have
// dedicated assembly implementations.
func procYield(cycles int) {
	// A simple, cheap volatile-like operation or a simple loop
	// to waste a few cycles without eating the entire pipeline.
	for i := 0; i < cycles; i++ {
		// On generic architectures, we just let the loop run.
		// It's not as power-efficient as a hardware PAUSE,
		// but it ensures your code still compiles and runs anywhere.
	}
}
