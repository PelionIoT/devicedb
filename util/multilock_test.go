package util_test

//
// Copyright (c) 2019 ARM Limited.
//
// SPDX-License-Identifier: MIT
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to
// deal in the Software without restriction, including without limitation the
// rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
// sell copies of the Software, and to permit persons to whom the Software is
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
// SOFTWARE.
//

import (
	"sync"

	. "github.com/PelionIoT/devicedb/util"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Multilock", func() {
	Describe("lock and unlock", func() {
		It("should serialize goroutines locking with the same partitioning key but allow others to run in parallel", func() {
			var wg sync.WaitGroup

			countA := 0
			countB := 0

			multiLock := NewMultiLock()
			multiLock.Lock([]byte("AAA"))

			go func() {
				multiLock.Lock([]byte("AAA"))

				countA += 1
			}()

			wg.Add(1)

			go func() {
				multiLock.Lock([]byte("BBB"))

				for i := 0; i < 1000000; i += 1 {
					countB += 1
				}

				multiLock.Unlock([]byte("BBB"))
				wg.Done()
			}()

			wg.Wait()

			Expect(countA).Should(Equal(0))
			Expect(countB).Should(Equal(1000000))
		})
	})
})
