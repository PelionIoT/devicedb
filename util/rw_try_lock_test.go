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
	"time"

	. "github.com/PelionIoT/devicedb/util"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("RwTryLock", func() {
	Describe("#TryRLock", func() {
		Context("When a call to WLock() is blocked", func() {
			It("Should return false", func() {
				var lock RWTryLock

				lock.TryRLock()

				writeLockSucceeded := make(chan int)

				go func() {
					lock.WLock()
					writeLockSucceeded <- 1
				}()

				go func() {
					<-time.After(time.Millisecond * 100)
					Expect(lock.TryRLock()).Should(BeFalse())
				}()

				select {
				case <-writeLockSucceeded:
					Fail("Write lock should have remained blocked")
				case <-time.After(time.Second):
				}
			})
		})

		Context("When a call to WLock() has already completed and WUnlock() has not been called yet", func() {
			It("Should return false", func() {
				var lock RWTryLock

				lock.WLock()
				Expect(lock.TryRLock()).Should(BeFalse())
			})
		})

		Context("When a call to WLock() has not been made yet", func() {
			It("Should return true", func() {
				var lock RWTryLock

				Expect(lock.TryRLock()).Should(BeTrue())
			})
		})

		Context("When a call to WLock() has been made and it has been unlocked by a call to WUnlock()", func() {
			It("Should return true", func() {
				var lock RWTryLock

				lock.WLock()
				lock.WUnlock()
				Expect(lock.TryRLock()).Should(BeTrue())
			})
		})
	})

	Describe("#WLock", func() {
		Context("When there are no readers currently", func() {
			It("Should not block", func() {
				var lock RWTryLock

				writeLockSucceeded := make(chan int)

				go func() {
					lock.WLock()
					writeLockSucceeded <- 1
				}()

				select {
				case <-writeLockSucceeded:
				case <-time.After(time.Second):
					Fail("Write lock should be done")
				}
			})
		})

		Context("When calls to TryRLock() have already succeeded", func() {
			It("Should block until there are a corresponding number of calls made to RUnlock()", func() {
				var lock RWTryLock

				Expect(lock.TryRLock()).Should(BeTrue())
				Expect(lock.TryRLock()).Should(BeTrue())
				Expect(lock.TryRLock()).Should(BeTrue())

				writeLockSucceeded := make(chan int)

				go func() {
					lock.WLock()
					writeLockSucceeded <- 1
				}()

				select {
				case <-writeLockSucceeded:
					Fail("Write lock should block until all reads are done")
				default:
				}

				lock.RUnlock()

				select {
				case <-writeLockSucceeded:
					Fail("Write lock should block until all reads are done")
				default:
				}

				lock.RUnlock()

				select {
				case <-writeLockSucceeded:
					Fail("Write lock should block until all reads are done")
				default:
				}

				lock.RUnlock()

				select {
				case <-writeLockSucceeded:
				case <-time.After(time.Second):
					Fail("Write lock should be done")
				}
			})
		})
	})
})
