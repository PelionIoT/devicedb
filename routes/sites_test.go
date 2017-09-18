package routes_test

import (
    //. "devicedb/routes"

    . "github.com/onsi/ginkgo"
    //. "github.com/onsi/gomega"
)

var _ = Describe("Sites", func() {
    Describe("/sites/{siteID}", func() {
        Describe("PUT", func() {
            It("Should call AddSite() on the node facade", func() {
                Fail("Not implemented")
            })

            Context("And if AddSite() returns an error", func() {
                It("Should respond with status code http.StatusInternalServerError", func() {
                })
            })

            Context("And if AddSite() is successful", func() {
                It("Should respond with status code http.StatusOK", func() {
                })
            })
        })
    })

    Describe("/sites/{siteID}", func() {
        Describe("DELETE", func() {
            It("Should call RemoveSite() on the node facade", func() {
            })

            Context("And if RemoveSite() returns an error", func() {
                It("Should respond with staus code http.StatusInternalServerError", func() {
                })
            })

            Context("And if RemoveSite() is successful", func() {
                It("Should respond with status code http.StatusOK", func() {
                })
            })
        })
    })

    Describe("/sites/{siteID}/buckets/{bucketID}/batches", func() {
        Describe("POST", func() {
            Context("", func() {
                
            })
        })
    })

    Describe("/sites/{siteID}/buckets/{bucketID}/keys", func() {
        Describe("GET", func() {
        })
    })
})
