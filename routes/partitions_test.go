package routes_test

import (
    //. "devicedb/routes"

    . "github.com/onsi/ginkgo"
    //. "github.com/onsi/gomega"
)

var _ = Describe("Partitions", func() {
    Describe("/partitions/{partitionID}/sites/{siteID}/buckets/{bucketID}/batches", func() {
        Describe("POST", func() {
            Context("When the provided body of the request cannot be parsed as an UpdateBatch", func() {
                It("Should respond with status code http.StatusBadRequest", func() {
                    Fail("Not implemented")
                })
            })

            Context("When LocalBatch() returns an error", func() {
                Context("And the error is ENoSuchPartition", func() {
                    It("Should respond with status code http.StatusNotFound", func() {
                        Fail("Not implemented")
                    })
                })

                Context("And the error is ENoSuchSite", func() {
                    It("Should respond with status code http.StatusNotFound", func() {
                        Fail("Not implemented")
                    })
                })

                Context("And the error is ENoSuchBucket", func() {
                    It("Should respond with status code http.StatusNotFound", func() {
                        Fail("Not implemented")
                    })
                })

                // ENoQuorum should indicate a case where the batch was applied locally but should
                // not count toward the write quorum because the local node is currently in the process of
                // obtaining a copy of that partition's data
                Context("And the error is ENoQuorum", func() {
                    It("Should respond with status code http.StatusOK", func() {
                        Fail("Not implemented")
                    })

                    It("Should respond with a JSON-encoded BatchResult body where NApplied is set to 0", func() {
                        Fail("Not implemented")
                    })
                })

                Context("Otherwise", func() {
                    It("Should respond with status code http.StatusInternalServerError", func() {
                        Fail("Not implemented")
                    })
                })
            })

            Context("When LocalBatch() is successful", func() {
                It("Should respond with status code http.StatusOK", func() {
                    Fail("Not implemented")
                })
                
                It("Should respond with a JSON-encoded BatchResult body where NApplied is set to 1", func() {
                    Fail("Not implemented")
                })
            })
        })
    })

    Describe("/partitions/{partitionID}/sites/{siteID}/buckets/{bucketID}/keys", func() {
        Describe("GET", func() {
            Context("When the request includes both \"key\" and \"prefix\" query parameters", func() {
                It("Should respond with status code http.StatusBadRequest", func() {
                    Fail("Not implemented")
                })
            })

            Context("When the request includes neither \"key\" nor \"prefix\" query parameters", func() {
                It("Should respond with status code http.StatusOK", func() {
                    Fail("Not implemented")
                })

                It("Should respond with an empty JSON-encoded list APIEntry database objects", func() {
                    Fail("Not implemented")
                })
            })

            Context("When the request includes one or more \"key\" parameters", func() {
                It("Should call LocalGet() on the node facade with the specified site, bucket and keys", func() {
                    Fail("Not implemented")
                })

                Context("And if LocalGet() returns an error", func() {
                    It("Should respond with status code http.StatusInternalServerError", func() {
                        Fail("Not implemented")
                    })
                })

                Context("And if LocalGet() is successful", func() {
                    It("Should respond with a JSON-encoded list of APIEntrys with one entry per key", func() {
                        Fail("Not implemented")
                    })
                })
            })

            Context("When the request includes one or more \"prefix\" parameters", func() {
                It("Should call LocalGetMatches() on the node facade with the specified site, bucket, and keys", func() {
                    Fail("Not implemented")
                })

                Context("And if LocalGetMatches() returns an error", func() {
                    It("Should respond with status code http.StatusInternalServerError", func() {
                        Fail("Not implemented")
                    })
                })

                Context("And if LocalGetMatches() is successful", func() {
                    It("Should respond with a JSON-encoded list of APIEntrys objects", func() {
                        Fail("Not implemented")
                    })
                })
            })
        })
    })
})
