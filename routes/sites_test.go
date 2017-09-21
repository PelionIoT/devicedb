package routes_test

import (
    //. "devicedb/routes"

    . "github.com/onsi/ginkgo"
    //. "github.com/onsi/gomega"
)

var _ = Describe("Sites", func() {
    Describe("/sites/{siteID}", func() {
        Describe("PUT", func() {
            It("Should call AddSite() on the node facade with the site ID specified in the path", func() {
                Fail("Not implemented")
            })

            Context("And if AddSite() returns an error", func() {
                It("Should respond with status code http.StatusInternalServerError", func() {
                    Fail("Not implemented")
                })
            })

            Context("And if AddSite() is successful", func() {
                It("Should respond with status code http.StatusOK", func() {
                    Fail("Not implemented")
                })
            })
        })
        
        Describe("DELETE", func() {
            It("Should call RemoveSite() on the node facade with the site ID specified in the path", func() {
                Fail("Not implemented")
            })

            Context("And if RemoveSite() returns an error", func() {
                It("Should respond with staus code http.StatusInternalServerError", func() {
                    Fail("Not implemented")
                })
            })

            Context("And if RemoveSite() is successful", func() {
                It("Should respond with status code http.StatusOK", func() {
                    Fail("Not implemented")
                })
            })
        })
    })

    Describe("/sites/{siteID}/buckets/{bucketID}/batches", func() {
        Describe("POST", func() {
            Context("When the provided body of the request cannot be parsed as a TransportUpdateBatch", func() {
                It("Should respond with status code http.StatusBadRequest", func() {
                    Fail("Not implemented")
                })
            })

            Context("When the provided TransportUpdateBatch cannot be convered into an UpdateBatch", func() {
                It("Should respond with status code http.StatusBadRequest", func() {
                    Fail("Not implemented")
                })
            })

            Context("When Batch() returns an error", func() {
                Context("And the error is ENoSuchSite", func() {
                    It("Should respond with status code http.StatusNotFound", func() {
                        Fail("Not implemented")
                    })

                    It("Should respond with an ENoSuchSite body", func() {
                        Fail("Not implemented")
                    })
                })

                Context("And the error is ENoSuchBucket", func() {
                    It("Should respond with status code http.StatusNotFound", func() {
                        Fail("Not implemented")
                    })

                    It("Should respond with an ENoSuchBucket body", func() {
                        Fail("Not implemented")
                    })
                })

                Context("And the error is ENoQuorum", func() {
                    It("Should respond with status code http.StatusInternalServerError", func() {
                        Fail("Not implemented")
                    })

                    It("Should respond with an ENoQuorum body", func() {
                        Fail("Not implemented")
                    })
                })

                Context("Otherwise", func() {
                    It("Should respond with status code http.StatusInternalServerError", func() {
                        Fail("Not implemented")
                    })

                    It("Should respond with an EStorage body", func() {
                        Fail("Not implemented")
                    })
                })
            })

            Context("When Batch() is successful", func() {
                It("Should respond with status code http.StatusOK", func() {
                    Fail("Not implemented")
                })
            })
        })
    })

    Describe("/sites/{siteID}/buckets/{bucketID}/keys", func() {
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
                It("Should call Get() on the node facade with the specified site, bucket and keys", func() {
                    Fail("Not implemented")
                })

                Context("And if Get() returns an error", func() {
                    Context("And the error is ENoSuchSite", func() {
                        It("Should respond with status code http.StatusNotFound", func() {
                            Fail("Not implemented")
                        })

                        It("Should respond with an ENoSuchSite body", func() {
                            Fail("Not implemented")
                        })
                    })

                    Context("And the error is ENoSuchBucket", func() {
                        It("Should respond with status code http.StatusNotFound", func() {
                            Fail("Not implemented")
                        })

                        It("Should respond with an ENoSuchBucket body", func() {
                            Fail("Not implemented")
                        })
                    })

                    Context("And the error is ENoQuorum", func() {
                        It("Should respond with status code http.StatusInternalServerError", func() {
                            Fail("Not implemented")
                        })

                        It("Should respond with an ENoQuorum body", func() {
                            Fail("Not implemented")
                        })
                    })

                    Context("Otherwise", func() {
                        It("Should respond with status code http.StatusInternalServerError", func() {
                            Fail("Not implemented")
                        })

                        It("Should respond with an EStorage body", func() {
                            Fail("Not implemented")
                        })
                    })
                })

                Context("And if Get() is successful", func() {
                    It("Should respond with status code http.StatusOK", func() {
                        Fail("Not implemented")
                    })

                    It("Should respond with a JSON-encoded list of APIEntrys with one entry per key", func() {
                        Fail("Not implemented")
                    })
                })
            })

            Context("When the request includes one or more \"prefix\" parameters", func() {
                It("Should call GetMatches() on the node facade with the specified site, bucket, and keys", func() {
                    Fail("Not implemented")
                })

                Context("And if GetMatches() returns an error", func() {
                    Context("And the error is ENoSuchSite", func() {
                        It("Should respond with status code http.StatusNotFound", func() {
                            Fail("Not implemented")
                        })

                        It("Should respond with an ENoSuchSite body", func() {
                            Fail("Not implemented")
                        })
                    })

                    Context("And the error is ENoSuchBucket", func() {
                        It("Should respond with status code http.StatusNotFound", func() {
                            Fail("Not implemented")
                        })

                        It("Should respond with an ENoSuchBucket body", func() {
                            Fail("Not implemented")
                        })
                    })

                    Context("And the error is ENoQuorum", func() {
                        It("Should respond with status code http.StatusInternalServerError", func() {
                            Fail("Not implemented")
                        })

                        It("Should respond with an ENoQuorum body", func() {
                            Fail("Not implemented")
                        })
                    })

                    Context("Otherwise", func() {
                        It("Should respond with status code http.StatusInternalServerError", func() {
                            Fail("Not implemented")
                        })

                        It("Should respond with an EStorage body", func() {
                            Fail("Not implemented")
                        })
                    })
                })

                Context("And if GetMatches() is successful", func() {
                    It("Should respond with status code http.StatusOK", func() {
                        Fail("Not implemented")
                    })

                    It("Should respond with a JSON-encoded list of APIEntrys objects", func() {
                        Fail("Not implemented")
                    })
                })
            })
        })
    })
})
