package routes_test

import (
    //. "devicedb/routes"

    . "github.com/onsi/ginkgo"
    //. "github.com/onsi/gomega"
)

var _ = Describe("Relays", func() {
    Describe("/relays/{relayID}", func() {
        Describe("PATCH", func() {
            Context("When the request body cannot be parsed as a RelaySettingsPatch", func() {
                It("Should respond with status code http.StatusBadRequest", func() {
                    Fail("Not implemented")
                })
            })

            Context("When the request body is successfully parsed", func() {
                It("Should call MoveRelay() on the node facade using the relay ID provide in the path and siteID provided in the body", func() {
                    Fail("Not implemented")
                })

                Context("And if MoveRelay() returns an error", func() {
                    Context("And the error is ENoSuchRelay", func() {
                        It("Should respond with status code http.StatusNotFound", func() {
                            Fail("Not implemented")
                        })
                    })

                    Context("Otherwise", func() {
                        It("Should respond with status code http.StatusInternalServerError", func() {
                            Fail("Not implemented")
                        })
                    })
                })

                Context("And if MoveRelay() is successful", func() {
                    It("Should respond with status code http.StatusOK", func() {
                        Fail("Not implemented")
                    })
                })
            })
        })

        Describe("PUT", func() {
            It("Should call AddRelay() on the node facade with the relay ID specified in the path", func() {
                Fail("Not implemented")
            })

            Context("And if AddRelay() returns an error", func() {
                It("Should respond with status code http.StatusInternalServerError", func() {
                    Fail("Not implemented")
                })
            })

            Context("And if AddRelay() is successful", func() {
                It("Should respond with status code http.StatusOK", func() {
                    Fail("Not implemented")
                })
            })
        })

        Describe("DELETE", func() {
            It("Should call RemoveRelay() on the node facade with the site ID specified in the path", func() {
                Fail("Not implemented")
            })

            Context("And if RemoveRelay() returns an error", func() {
                It("Should respond with staus code http.StatusInternalServerError", func() {
                    Fail("Not implemented")
                })
            })

            Context("And if RemoveRelay() is successful", func() {
                It("Should respond with status code http.StatusOK", func() {
                    Fail("Not implemented")
                })
            })
        })
    })
})
