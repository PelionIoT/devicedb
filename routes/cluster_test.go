package routes_test

import (
    //"net/http"
    //"net/http/httptest"

    //. "devicedb/routes"

    . "github.com/onsi/ginkgo"
    //. "github.com/onsi/gomega"
)

var _ = Describe("Cluster", func() {
    Describe("/cluster/nodes", func() {
        Describe("POST", func() {
            Context("When the message body is not a valid node config", func() {
                It("Should respond with status code http.StatusBadRequest", func() {
                    Fail("Not implemented")
                })
            })

            Context("When the message body is a valid node config", func() {
                Context("But AddNode() returns an ECancelConfChange error", func() {
                    It("Should respond with status code http.StatusInternalServerError", func() {
                        Fail("Not implemented")
                    })

                    It("Should respond with an EDuplicateNodeID body", func() {
                        Fail("Not implemented")
                    })
                })

                Context("But AddNode() returns an error other than an ECancelConfChange error", func() {
                    It("Should respond with status code http.StatusInternalServerError", func() {
                        Fail("Not implemented")
                    })

                    It("Should respond with an EProposalError body", func() {
                        Fail("Not implemented")
                    })
                })

                Context("And AddNode() returns no error", func() {
                    It("Should respond with status code http.StatusOK", func() {
                        Fail("Not implemented")
                    })
                })
            })
        })
    })

    Describe("/cluster/nodes/{nodeID}", func() {
        Describe("DELETE", func() {
            Context("When a replacement node id is specified and the decommissioning flag is set", func() {
                It("Should respond with status code http.StatusBadRequest", func() {
                    Fail("Not implemented")
                })
            })

            Context("When the nodeID query parameter is not specified", func() {
                It("Should respond with status code http.StatusBadRequest", func() {
                    Fail("Not implemented")
                })
            })

            Context("When the nodeID query parameter is specified but it is not a base 10 encoded 64 bit number", func() {
                It("Should respond with status code http.StatusBadRequest", func() {
                    Fail("Not implemented")
                })
            })

            Context("When the decommission flag is set", func() {
                Context("And the nodeID is set to zero", func() {
                    It("Should call Decommission() on the receiving node", func() {
                        Fail("Not implemented")
                    })

                    Context("And if the Decommission() call returns an error", func() {
                        It("Should respond with status code http.StatusInternalServerError", func() {
                            Fail("Not implemented")
                        })
                    })

                    Context("And the Decommission() call is successful", func() {
                        It("Should respond with status code http.StatusOK", func() {
                            Fail("Not implemented")
                        })
                    })
                })

                Context("And the nodeID is set to the ID of the node receiving the request", func() {
                    It("Should call Decommission() on the receiving node", func() {
                        Fail("Not implemented")
                    })

                    Context("And if the Decommission() call returns an error", func() {
                        It("Should respond with status code http.StatusInternalServerError", func() {
                            Fail("Not implemented")
                        })
                    })

                    Context("And the Decommission() call is successful", func() {
                        It("Should respond with status code http.StatusOK", func() {
                            Fail("Not implemented")
                        })
                    })
                })

                Context("And the nodeID is not set to the ID of the node receiving the request", func() {
                    Context("And the wasForwarded flag is set", func() {
                        It("Should respond with status code http.StatusForbidden", func() {
                            Fail("Not implemented")
                        })
                    })

                    Context("And the wasForwarded flag is not set", func() {
                        Context("And the node receiving the request does not know how the address of the specified node", func() {
                            It("Should respond with status code http.StatusBadGateway", func() {
                                Fail("Not implemented")
                            })
                        })

                        Context("And the node receiving the request knows the address of the specified node", func() {
                            It("Should forward the request to the specified node, making sure to set the wasForwarded flag", func() {
                                Fail("Not implemented")
                            })

                            Context("And if the RemoveNode() RPC returns an error", func() {
                                It("Should respond with status code http.StatusBadGateway", func() {
                                    Fail("Not implemented")
                                })
                            })

                            Context("And if the RemoveNode() RPC is successful", func() {
                                It("Should respond with status code http.StatusOK", func() {
                                    Fail("Not implemented")
                                })
                            })
                        })
                    })
                })
            })

            Context("When a replacement node id is specified", func() {
                Context("And the replacement node id cannot be parsed as a base 10 encoded uint64", func() {
                    It("Should respond with status code http.StatusBadRequest", func() {
                        Fail("Not implemented")
                    })
                })

                Context("And the nodeID is set to zero", func() {
                    It("Should call ReplaceNode() to remove the receiving node from the cluster and replace it with the specified node", func() {
                        Fail("Not implemented")
                    })

                    Context("And if the call to ReplaceNode() returns an error", func() {
                        It("Should respond with status code http.StatusInternalServerError", func() {
                            Fail("Not implemented")
                        })
                    })

                    Context("And if the call to ReplaceNode() is successful", func() {
                        It("Should respond with status code http.StatusOK", func() {
                            Fail("Not implemented")
                        })
                    })
                })

                Context("And the nodeID is set to the ID of the receiving node", func() {
                    It("Should call ReplaceNode() to remove the receiving node from the cluster and replace it with the specified node", func() {
                        Fail("Not implemented")
                    })

                    Context("And if the call to ReplaceNode() returns an error", func() {
                        It("Should respond with status code http.StatusInternalServerError", func() {
                            Fail("Not implemented")
                        })
                    })

                    Context("And if the call to ReplaceNode() is successful", func() {
                        It("Should respond with status code http.StatusOK", func() {
                            Fail("Not implemented")
                        })
                    })
                })

                Context("And the nodeID is non-zero and not set to the ID of the receiving node", func() {
                    It("Should call ReplaceNode() to remove that node from the cluster and replace it with the specified node", func() {
                        Fail("Not implemented")
                    })

                    Context("And if the call to ReplaceNode() returns an error", func() {
                        It("Should respond with status code http.StatusInternalServerError", func() {
                            Fail("Not implemented")
                        })
                    })

                    Context("And if the call to ReplaceNode() is successful", func() {
                        It("Should respond with status code http.StatusOK", func() {
                            Fail("Not implemented")
                        })
                    })
                })
            })

            Context("When a replacement node id is not specified", func() {
                Context("And the nodeID is set to zero", func() {
                    It("Should call RemoveNode() to remove the receiving node from the cluster", func() {
                        Fail("Not implemented")
                    })

                    Context("And if the call to RemoveNode() returns an error", func() {
                        It("Should respond with status code http.StatusInternalServerError", func() {
                            Fail("Not implemented")
                        })
                    })

                    Context("And if the call to RemoveNode() is successful", func() {
                        It("Should respond with status code http.StatusOK", func() {
                            Fail("Not implemented")
                        })
                    })
                })

                Context("And the nodeID is set to the ID of the receiving node", func() {
                    It("Should call RemoveNode() to remove the receiving node from the cluster", func() {
                        Fail("Not implemented")
                    })

                    Context("And if the call to RemoveNode() returns an error", func() {
                        It("Should respond with status code http.StatusInternalServerError", func() {
                            Fail("Not implemented")
                        })
                    })

                    Context("And if the call to RemoveNode() is successful", func() {
                        It("Should respond with status code http.StatusOK", func() {
                            Fail("Not implemented")
                        })
                    })
                })

                Context("And the nodeID is non-zero and not set to the ID of the receiving node", func() {
                    It("Should call RemoveNode() to remove that node from the cluster", func() {
                        Fail("Not implemented")
                    })

                    Context("And if the call to RemoveNode() returns an error", func() {
                        It("Should respond with status code http.StatusInternalServerError", func() {
                            Fail("Not implemented")
                        })
                    })

                    Context("And if the call to RemoveNode() is successful", func() {
                        It("Should respond with status code http.StatusOK", func() {
                            Fail("Not implemented")
                        })
                    })
                })
            })
        })
    })
})
