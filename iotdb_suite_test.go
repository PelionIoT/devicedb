package devicedb_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func Testdevicedb(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "devicedb Suite")
}
