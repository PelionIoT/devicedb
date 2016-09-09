package halodb_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestHalodb(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Halodb Suite")
}
