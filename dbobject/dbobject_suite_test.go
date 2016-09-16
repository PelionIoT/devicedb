package dbobject_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestDbobject(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "dbobject Suite")
}
