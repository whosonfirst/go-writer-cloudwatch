CWD=$(shell pwd)
GOPATH := $(CWD)

prep:
	if test -d pkg; then rm -rf pkg; fi

self:   prep
	if test -d src; then rm -rf src; fi
	mkdir -p src/github.com/whosonfirst/go-writer-cloudwatch
	cp *.go src/github.com/whosonfirst/go-writer-cloudwatch/
	cp -r vendor/* src/

rmdeps:
	if test -d src; then rm -rf src; fi 

build:	fmt bin

fmt:
	go fmt *.go

deps:
	@GOPATH=$(GOPATH) go get -u "github.com/aaronland/go-string"
	@GOPATH=$(GOPATH) go get -u "github.com/whosonfirst/go-whosonfirst-aws"
	mv src/github.com/whosonfirst/go-whosonfirst-aws/vendor/github.com/aws src/github.com/

vendor-deps: rmdeps deps
	if test ! -d vendor; then mkdir vendor; fi
	if test -d vendor; then rm -rf vendor; fi
	cp -r src vendor
	find vendor -name '.git' -print -type d -exec rm -rf {} +
	rm -rf src
