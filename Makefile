# make formats, tests, and validates

DOLLAR=$

all:
	git status | awk '/modified:/{print ${DOLLAR}NF}' | perl -n -e 'print if /\.go${DOLLAR}/' | xargs -r gofmt -w -s
	go mod tidy
	go test ./...
	golangci-lint run
	@ echo any output from the following command indicates an out-of-date direct dependency
	go list -u -m -f '{{if (and (not .Indirect) .Update)}}{{.}}{{end}}' all

misspell:;
	go install github.com/client9/misspell/cmd/misspell@latest
	misspell -w `find . -name \*.md`

calculate_coverage:
	echo "mode: atomic" > coverage.txt
	for d in $$(go list ./...); do \
	  go test -race -covermode=atomic -coverprofile=profile.out -coverpkg=github.com/singlestore-labs/events/... $$d; \
	  if [ -f profile.out ]; then \
	    grep -v ^mode profile.out >> coverage.txt; \
	    rm profile.out; \
	  fi; \
	done

coverage: calculate_coverage
	go tool cover -html=coverage.txt

