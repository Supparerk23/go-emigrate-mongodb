#
# Build all Versions of Up in the bin directory
#
GOOS=linux GOARCH=amd64 go build -o bin/go-emigrate-mongodb-linux-amd64
GOOS=darwin GOARCH=amd64 go build -o bin/go-emigrate-mongodb-darwin-amd64
GOOS=windows GOARCH=amd64 go build -o bin/go-emigrate-mongodb-windows-amd64.exe
