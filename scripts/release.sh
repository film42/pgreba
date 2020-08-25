#!/bin/bash
set -x

rm -rf pkg/

GOOS=darwin GOARCH=amd64 go build -o pkg/pgreba-darwin-amd64 
GOOS=windows GOARCH=amd64 go build -o pkg/pgreba-windows-amd64.exe 
GOOS=windows GOARCH=386 go build -o pkg/pgreba-windows-386.exe 
GOOS=linux GOARCH=amd64 go build -o pkg/pgreba-linux-amd64 
GOOS=linux GOARCH=386 go build -o pkg/pgreba-linux-386 

pushd pkg
  for file in $(ls -1 .); do
    shafile="${file}.sha"
    tarfile="${file}.tar.gz"
    tar -cf "${tarfile}" "${file}"
    shasum -a 256 "${tarfile}" > "${shafile}"
    rm "${file}"
  done
popd
