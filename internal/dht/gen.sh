#!/bin/bash
set -e

CERT_DIR=$1

mkdir -p "$CERT_DIR"
cd "$CERT_DIR"

rm -f *.pem *.csr *.srl

openssl req -new -newkey rsa:2048 -nodes \
  -keyout node-key.pem \
  -out node.csr \
  -subj "/CN=$(hostname -f)"

echo "Generated node-key.pem and node.csr in $CERT_DIR"
