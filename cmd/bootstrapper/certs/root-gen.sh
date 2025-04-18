openssl req -x509 -newkey rsa:4096 -days 3650 -nodes -keyout ca-key.pem -out ca-cert.pem \
        -subj "/C=IN/ST=Telangana/L=Hyderabad/O=IIITH/OU=College/CN=ujjwal-shekhar"

echo "CA's self-signed certificate"
openssl x509 -in ca-cert.pem -noout -text