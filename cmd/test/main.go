package main

import (
	"log"
	"net"
)

// GetIPAddress returns the IP address of the current device
func main() {
	addrs, _ := net.InterfaceAddrs()

	for _, addr := range addrs {
		if ipNet, ok := addr.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
			if ipNet.IP.To4() != nil {
				log.Println(ipNet.IP.String())
			}
		}
	}
}

// GetAvailablePort returns an available port on the local machine
func GetAvailablePort() (int, error) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		return 0, err
	}
	defer listener.Close()

	addr := listener.Addr().(*net.TCPAddr)
	return addr.Port, nil
}
