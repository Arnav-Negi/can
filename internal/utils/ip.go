package utils

import "net"

// ValidateIP validates that the ip provided by user is part of one of the interfaces
func ValidateIP(ip string) bool {
	if ip == "localhost" {
		return true
	}
	addrs, _ := net.InterfaceAddrs()
	for _, addr := range addrs {
		var localIP net.IP
		switch v := addr.(type) {
		case *net.IPNet:
			localIP = v.IP
		case *net.IPAddr:
			localIP = v.IP
		}
		if localIP != nil && localIP.String() == ip {
			return true
		}
	}
	return false
}
