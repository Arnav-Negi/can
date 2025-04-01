package topology

import (
	"github.com/Arnav-Negi/can/internal/utils"
	pb "github.com/Arnav-Negi/can/protofiles"
	"math"
)

type Zone struct {
	/*
		Coordinates of the zone in a dims dimensional toroidal space
		Coordinates are points [x1, x2, ..., xd] where
		coordMins[i] <= xi <= coordMaxs[i]
	*/
	coordMins []float32
	coordMaxs []float32

	/*
		For zone splitting, splits have to be made along the
		first axis, then the second and so on. A counter is stored for
		last split.
	*/
	nextSplitIndex uint32
}

/*
NodeInfo is used to store the information of a node in the
routing table.
*/
type NodeInfo struct {
	NodeId    string
	IpAddress string
	Zone      Zone
}

func NewZone(dims uint) Zone {
	// Initialize the zone with default values
	zone := Zone{
		nextSplitIndex: 0,
	}
	zone.coordMins = make([]float32, dims)
	zone.coordMaxs = make([]float32, dims)
	for i := 0; i < int(dims); i++ {
		zone.coordMins[i] = 0
		zone.coordMaxs[i] = 1
	}
	return zone
}

func NewZoneFromProto(zone *pb.Zone) Zone {
	// Initialize the zone with default values
	zoneObj := Zone{
		nextSplitIndex: 0,
	}
	dims := len(zone.MinCoordinates)
	zoneObj.coordMins = make([]float32, dims)
	zoneObj.coordMaxs = make([]float32, dims)
	for i := 0; i < dims; i++ {
		zoneObj.coordMins[i] = zone.MinCoordinates[i]
		zoneObj.coordMaxs[i] = zone.MaxCoordinates[i]
	}
	return zoneObj
}

func (z Zone) Distance(coords []float32) float32 {
	utils.Assert(len(coords) == len(z.coordMaxs), "Invalid coordinates")
	midCoords := make([]float32, len(z.coordMaxs))
	dist := float32(0)
	for i := 0; i < len(z.coordMaxs); i++ {
		midCoords[i] = (z.coordMins[i] + z.coordMaxs[i]) / 2
		disti := coords[i] - midCoords[i]
		disti = disti * disti
		dist += disti
	}

	return float32(math.Sqrt(float64(dist)))
}

func (z Zone) Contains(coords []float32) bool {
	utils.Assert(len(coords) == len(z.coordMaxs), "Invalid coordinates")
	for i := 0; i < len(z.coordMaxs); i++ {
		if coords[i] < z.coordMins[i] || coords[i] > z.coordMaxs[i] {
			return false
		}
	}
	return true
}
