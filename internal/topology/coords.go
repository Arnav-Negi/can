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
		coordMins[i] <= xi < coordMaxs[i]
	*/
	coordMins []float32
	coordMaxs []float32
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
	zone := Zone{}
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
	zoneObj := Zone{}
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
	// Ensure coordinate dimensions match
	utils.Assert(len(coords) == len(z.coordMaxs), "Invalid coordinates")

	var distSquared float64 = 0.0
	dims := len(z.coordMaxs)

	// Iterate over every dimension
	for i := 0; i < dims; i++ {
		point := coords[i]
		zmin := z.coordMins[i]
		zmax := z.coordMaxs[i]

		// Check if the point lies within the zone in this dimension.
		// (We assume that the zone does not wrap around. In many CAN implementations,
		// zones are kept contiguous and do not cross the domain boundary.
		// If zones can cross boundaries, you'll need extra logic here.)
		if point >= zmin && point <= zmax {
			// No distance if inside the zone.
			continue
		}

		// Compute direct distances from the point to the zone boundaries.
		dToMin := float32(math.Abs(float64(point - zmin)))
		dToMax := float32(math.Abs(float64(point - zmax)))

		// Consider the toroidal (wrap-around) distances.
		// If the domain is [0, 1], wrap-around distance is 1 - direct distance.
		wrapDToMin := 1 - dToMin
		wrapDToMax := 1 - dToMax

		// Choose the smallest distance considering wrap-around.
		d1 := dToMin
		if wrapDToMin < d1 {
			d1 = wrapDToMin
		}

		d2 := dToMax
		if wrapDToMax < d2 {
			d2 = wrapDToMax
		}

		d := d1
		if d2 < d1 {
			d = d2
		}

		// Square the chosen distance and add to the cumulative sum.
		distSquared += float64(d * d)
	}

	// Return the Euclidean distance across all dimensions.
	return float32(math.Sqrt(distSquared))
}

func (z Zone) Contains(coords []float32) bool {
	utils.Assert(len(coords) == len(z.coordMaxs), "Invalid coordinates")
	for i := 0; i < len(z.coordMaxs); i++ {
		if coords[i] < z.coordMins[i] || coords[i] >= z.coordMaxs[i] {
			return false
		}
	}
	return true
}

func (z Zone) IsAdjacent(other Zone) bool {
	dims := len(z.coordMins)
	const domainMin, domainMax float32 = 0.0, 1.0
	adjacentCount := 0

	for i := 0; i < dims; i++ {
		// Check if intervals overlap in dimension i.
		// Two intervals overlap if the end of one is greater than the start of the other
		// (This does not include exact touching.)
		if z.coordMaxs[i] > other.coordMins[i] && z.coordMins[i] < other.coordMaxs[i] {
			// They overlap; continue to next dimension.
			continue
		}
		// If they do not overlap, they must exactly touch (be adjacent) in this dimension.
		// Normal adjacency: one zone's max equals the other's min, or vice versa.
		// Wrap-around adjacency: e.g. one zone touches the domain boundary at 1.0 and the other's touches 0.0.
		if z.coordMaxs[i] == other.coordMins[i] ||
			z.coordMins[i] == other.coordMaxs[i] ||
			(z.coordMaxs[i] == domainMax && other.coordMins[i] == domainMin) ||
			(z.coordMins[i] == domainMin && other.coordMaxs[i] == domainMax) {
			adjacentCount++
		} else {
			// In this dimension they neither overlap nor are adjacent.
			return false
		}
	}
	// The zones are considered adjacent if they touch in exactly one dimension
	// and overlap in all the others.
	return adjacentCount == 1
}

func (z *Zone) GetCoordMins() []float32 {
	return z.coordMins
}

func (z *Zone) GetCoordMaxs() []float32 {
	return z.coordMaxs
}

func (z *Zone) SetCoordMins(mins []float32) {
	z.coordMins = mins
}

func (z *Zone) SetCoordMaxs(maxs []float32) {
	z.coordMaxs = maxs
}
