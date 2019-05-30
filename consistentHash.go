package consistentHash

import (
	"errors"
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

var (
	GHashRing           *HashRing
	DefaultVirtualCubes = 128
)

// Implement sort interface
type uintArray []uint32

func (x uintArray) Len() int           { return len(x) }
func (x uintArray) Less(i, j int) bool { return x[i] < x[j] }
func (x uintArray) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }

// HashRing struct
// ring:          map, key is hash of cubes, value is real node
// sortedRing:    slice, sorted array which elements is the ring's key
// members:       map, key is real nodes, value is true or false
// weights:       map, key is real nodes, value is this node's weight
// numberOfCubes: number of virtual cubes per node
type HashRing struct {
	ring          map[uint32]string
	sortedRing    uintArray
	members       map[string]bool
	weights       map[string]int
	numberOfCubes int
	sync.RWMutex
}

func InitHashRing() *HashRing {
	GHashRing = &HashRing{
		ring:          make(map[uint32]string),
		members:       make(map[string]bool),
		weights:       make(map[string]int),
		numberOfCubes: DefaultVirtualCubes,
	}
	return GHashRing
}

func GetHashRing() *HashRing {
	if GHashRing != nil {
		return GHashRing
	}
	GHashRing = &HashRing{
		ring:          make(map[uint32]string),
		members:       make(map[string]bool),
		weights:       make(map[string]int),
		numberOfCubes: DefaultVirtualCubes,
	}
	return GHashRing
}

// Set the number of virtual cubes per node
// Notice: SetCubeNumber must be called before AddNode or AddNodes
func (r *HashRing) SetCubeNumber(num int) (err error) {
	if len(GHashRing.members) != 0 {
		err = errors.New("nodes already exist in the ring, modify cube number is not allowed")
		return
	}
	if num <= 0 {
		err = errors.New("num must be more than 0, suggest more than 32")
		return
	}
	r.numberOfCubes = num
	err = nil
	return
}

// Get the real nodes in the consistent hash ring
func (r *HashRing) Members() []string {
	r.RLock()
	defer r.RUnlock()

	var m []string
	for k := range r.members {
		m = append(m, k)
	}
	return m
}

// Generate key based on node ip and cube index
func (r *HashRing) generateKey(ip string, i int) string {
	return ip + "#" + strconv.Itoa(i)
}

// Generate hash value based on the above key
func (r *HashRing) generateHash(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

// AddNode: add a node in the consistent hash ring.
func (r *HashRing) AddNode(ip string, weight int) {
	r.Lock()
	defer r.Unlock()

	if weight <= 0 {
		weight = 1
	}
	for i := 0; i < r.numberOfCubes*weight; i++ {
		r.ring[r.generateHash(r.generateKey(ip, i))] = ip
	}
	r.members[ip] = true
	r.weights[ip] = weight

	r.updateSortedRing()
}

// AddNodes: add multiple nodes at once
// Param: map, key is real node ip, value is this node's weight
func (r *HashRing) AddNodes(ipWeight map[string]int) {
	r.Lock()
	defer r.Unlock()

	for ip, weight := range ipWeight {
		if weight <= 0 {
			weight = 1
		}
		for i := 0; i < r.numberOfCubes*weight; i++ {
			r.ring[r.generateHash(r.generateKey(ip, i))] = ip
		}
		r.members[ip] = true
		r.weights[ip] = weight
	}

	r.updateSortedRing()
}

// RemoveNode: removes a node from the consistent hash ring.
func (r *HashRing) RemoveNode(elt string) {
	r.Lock()
	defer r.Unlock()

	weight := r.weights[elt]
	for i := 0; i < r.numberOfCubes*weight; i++ {
		delete(r.ring, r.generateHash(r.generateKey(elt, i)))
	}
	delete(r.members, elt)
	delete(r.weights, elt)
	r.updateSortedRing()
}

// GetNode returns a node close to where name hashes to in the ring.
func (r *HashRing) GetNode(name string) (node string, err error) {
	r.RLock()
	defer r.RUnlock()

	if len(r.ring) == 0 {
		return "", errors.New("empty hash ring")
	}
	key := r.generateHash(name)
	index := r.search(key)
	node = r.ring[r.sortedRing[index]]
	err = nil
	return
}

// GetN returns the N closest distinct real nodes to the name input in the ring.
func (r *HashRing) GetNodes(name string, n int) (nodes []string, err error) {
	r.RLock()
	defer r.RUnlock()

	err = nil
	if len(r.ring) == 0 {
		nodes = nil
		return
	}

	memberCount := len(r.Members())
	if int64(memberCount) < int64(n) {
		n = int(memberCount)
	}

	// get the first node
	key := r.generateHash(name)
	i := r.search(key)
	elem := r.ring[r.sortedRing[i]]
	nodes = append(nodes, elem)
	if len(nodes) == n {
		return
	}

	// get the rest of the nodes
	start := i
	for i = start + 1; i != start; i++ {
		if i >= len(r.sortedRing) {
			i = 0
		}
		elem = r.ring[r.sortedRing[i]]
		if !sliceHasMember(nodes, elem) {
			nodes = append(nodes, elem)
		}
		if len(nodes) == n {
			break
		}
	}

	return
}

// search: find the cube of key's hash value clockwise
func (r *HashRing) search(key uint32) (index int) {
	compareFunc := func(x int) bool {
		return r.sortedRing[x] > key
	}
	index = sort.Search(len(r.sortedRing), compareFunc)
	if index >= len(r.sortedRing) {
		index = 0
	}
	return
}

// updateSortedRing: when hash ring is change, update sortedRing
func (r *HashRing) updateSortedRing() {
	hashes := uintArray{}
	for k := range r.ring {
		hashes = append(hashes, k)
	}
	sort.Sort(hashes)
	r.sortedRing = hashes
}

// sliceHasMember: judge whether the member is include in the slice
func sliceHasMember(slice []string, member string) bool {
	for _, m := range slice {
		if m == member {
			return true
		}
	}
	return false
}
