package consistentHash

import (
	"errors"
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

var (
	DefaultVirtualCubes = 128
	ErrEmptyHashRing    = errors.New("empty hash ring")
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

func NewHashRing() *HashRing {
	return &HashRing{
		ring:          make(map[uint32]string),
		members:       make(map[string]bool),
		weights:       make(map[string]int),
		numberOfCubes: DefaultVirtualCubes,
	}
}

// Set the number of virtual cubes per node
func SetCubeNumber(num int) (err error) {
	if num <= 0 {
		err = errors.New("num must be more than 0, suggest more than 32")
		return
	}
	DefaultVirtualCubes = num
	err = nil
	return
}

// Get the real nodes in the consistent hash ring
func (c *HashRing) Members() []string {
	c.RLock()
	defer c.RUnlock()

	var m []string
	for k := range c.members {
		m = append(m, k)
	}
	return m
}

// Generate key based on node ip and cube index
func (c *HashRing) generateKey(ip string, i int) string {
	return ip + "#" + strconv.Itoa(i)
}

// Generate hash value based on the above key
func (c *HashRing) generateHash(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

// AddNode: add a node in the consistent hash ring.
func (c *HashRing) AddNode(ip string, weight int) {
	c.Lock()
	defer c.Unlock()

	if weight <= 0 {
		weight = 1
	}
	for i := 0; i < c.numberOfCubes*weight; i++ {
		c.ring[c.generateHash(c.generateKey(ip, i))] = ip
	}
	c.members[ip] = true
	c.weights[ip] = weight

	c.updateSortedRing()
}

// AddNodes: add multiple nodes at once
// Param: map, key is real node ip, value is this node's weight
func (c *HashRing) AddNodes(ipWeight map[string]int) {
	c.Lock()
	defer c.Unlock()

	for ip, weight := range ipWeight {
		if weight <= 0 {
			weight = 1
		}
		for i := 0; i < c.numberOfCubes*weight; i++ {
			c.ring[c.generateHash(c.generateKey(ip, i))] = ip
		}
		c.members[ip] = true
		c.weights[ip] = weight
	}

	c.updateSortedRing()
}

// RemoveNode: removes a node from the consistent hash ring.
func (c *HashRing) RemoveNode(elt string) {
	c.Lock()
	defer c.Unlock()

	weight := c.weights[elt]
	for i := 0; i < c.numberOfCubes*weight; i++ {
		delete(c.ring, c.generateHash(c.generateKey(elt, i)))
	}
	delete(c.members, elt)
	delete(c.weights, elt)
	c.updateSortedRing()
}

// GetNode returns a node close to where name hashes to in the ring.
func (c *HashRing) GetNode(name string) (node string, err error) {
	c.RLock()
	defer c.RUnlock()

	if len(c.ring) == 0 {
		return "", ErrEmptyHashRing
	}
	key := c.generateHash(name)
	index := c.search(key)
	node = c.ring[c.sortedRing[index]]
	err = nil
	return
}

// GetN returns the N closest distinct real nodes to the name input in the ring.
func (c *HashRing) GetNodes(name string, n int) (nodes []string, err error) {
	c.RLock()
	defer c.RUnlock()

	err = nil
	if len(c.ring) == 0 {
		nodes = nil
		return
	}

	memberCount := len(c.Members())
	if int64(memberCount) < int64(n) {
		n = int(memberCount)
	}

	// get the first node
	key := c.generateHash(name)
	i := c.search(key)
	elem := c.ring[c.sortedRing[i]]
	nodes = append(nodes, elem)
	if len(nodes) == n {
		return
	}

	// get the rest of the nodes
	start := i
	for i = start + 1; i != start; i++ {
		if i >= len(c.sortedRing) {
			i = 0
		}
		elem = c.ring[c.sortedRing[i]]
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
func (c *HashRing) search(key uint32) (index int) {
	compareFunc := func(x int) bool {
		return c.sortedRing[x] > key
	}
	index = sort.Search(len(c.sortedRing), compareFunc)
	if index >= len(c.sortedRing) {
		index = 0
	}
	return
}

// updateSortedRing: when hash ring is change, update sortedRing
func (c *HashRing) updateSortedRing() {
	hashes := uintArray{}
	for k := range c.ring {
		hashes = append(hashes, k)
	}
	sort.Sort(hashes)
	c.sortedRing = hashes
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
