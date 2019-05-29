# consistentHash
A golang consistent hash implement

# Install

```
go get github.com/dong329041/consistentHash
```

# Usage

```
// if not call SetCubeNumber, the default cube number is 128
consistentHash.SetCubeNumber(64)

// create hash ring
r := NewHashRing()

// add node: the first parameter is node ip
// the second parameter is this node's weight
r.AddNode("192.168.1.1", 1)
r.AddNode("192.168.1.2", 3)
r.AddNode("192.168.1.3", 5)

// or, add multiple nodes at once
Nodes := make(map[string]int)
for i := 0; i < 10; i++ {
	ip := "192.168.1." + strconv.Itoa(i+1)
	Nodes[ip] = i + 1
}

// get the node closest to the key
node, err := r.GetNode("key1")
// get three nodes closest to the key (for multiple replicas)
node, err := r.GetNodes("key1", 3)

// remove node
r.RemoveNode("192.168.1.2")
```
