package consistentHash

import (
	"fmt"
	"sort"
	"strconv"
	"testing"
)

func checkEqual(num, expected int, t *testing.T) {
	if num != expected {
		t.Errorf("value is %d, expected %d", num, expected)
	}
}

func TestNewHashRing(t *testing.T) {
	r := NewHashRing()
	if r == nil {
		t.Error("NewHashRing failed.")
	}
}

func TestHashRing_AddNode(t *testing.T) {
	r := NewHashRing()
	r.AddNode("192.168.1.10", 1)
	checkEqual(len(r.ring), DefaultVirtualCubes, t)
	checkEqual(len(r.sortedRing), DefaultVirtualCubes, t)
	if sort.IsSorted(r.sortedRing) == false {
		t.Errorf("expected sorted ring to be sorted")
	}
}

func TestSetCubeNumber(t *testing.T) {
	oldVirtualCubes := DefaultVirtualCubes
	SetCubeNumber(40)
	checkEqual(DefaultVirtualCubes, 40, t)

	r := NewHashRing()
	r.AddNode("192.168.1.10", 1)
	checkEqual(len(r.ring), 40, t)
	SetCubeNumber(oldVirtualCubes)
}

func TestHashRing_AddNodes(t *testing.T) {
	r := NewHashRing()
	Nodes := make(map[string]int)
	for i := 0; i < 10; i++ {
		ip := "192.168.1." + strconv.Itoa(i+1)
		Nodes[ip] = i + 1
	}
	r.AddNodes(Nodes)
	checkEqual(len(r.ring), DefaultVirtualCubes*55, t)
	checkEqual(len(r.sortedRing), DefaultVirtualCubes*55, t)
	if sort.IsSorted(r.sortedRing) == false {
		t.Errorf("expected sorted ring to be sorted")
	}
}

func TestHashRing_RemoveNode(t *testing.T) {
	r := NewHashRing()
	r.AddNode("192.168.1.10", 1)
	r.RemoveNode("192.168.1.10")
	checkEqual(len(r.ring), 0, t)
	checkEqual(len(r.sortedRing), 0, t)

	Nodes := make(map[string]int)
	for i := 0; i < 10; i++ {
		ip := "192.168.1." + strconv.Itoa(i+1)
		Nodes[ip] = i + 1
	}
	r.AddNodes(Nodes)
	checkEqual(len(r.ring), 7040, t)
	r.RemoveNode("192.168.1.10")
	checkEqual(len(r.ring), 5760, t)
}

func TestHashRing_Members(t *testing.T) {
	r := NewHashRing()
	Nodes := make(map[string]int)
	for i := 0; i < 10; i++ {
		ip := "192.168.1." + strconv.Itoa(i+1)
		Nodes[ip] = i + 1
	}
	r.AddNodes(Nodes)
	checkEqual(len(r.Members()), 10, t)
}

func TestHashRing_GetNode(t *testing.T) {
	testGet := []struct {
		in, out string
	}{
		{"key1", "192.168.1.3"},
		{"key2", "192.168.1.7"},
		{"key3", "192.168.1.7"},
		{"key4", "192.168.1.9"},
		{"key5", "192.168.1.10"},
	}
	testGetAfterRemove := []struct {
		in, out string
	}{
		{"key1", "192.168.1.3"},
		{"key2", "192.168.1.7"},
		{"key3", "192.168.1.7"},
		{"key4", "192.168.1.9"},
		{"key5", "192.168.1.3"},
	}

	r := NewHashRing()
	Nodes := make(map[string]int)
	for i := 0; i < 10; i++ {
		ip := "192.168.1." + strconv.Itoa(i+1)
		Nodes[ip] = i + 1
	}
	r.AddNodes(Nodes)

	for i, v := range testGet {
		node, err := r.GetNode(v.in)
		if err != nil {
			t.Fatal(i, "err: ", err)
		}
		if node != v.out {
			t.Error("index", i, "err: got", node, ", expected", v.out)
		}
	}

	r.RemoveNode("192.168.1.10")
	for i, v := range testGetAfterRemove {
		node, err := r.GetNode(v.in)
		if err != nil {
			t.Fatal(i, "err: ", err)
		}
		if node != v.out {
			t.Error("index", i, "err: got", node, ", expected", v.out)
		}
	}
}

func TestHashRing_GetNodes(t *testing.T) {
	r := NewHashRing()
	Nodes := make(map[string]int)
	for i := 0; i < 10; i++ {
		ip := "192.168.1." + strconv.Itoa(i+1)
		Nodes[ip] = i + 1
	}
	r.AddNodes(Nodes)

	nodes, err := r.GetNodes("key1", 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 3 {
		t.Error("expected 3 members instead of", len(nodes))
	}
	if nodes[0] != "192.168.1.3" {
		t.Error("First node error, expected 192.168.1.3, but got", nodes[0])
	}
	if nodes[1] != "192.168.1.5" {
		t.Error("Second node error, expected 192.168.1.5, but got", nodes[1])
	}
	if nodes[2] != "192.168.1.7" {
		t.Error("Third node error, expected 192.168.1.7, but got", nodes[2])
	}

	r.RemoveNode("192.168.1.3")
	nodes, err = r.GetNodes("key1", 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 3 {
		t.Error("expected 3 members instead of", len(nodes))
	}
	if nodes[0] != "192.168.1.5" {
		t.Error("First node error, expected 192.168.1.3, but got", nodes[0])
	}
	if nodes[1] != "192.168.1.7" {
		t.Error("Second node error, expected 192.168.1.5, but got", nodes[1])
	}
	if nodes[2] != "192.168.1.8" {
		t.Error("Third node error, expected 192.168.1.7, but got", nodes[2])
	}
}

func TestHashRing_Dispersion(t *testing.T) {
	r := NewHashRing()
	Nodes := make(map[string]int)
	for i := 0; i < 10; i++ {
		ip := "192.168.1." + strconv.Itoa(i+1)
		Nodes[ip] = i + 1
	}
	r.AddNodes(Nodes)

	nodeMap := make(map[string]int, 0)
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key%d", i)
		node, _ := r.GetNode(key)
		if _, ok := nodeMap[node]; ok {
			nodeMap[node] += 1
		} else {
			nodeMap[node] = 1
		}
	}

	expectedNodeMap := make(map[string]int)
	expectedNodeMap["192.168.1.1"] = 130
	expectedNodeMap["192.168.1.2"] = 366
	expectedNodeMap["192.168.1.3"] = 463
	expectedNodeMap["192.168.1.4"] = 623
	expectedNodeMap["192.168.1.5"] = 987
	expectedNodeMap["192.168.1.6"] = 1009
	expectedNodeMap["192.168.1.7"] = 1465
	expectedNodeMap["192.168.1.8"] = 1333
	expectedNodeMap["192.168.1.9"] = 1578
	expectedNodeMap["192.168.1.10"] = 2046

	for k, v := range nodeMap {
		if v != expectedNodeMap[k] {
			t.Error(k, "key quantity error: got", v, ", expected", expectedNodeMap[k])
		}
	}
}
