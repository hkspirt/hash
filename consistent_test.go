package hash

import (
	"fmt"
	"testing"
)

func dumpData(h *ConsistentHash) map[string]string {
	keyMap := make(map[string]string, 0)
	for i := 0; i < 10000; i++ {
		si := fmt.Sprintf("key%d", i)
		k := h.Get(si)
		d := k.Data.(string)
		keyMap[si] = d
	}
	return keyMap
}

func TestConsistentHash_Add(t *testing.T) {
	hc := NewConsistentHash()
	for i := 0; i < 5; i++ {
		si := fmt.Sprintf("%d", i)
		hc.Add("192.168.1."+si, "192.168.1."+si, 1)
	}
	for k, v := range hc.actNodes {
		fmt.Println("k:", k, " v:", v)
	}
	ipMap := make(map[string]int, 0)
	for i := 0; i < 10000; i++ {
		si := fmt.Sprintf("key%d", i)
		k := hc.Get(si)
		d := k.Data.(string)
		if _, ok := ipMap[d]; ok {
			ipMap[d] += 1
		} else {
			ipMap[d] = 1
		}
	}
	for k, v := range ipMap {
		fmt.Println("k:", k, " count:", v)
	}
}

func TestConsistentHash_Get(t *testing.T) {
	hc := NewConsistentHash()
	for i := 0; i < 5; i++ {
		si := fmt.Sprintf("%d", i)
		hc.Add("192.168.1."+si, "192.168.1."+si, 1)
	}

	keyMap := dumpData(hc)
	hc.Remove("192.168.1.0")
	remove := 0
	for i := 0; i < 10000; i++ {
		si := fmt.Sprintf("key%d", i)
		k := hc.Get(si)
		d := k.Data.(string)
		if keyMap[si] != d {
			remove++
		}
	}
	fmt.Println("remove:", remove)

	keyMap = dumpData(hc)
	hc.Add("192.168.1.5", "192.168.1.5", 1)
	add := 0
	for i := 0; i < 10000; i++ {
		si := fmt.Sprintf("key%d", i)
		k := hc.Get(si)
		d := k.Data.(string)
		if keyMap[si] != d {
			add++
		}
	}
	fmt.Println("add:", add)
}

func BenchmarkConsistentHash_Get(b *testing.B) {
	cHashRing := NewConsistentHash()
	for i := 0; i < 5; i++ {
		si := fmt.Sprintf("%d", i)
		cHashRing.Add("192.168.1."+si, "192.168.1."+si, 1)
	}
	strNum := 1000
	str := make([]string, strNum)
	for i := 0; i < strNum; i++ {
		str[i] = fmt.Sprintf("key%d", i)
	}
	for i := 0; i < b.N; i++ {
		cHashRing.Get(str[i%strNum])
	}
}

type ForTest struct {
}

func (ft *ForTest) Loops(hc *ConsistentHash, str []string) {
	ls := len(str)
	for i := 0; i < 1000; i++ {
		hc.Get(str[i%ls])
	}
}

func BenchmarkConsistentHash_Get2(b *testing.B) {
	hc := NewConsistentHash()
	for i := 0; i < 5; i++ {
		si := fmt.Sprintf("%d", i)
		hc.Add("192.168.1."+si, "192.168.1."+si, 1)
	}
	strNum := 1000
	str := make([]string, strNum)
	for i := 0; i < strNum; i++ {
		str[i] = fmt.Sprintf("key%d", i)
	}

	b.RunParallel(func(pb *testing.PB) {
		var test ForTest
		ptr := &test
		for pb.Next() {
			ptr.Loops(hc, str)
		}
	})
}
