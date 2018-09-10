//一致性哈希

package hash

import (
	"fmt"
	"hash/crc32"
	"sort"
	"sync"
)

const VirtualNodesFactor = 256

type node struct {
	key    string
	Data   interface{}
	weight uint
}

type ConsistentHash struct {
	sync.RWMutex

	virtualNodes map[uint32]*node
	actNodes     map[string]*node
	sortRing     []uint32
}

func NewConsistentHash() *ConsistentHash {
	return &ConsistentHash{
		virtualNodes: make(map[uint32]*node),
		actNodes:     make(map[string]*node),
		sortRing:     []uint32{},
	}
}

func (c *ConsistentHash) Add(nk string, nd interface{}, nw uint) bool {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.actNodes[nk]; ok {
		return false
	}

	n := &node{
		key:    nk,
		Data:   nd,
		weight: nw,
	}
	count := int(VirtualNodesFactor * nw)
	for i := 0; i < count; i++ {
		c.virtualNodes[c.hashStr(fmt.Sprintf("%s#%d", nk, i))] = n
	}
	c.actNodes[nk] = n
	c.sortHashRing()
	return true
}

func (c *ConsistentHash) Remove(key string) {
	c.Lock()
	defer c.Unlock()

	node, ok := c.actNodes[key]
	if !ok {
		return
	}
	delete(c.actNodes, key)
	count := int(VirtualNodesFactor * node.weight)
	for i := 0; i < count; i++ {
		delete(c.virtualNodes, c.hashStr(fmt.Sprintf("%s#%d", key, i)))
	}
	c.sortHashRing()
}

func (c *ConsistentHash) sortHashRing() {
	c.sortRing = []uint32{}
	for k := range c.virtualNodes {
		c.sortRing = append(c.sortRing, k)
	}
	sort.Slice(c.sortRing, func(i, j int) bool {
		return c.sortRing[i] < c.sortRing[j]
	})
}

func (c *ConsistentHash) Get(key string) *node {
	hash := c.hashStr(key)

	c.RLock()
	defer c.RUnlock()

	if len(c.virtualNodes) == 0 {
		return nil
	}
	i := c.search(hash)
	return c.virtualNodes[c.sortRing[i]]
}

func (c *ConsistentHash) hashStr(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

func (c *ConsistentHash) search(hash uint32) int {
	i := sort.Search(len(c.sortRing), func(i int) bool { return c.sortRing[i] >= hash })
	if i < len(c.sortRing) {
		if i == len(c.sortRing)-1 {
			return 0
		} else {
			return i
		}
	} else {
		return len(c.sortRing) - 1
	}
}
