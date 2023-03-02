package bloom

import (
	"math"

	"github.com/spaolacci/murmur3"
)

/*
	布隆过滤器：
	基于概率进行验重的数据结构，基本原理是利用小概率事件不容易同时发生
	布隆过滤器使用多个哈希函数把同一个字符串转换成多个不同的哈希值(哈希值为16进制整型)，利用Redis中简单字符串的位操作来记录每个哈希值，
	当面对新的字符串时，过滤器再次使用原先的哈希函数计算出新字符串的哈希值，然后将新的哈希值与既有的进行比对，如果每个哈希值对应的二进制
	位均为1，则证明新的字符串可能存在（布隆过滤器只能证明一定不存在，不能证明一定存在）


	假设选择K个哈希函数，对同一个字符串计算哈希值，可以得到K个完全不同的哈希值（不考虑hash冲突的话），让这K个哈希值同时除以一个数M，
	得到K个余数，在Redis中记录下这K个余数
	对于新的字符串，重复上述步骤，如果Redis中新的哈希值对应的每个二进制位均被置为了1，则证明新的字符串可能已经存在（可能性可以通过待
	验重字符串总数N，哈希函数个数K和被除数M计算出来，公式为：1-(1-e^(-KN/M))^K）。

	如何利用Redis实现布隆过滤器:
	Redis中的简单字符串一个key最多可以存储2^32个字符，用每个字符存储一个二进制位，那么一个简单字符串可以存储2^(2^32)个数。所以完全可以
	用一个简单字符串的key来实现布隆过滤器。


	如果最多对N个字符串验重，能够容忍的最大误报率位p，那么布隆过滤器将会使用到的二进制位的数量为: m = -(n*lnp)/(ln2)^2
	哈希函数的个数为: k = m/n*ln2
*/

type BitMapper interface {
	// GetBit 返回存储在offset位置的值
	GetBit(key string, offset int64) int64
	// SetBit 设置存储在offset位置的值为value，并返回offset处的旧值
	SetBit(key string, offset int64, value int) int64
}

type Filter struct {
	// 布隆过滤器对应的简单字符串的key
	key string
	// 过滤器能够容忍的最大误报率
	tolerance float64
	// 过滤器需要验重的数据量
	scale uint64
	// 根据误报率和数据量计算出来的hash函数个数
	hashCount uint64
	// 根据误报率和数据量计算出来的需要的最大二进制位数
	maxBitSize uint64
}

func NewFilter(key string, scale uint64, tolerance float64) *Filter {
	filter := &Filter{
		key:       key,
		tolerance: tolerance,
		scale:     scale,
	}

	filter.maxBitSize = uint64(-float64(scale) * math.Log(tolerance) / math.Pow(math.Log(2), 2))
	filter.hashCount = uint64(float64(filter.maxBitSize) / float64(scale) * math.Log(2))
	return filter
}

// BSet 设置target对应的位为1
// 设置成功返回true， 否则返回false
func (filter *Filter) BSet(bitMapper BitMapper, target string) bool {
	var (
		hashPos = make([]int64, filter.hashCount)
		exist   = true
	)
	for i := 1; i <= int(filter.hashCount); i++ {
		pos := murmur3.Sum64WithSeed([]byte(target), uint32(i)) % filter.maxBitSize
		hashPos[i-1] = int64(pos)
		// 如果已经确定target没出现过，则不需要在判断对应位的值是否为1
		if !exist {
			continue
		}
		// 只有当尚未确定target是否存在时，才进行对应位的判断
		if bitMapper.GetBit(filter.key, int64(pos)) == 0 {
			exist = false
		}
	}
	// 设置位为1
	// 这里需要加分布式锁
	if !exist {
		for _, pos := range hashPos {
			bitMapper.SetBit(filter.key, pos, 1)
		}
	}

	return !exist
}
