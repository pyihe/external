package sonyflake

import (
	"sync"
	"time"
)

var (
	defaultBuilder      *Builder
	defaultStartTime, _ = time.ParseInLocation("2006-01-02 15:04:05", "2022-03-15 10:21:00", time.UTC)
)

func init() {
	mID, err := lower16BitPrivateIP()
	if err != nil {
		panic(err)
	}
	defaultBuilder = &Builder{
		mu:        sync.Mutex{},
		startTime: timestamp(defaultStartTime),
		sequence:  maxSequence,
		machineID: mID,
	}
}

// Next 生成新的ID
func Next() uint64 {
	return defaultBuilder.Next()
}

// Sequence 获取ID对应的序列号
func Sequence(id uint64) uint64 {
	return id & maskSequence >> bitLenMachineID
}

// MachineID 获取ID对应的机器ID
func MachineID(id uint64) uint64 {
	return id & maxMachineID
}

// UnixMilli 获取ID对应的时间戳，单位毫秒
func UnixMilli(id uint64) int64 {
	elapsedTime := id >> (bitLenSequence + bitLenMachineID)
	return (int64(elapsedTime) + defaultBuilder.startTime) * 10
}
