package sonyflake

import (
	"errors"
	"net"
	"sync"
	"time"
)

// from https://github.com/sony/sonyflake
// 39 表示精度为10ms的时间戳
// 8 中间8位表示序列号
// 16 右边16位表示机器ID

const (
	bitLenTime      = 39                               // bit length of time
	bitLenSequence  = 8                                // bit length of sequence number
	bitLenMachineID = 63 - bitLenTime - bitLenSequence // bit length of machine id
	maxMachineID    = -1 ^ (-1 << bitLenMachineID)     // 支持的最大机器序号
	maxSequence     = 1<<bitLenSequence - 1            // 可以生成的序列号的最大值

	timeUnit     = 1e7 // 时间戳精度：10ms
	maskSequence = (1<<bitLenSequence - 1) << bitLenMachineID
)

type Settings struct {
	StartAt      time.Time
	GetMachineId func() uint16
}

type Builder struct {
	mu          sync.Mutex
	startTime   int64  // ID生成器计算的起始时间戳，不能超前
	elapsedTime int64  // 当前时间距离起始时间的时间戳间隔
	sequence    uint16 // 生成的序列号
	machineID   uint16 // 机器ID
}

func New(settings ...Settings) *Builder {
	if len(settings) == 0 || (settings[0].GetMachineId == nil && settings[0].StartAt.IsZero()) {
		return defaultBuilder
	}

	var (
		err       error
		machineId uint16
		setting   = settings[0]
		start     = setting.StartAt
	)

	if setting.GetMachineId != nil {
		machineId = setting.GetMachineId()
	} else {
		machineId, err = lower16BitPrivateIP()
		if err != nil {
			panic(err)
		}
	}
	if start.IsZero() {
		start = defaultStartTime
	}

	return &Builder{
		startTime: timestamp(start),
		machineID: machineId,
		sequence:  maxSequence,
	}
}

func (b *Builder) Next() (id uint64) {
	b.mu.Lock()

	// 当前时间与生成器起始时间的距离
	pastUnix := timestamp(nowFunc()) - b.startTime
	switch b.elapsedTime < pastUnix {
	case true: // 不同的10ms周期内，序列号从0开始计算
		b.elapsedTime = pastUnix
		b.sequence = 0

	default: // 对于同一个周期内的ID，序列号+1，如果序列号达到了上限，将时间偏移量+1来防止ID冲突
		// 否则将sequence加1
		b.sequence = (b.sequence + 1) & maxSequence
		// sequence==0证明走了一圈，为了防止ID冲突，将elapsedTime加1
		if b.sequence == 0 {
			b.elapsedTime += 1
			overtime := b.elapsedTime - pastUnix
			// 通过sleep来同步因为sequence造成的时间差
			time.Sleep(sleepTime(overtime))
		}
	}
	// 这里没有判断是否超过最大时间值，因为最大时间值算下来为174年
	id = uint64(b.elapsedTime)<<(bitLenSequence+bitLenMachineID) |
		uint64(b.sequence)<<bitLenMachineID |
		uint64(b.machineID)

	b.mu.Unlock()

	return
}

// Sequence 获取ID对应的序列号
func (b *Builder) Sequence(id uint64) uint64 {
	return id & maskSequence >> bitLenMachineID
}

// MachineID 获取ID对应的机器ID
func (b *Builder) MachineID(id uint64) uint64 {
	return id & maxMachineID
}

// UnixMilli 获取ID对应的时间戳，单位毫秒
func (b *Builder) UnixMilli(id uint64) int64 {
	elapsedTime := id >> (bitLenSequence + bitLenMachineID)
	return (int64(elapsedTime) + b.startTime) * 10
}

func nowFunc() time.Time {
	return time.Now().UTC()
}

// 转换成精度为10ms的时间戳
func timestamp(t time.Time) int64 {
	return t.UnixNano() / timeUnit
}

func sleepTime(overtime int64) time.Duration {
	return time.Duration(overtime*timeUnit) -
		time.Duration(nowFunc().UnixNano()%timeUnit)
}

func lower16BitPrivateIP() (uint16, error) {
	ip, err := privateIPv4()
	if err != nil {
		return 0, err
	}

	return uint16(ip[2])<<8 + uint16(ip[3]), nil
}

func privateIPv4() (net.IP, error) {
	as, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}

	for _, a := range as {
		ipnet, ok := a.(*net.IPNet)
		if !ok || ipnet.IP.IsLoopback() {
			continue
		}

		ip := ipnet.IP.To4()
		if isPrivateIPv4(ip) {
			return ip, nil
		}
	}
	return nil, errors.New("no private ip address")
}

func isPrivateIPv4(ip net.IP) bool {
	return ip != nil &&
		(ip[0] == 10 || ip[0] == 172 && (ip[1] >= 16 && ip[1] < 32) || ip[0] == 192 && ip[1] == 168)
}
