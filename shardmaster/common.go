package shardmaster

import (
	"sort"
)

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK = "OK"
)

type Err string

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
	ReqId   int64
	Me      int64
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs  []int
	Me    int64
	ReqId int64
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard int
	GID   int
	//
	Me    int64
	ReqId int64
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num int // desired config number
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

//把group写入 config的gruop内
func CopyGroups(config *Config, groups map[int][]string) {
	config.Groups = make(map[int][]string)
	for key, value := range groups {
		config.Groups[key] = value
	}
}

//把所有的group内的serverId sort 返回
func SortGroup(groups map[int][]string) []int {
	var keys []int
	for key, _ := range groups {
		keys = append(keys, key)
	}
	sort.Ints(keys)
	return keys
}

//分配每个shard到server
func DistributionGroups(config *Config) {
	keys := SortGroup(config.Groups)
	for index := 0; index < NShards; {
		for i := 0; i < len(keys) && index < NShards; i++ {
			key := keys[i]
			config.Shards[index] = key //把这个shard的server设置为key
			index++
		}
	}
}

func MergeGroups(config *Config, groups map[int][]string) {
	//每个gid 和 对应的servers
	for key, value := range groups {
		//找到config的那个group
		servers, ok := config.Groups[key]
		if ok { //原来就有这个group
			//加入所有的servers
			servers = append(servers, value...)
		} else { //需要加一个组，还要调整每个组的shards
			for cnt := 0; ; cnt++ {
				//遍历获取分片数量最大组，写入新组。
				maxGroup, maxGroups := GetMaxCountShards(config)
				if cnt >= len(maxGroups)-1 && maxGroup != 0 { //均匀：新组的个数大于等于最大组的 - 1
					//分配均匀，完成分配
					break
				}
				config.Shards[maxGroups[0]] = key //最大组的第一个shard 的从属为 key这个组
			}
			servers = value
		}
		config.Groups[key] = servers
	}
}

//获取分片数量最多组
func GetMaxCountShards(config *Config) (group int, rst []int) {
	maps := GetCountShards(config) //得到每个group所有的shards
	keys := SortCountShards(maps)  //gid排序 保证如果有同样多shards的group 选小的那个
	max := 0
	for i := 0; i < len(keys); i++ {
		key := keys[i]
		value := maps[key]
		if len(value) > max {
			group = key
			rst = value
			max = len(value)
		}
	}
	return
}

func GetMinCountShards(config *Config, without int) int {
	rst := 0
	maps := GetCountShards(config)
	keys := SortCountShards(maps)
	min := NShards + 1

	for i := 0; i < len(keys); i++ {
		key := keys[i]
		value := maps[key]
		if key == without {
			continue
		}
		if len(value) < min {
			rst = key
			min = len(value)
		}
	}
	return rst
}

//获得每个组下的分片  返回一个map  gid -> list of shards
func GetCountShards(config *Config) map[int][]int {
	rst := make(map[int][]int)
	//初始化每个gid 的一个list
	for key, _ := range config.Groups {
		rst[key] = make([]int, 0)
	}
	//遍历每个shard
	for i := 0; i < len(config.Shards); i++ {
		//从属的组
		group := config.Shards[i]
		_, ok := rst[group]
		if ok {
			rst[group] = append(rst[group], i)
		} else {
			rst[group] = make([]int, 1)
			rst[group][0] = i
		}
	}
	return rst
}

//把所有组的gid 排序
func SortCountShards(shards map[int][]int) []int {
	var keys []int
	for key, _ := range shards {
		keys = append(keys, key)
	}
	sort.Ints(keys)
	return keys
}

//delete
func DeleteGroups(config *Config, groups []int) {
	for i := 0; i < len(groups); i++ {
		group := groups[i]
		_, ok := config.Groups[group]
		if ok {
			//获取该组分片
			shards := GetCountGroup(&(config.Shards), group)
			//遍历依次加入最小组
			for j := 0; j < len(shards); j++ {
				min := GetMinCountShards(config, group)
				config.Shards[shards[j]] = min
			}
			//删除组
			delete(config.Groups, group)
		}
	}
}

//得到一个group下的shards
func GetCountGroup(Shards *[NShards]int, group int) (rst []int) {
	for i := 0; i < len(*Shards); i++ {
		if (*Shards)[i] == group {
			rst = append(rst, i)
		}
	}
	return
}



