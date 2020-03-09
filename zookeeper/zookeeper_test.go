package zookeeper

import (
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"testing"
)

func DumpZkStateString(s *zk.Stat) string {
	return fmt.Sprintf("Czxid:%d, Mzxid: %d, Ctime: %d, Mtime: %d, Version: %d, Cversion: %d, Aversion: %d, EphemeralOwner: %d, DataLength: %d, NumChildren: %d, Pzxid: %d",
		s.Czxid, s.Mzxid, s.Ctime, s.Mtime, s.Version, s.Cversion, s.Aversion, s.EphemeralOwner, s.DataLength, s.NumChildren, s.Pzxid)
}

func DumpZkStateStringFormat(s *zk.Stat) string {
	return fmt.Sprintf("Czxid:%d\nMzxid: %d\nCtime: %d\nMtime: %d\nVersion: %d\nCversion: %d\nAversion: %d\nEphemeralOwner: %d\nDataLength: %d\nNumChildren: %d\nPzxid: %d\n",
		s.Czxid, s.Mzxid, s.Ctime, s.Mtime, s.Version, s.Cversion, s.Aversion, s.EphemeralOwner, s.DataLength, s.NumChildren, s.Pzxid)
}

func TestZookeeper(t *testing.T){
	connstr:="9.134.194.119:2181,9.134.194.119:2182,9.134.194.119:2183"
	zkConn,err:=initZkConn(connstr)
	if err!=nil{
		fmt.Println(err)
		return
	}
	//创建临时节点
	res,err:=zkConn.CreateNode("/server",[]byte("9.134.194.119:2181"))
	if err!=nil{
		fmt.Println("create node error: ",err)
		return
	}
	fmt.Println("create node success",res)

	fmt.Println("-------------------获取节点数据-------------------")
	nodeVal,err:=zkConn.GetNode("/server")
	if err!=nil{
		fmt.Println(err)
		return
	}
	fmt.Printf("/server=%s\n",nodeVal)
	fmt.Println("-------------------获取节点数据-------------------")

	fmt.Println("-------------------列出节点子目录-------------------")
	list,err:=zkConn.ListChildren("/")
	for _,v:=range list{
		fmt.Println(v)
	}
	fmt.Println("-------------------列出节点子目录-------------------")

	fmt.Println("-------------------修改节点数据-------------------")
	err=zkConn.SetNode("/server",[]byte("0.0.0.0:1"))
	if err!=nil{
		fmt.Println(err)
		return
	}else{
		fmt.Println("change /server data successed")
	}
	fmt.Println("-------------------修改节点数据-------------------")

	fmt.Println("-------------------获取节点数据-------------------")
	nodeVal,err=zkConn.GetNode("/server")
	if err!=nil{
		fmt.Println(err)
		return
	}
	fmt.Printf("/server=%s\n",nodeVal)
	fmt.Println("-------------------获取节点数据-------------------")


	//删除节点
	fmt.Println("-------------------删除节点数据-------------------")
	err=zkConn.DeleteNode("/server")
	if err!=nil{
		fmt.Println(err)
		return
	}else{
		fmt.Println("success delete node of /server ")
	}
	fmt.Println("-------------------删除节点数据-------------------")

	fmt.Println("-------------------判断节点是否存在-------------------")
	exist:=NodeExist(zkConn,"/server")
	if !exist{
		fmt.Println("node /server is not exist")
	}
	fmt.Println("-------------------判断节点是否存在-------------------")
}


func NodeExist(zkConn *ZkConn,path string)(exist bool){
	/*
		*获取节点信息
		*
		type Stat struct {
			Czxid          int64 // The zxid of the change that caused this znode to be created.
			Mzxid          int64 // The zxid of the change that last modified this znode.
			Ctime          int64 // The time in milliseconds from epoch when this znode was created.
			Mtime          int64 // The time in milliseconds from epoch when this znode was last modified.
			Version        int32 // The number of changes to the data of this znode.
			Cversion       int32 // The number of changes to the children of this znode.
			Aversion       int32 // The number of changes to the ACL of this znode.
			EphemeralOwner int64 // The session id of the owner of this znode if the znode is an ephemeral node. If it is not an ephemeral node, it will be zero.
			DataLength     int32 // The length of the data field of this znode.
			NumChildren    int32 // The number of children of this znode.
			Pzxid          int64 // last modified children
		}
	*/
	exist,stat,err:=zkConn.NodeExists(path)
	if err!=nil{
		fmt.Println(err)
		return false
	}else if(!exist){
		return false
	}
	dumpStr:=DumpZkStateString(stat)

	fmt.Println(dumpStr)
	return true
}