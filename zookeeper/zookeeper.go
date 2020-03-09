package zookeeper

import (
	"errors"
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	logger "utils/log"

	"strings"
	"sync"
	"time"
)

var(
	ZkConnPoolMu sync.Mutex
	ZkConnPool =make(map[string]*ZkConn)
)


//初始化
func initZkConn(connstr string)(zkConn *ZkConn,err error){
	if connstr == ""{
		logger.ERRORF("connect zookeeper server failed: connection string is empty")
		err=errors.New("connection string is empty")
		return
	}

	connStr:=strings.Split(connstr,",")
	conn,ec,err:=zk.Connect(connStr,3*time.Second)
	if err!=nil{
		logger.ERRORF("connect zookeeper server [\"%s\"] failed: %s",connStr,err)
		return
	}

	for{
		connEvent:=<-ec
		switch connEvent.State{
		case zk.StateDisconnected:
			logger.ERRORF("connection zookeeper: [\"%s\"] failed: %s",connStr,err)
			err=errors.New(fmt.Sprintf("connect zookeeper server[\"%s\"] failed",connStr))
			return
		case zk.StateConnected:
			zkConn=&ZkConn{
				conn:conn,
			}
			ZkConnPoolMu.Lock()
			ZkConnPool[connstr]=zkConn
			ZkConnPoolMu.Unlock()
			fmt.Println("connection success")
			return
		default:
			continue
		}
	}
}

//获取集群连接实例
func GetZkInstance(connstr string)(zkConn *ZkConn,err error){
	ZkConnPoolMu.Lock()
	conn,ok:=ZkConnPool[connstr]
	ZkConnPoolMu.Unlock()
	if ok{
		//判断连接是否断开,断开重连
		if conn.conn.State()==zk.StateDisconnected{
			return initZkConn(connstr)
		}
		zkConn=conn
	}
	return initZkConn(connstr)
}

//根据实例获取连接信息
func getConnStrByConn(conn *ZkConn)(connstr string,err error){
	ZkConnPoolMu.Lock()
	for k,v:=range ZkConnPool{
		if v==conn{
			connstr=k
			return
		}
	}
	ZkConnPoolMu.Unlock()
	err=errors.New("get connstr fail,conn is Invalid")
	return
}



type ZkConn struct{
	conn *zk.Conn
}

//创建
func (zook *ZkConn) CreateNode(path string,data []byte)(resPath string,err error){
	if path==""{
		return "",errors.New("Invalid Path")
	}

	//判断连接是否断开
	if zook.conn.State()==zk.StateDisconnected{
		connstr,err:=getConnStrByConn(zook)
		if err!=nil{
			return "",err
		}
		zook,err=GetZkInstance(connstr)
		if err!=nil{
			return "",err
		}

		return zook.CreateNode(path,data)
	}


	//节点参数
	flag:=int32((zk.FlagEphemeral))
	acl:=zk.WorldACL(zk.PermAll)
	childPath:=path

	//创建父节点
	paths:=strings.Split(path,"/")
	var parentPath string
	for _,v:=range paths[1:len(paths)-1]{
		parentPath+="/" +v
		exist,_,err:=zook.conn.Exists(parentPath)
		if err!=nil{
			return "",err
		}
		if !exist{
			//父节点必须是持久节点
			_,err:=zook.conn.Create(parentPath,nil,0,acl)
			if err!=nil{
				return "",err
			}
		}
	}

	//创建子节点
	exist,_,err:=zook.conn.Exists(childPath)
	if err!=nil{
		return "",err
	}
	if !exist{
		resPath,err=zook.conn.Create(childPath,data,flag,acl)
		if err!=nil{
			return "",err
		}
	}else{
		err=errors.New(fmt.Sprintf("[%s] exists",childPath))
	}
	return
}

//修改数据
func (zook *ZkConn) SetNode(path string,data []byte)(err error){
	if path==""{
		return errors.New("Invalid Path")
	}

	//判断连接是否断开
	if zook.conn.State()==zk.StateDisconnected{
		connstr,err:=getConnStrByConn(zook)
		if err!=nil{
			return err
		}
		zook,err=GetZkInstance(connstr)
		if err!=nil{
			return err
		}
		return zook.SetNode(path,data)
	}

	//判断是否存在
	exist,stat,err:=zook.conn.Exists(path)
	if err!=nil{
		return
	}
	if !exist{
		return errors.New(fmt.Sprintf("node [%s] dosen't exist, can't setted",path))
	}
	_,err=zook.conn.Set(path,data,stat.Version)
	if err!=nil{
		return
	}
	return
}

//获取数据
func (zook *ZkConn) GetNode(path string)(data []byte,err error){
	//判断路径是否为空
	if path==""{
		return nil,errors.New("Invalid Path")
	}
	//判断连接是否断开
	if zook.conn.State()==zk.StateDisconnected{
		connstr,err:=getConnStrByConn(zook)
		if err!=nil{
			return nil,err
		}
		zook,err=GetZkInstance(connstr)
		if err!=nil{
			return nil,err
		}
		return zook.GetNode(path)
	}
	//Get
	data,_,err=zook.conn.Get(path)
	return
}

//删除
func (zook *ZkConn) DeleteNode(path string)(err error){
	//判断路径是否为空
	if path==""{
		return errors.New("Invalid Path")
	}
	//判断连接是否断开
	if zook.conn.State()==zk.StateDisconnected{
		connstr,err:=getConnStrByConn(zook)
		if err!=nil{
			return err
		}
		zook,err=GetZkInstance(connstr)
		if err!=nil{
			return err
		}
		return zook.DeleteNode(path)
	}
	//判断节点是否存在
	exist,stat,err:=zook.conn.Exists(path)
	if err!=nil{
		return err
	}
	if !exist{
		return errors.New(fmt.Sprintf("path [\"%s\"] doesn't exist",path))
	}
	//删除节点
	return zook.conn.Delete(path,stat.Version)
}

//列出子目录
func(zook *ZkConn) ListChildren(path string)(children []string,err error){
	//判断路径是否为空
	if path==""{
		return nil,errors.New("Invalid Path")
	}
	//判断连接是否断开
	if zook.conn.State()==zk.StateDisconnected{
		connstr,err:=getConnStrByConn(zook)
		if err!=nil{
			return nil,err
		}
		zook,err=GetZkInstance(connstr)
		if err!=nil{
			return nil,err
		}
		return zook.ListChildren(path)
	}
	//List
	children,_,err=zook.conn.Children(path)
	return
}

//获取当前节点 （完整路径）的watcher
func (zook *ZkConn) GetZNodeWatcher(path string)([]byte,*zk.Stat,<-chan zk.Event,error){
	return zook.conn.GetW(path)
}

//获取当前节点所有子节点变化的watcher
func (zook *ZkConn) GetChildrenWatcher(path string)([]string,*zk.Stat,<-chan zk.Event,error){
	return zook.conn.ChildrenW(path)
}

//当前节点是否存在
func (zook *ZkConn) NodeExists(path string)(exist bool,stat *zk.Stat,err error){
	//判断路径是否为空
	if path==""{
		return false,nil,errors.New("Invalid Path")
	}
	//判断连接是否断开
	if zook.conn.State()==zk.StateDisconnected{
		connstr,err:=getConnStrByConn(zook)
		if err!=nil{
			return false,nil,err
		}

		zook,err=GetZkInstance(connstr)
		if err!=nil{
			return false,nil,err
		}
		return zook.NodeExists(path)
	}
	//判断节点是否存在
	return zook.conn.Exists(path)
}




