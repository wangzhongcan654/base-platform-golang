/*=============================================================================
#     FileName: test.go
#         Desc: log test
#       Author: curlwang
#        Email: 657688162@qq.com
#      Version: 0.0.1
#   LastChange: 2019-12-28 20:23:01
#      History:
=============================================================================*/
package logger

import (
	"runtime"
	"testing"
)

func TestWrite(t *testing.T){
	InitLogger(".","test","logtest",10,"DEBUG")
	INFO("test")
}



func BenchmarkWrite(b *testing.B) {
	InitLogger(".", "test", "logtest", 100, "DEBUG")

	for i := 0; i < b.N; i++ {
		mem := new(runtime.MemStats)
		runtime.ReadMemStats(mem)
		INFO("every log mem alloced: ", mem.Alloc)
		INFO(i)
		runtime.ReadMemStats(mem)
		INFO("after log mem alloced: ", mem.Alloc)
	}
	runtime.GC()
	mem := new(runtime.MemStats)
	runtime.ReadMemStats(mem)
	INFO("after gc mem alloced: ", mem.Alloc)
}
