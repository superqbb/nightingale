package funcs

import (
	"bufio"
	"io"
	"math/rand"
	"net"
	"net/rpc"
	"reflect"
	"time"

	"github.com/toolkits/pkg/logger"
	"github.com/ugorji/go/codec"

	"github.com/didi/nightingale/src/dataobj"
	"github.com/didi/nightingale/src/toolkits/address"
)

func Push(items []*dataobj.MetricValue) {
	//codec是一个序列化工具包，当前使用了msgpack格式进行编码序列化，MessagePack是一种有效的二进制序列化格式。就像JSON。但又快又小。
	var mh codec.MsgpackHandle
	mh.MapType = reflect.TypeOf(map[string]interface{}(nil))

	addrs := address.GetRPCAddresses("transfer")
	count := len(addrs)
	retry := 0
	for {
		//rand.Perm 将count个transfer随机均匀打散到数组数组下标
		for _, i := range rand.Perm(count) {
			addr := addrs[i]
			conn, err := net.DialTimeout("tcp", addr, time.Millisecond*3000)
			if err != nil {
				logger.Error("dial transfer err:", err)
				continue
			}

			var bufconn = struct { // bufconn here is a buffered io.ReadWriteCloser
				io.Closer
				*bufio.Reader
				*bufio.Writer
			}{conn, bufio.NewReader(conn), bufio.NewWriter(conn)}

			//【不同点】：open-falcon的RPC，使用的是json格式进行传输（net.JsonRpcClient），而夜莺使用的是msgpack，网络传输数据包更小，速度更快，解析速度也比json快。
			rpcCodec := codec.MsgpackSpecRpc.ClientCodec(bufconn, &mh)
			client := rpc.NewClientWithCodec(rpcCodec)

			var reply dataobj.TransferResp
			err = client.Call("Transfer.Push", items, &reply)
			client.Close()
			if err != nil {
				logger.Error(err)
				continue
			} else {
				if reply.Msg != "ok" {
					logger.Error("some item push err", reply)
				}
				return
			}
		}
		time.Sleep(time.Millisecond * 500)

		retry += 1
		if retry == 3 {
			retry = 0
			break
		}
	}
}
