package resp

import (
	"bytes"
	"fmt"
	//	log "github.com/cihub/seelog"
	"github.com/gansidui/gotcp"
	"io"
	"io/ioutil"
)

var (
	endTag = []byte("\r\n") //Telnet command's end tag
)

// Packet
type RESPPacket struct {
	cmd  []byte
	args [][]byte
}

func (p *RESPPacket) Serialize() []byte {
	return nil
}

func (p *RESPPacket) GetCMD() []byte {
	return p.cmd
}

func (p *RESPPacket) GetArgs() [][]byte {
	return p.args
}

func NewRESPPacket(cmd []byte, args [][]byte) *RESPPacket {
	return &RESPPacket{
		cmd:  cmd,
		args: args,
	}
}

type RESPProtocol struct {
}

//func (this *RESPProtocol) ReadPacket(r io.Reader, packetSizeLimit uint32) (gotcp.Packet, error) {
//	args := make([][]byte, packetSizeLimit)
//	return NewRESPPacket(args[0], args[1:]), nil
//}

func (this *RESPProtocol) ReadPacket(r io.Reader, packetSizeLimit uint32) (gotcp.Packet, error) {
	//	return nil, nil // test for nothing receive
	fullBuf := bytes.NewBuffer([]byte{})

	//	for {

	data := make([]byte, packetSizeLimit)
	readLengh, err := r.Read(data)
	//	log.Info("ReadPacket:", readLengh)
	if err != nil { //EOF, or worse
		return nil, err
	}

	if readLengh == 0 { // Connection maybe closed by the client
		return nil, gotcp.ErrConnClosing
	} else {
		fullBuf.Write(data[:readLengh])
		// log.Debug(fullBuf)

		line, _ := fullBuf.ReadString('\n')
		var argsCount int

		fmt.Sscanf(line, "*%d\r", &argsCount)
		// log.Debug(argsCount)

		args := make([][]byte, argsCount)
		for i := 0; i < argsCount; i++ {

			var argSize int
			line, _ := fullBuf.ReadString('\n')

			fmt.Sscanf(line, "$%d\r", &argSize)
			data, _ := ioutil.ReadAll(io.LimitReader(fullBuf, int64(argSize)))
			args[i] = data

			fullBuf.ReadByte() // Now check for trailing CR
			fullBuf.ReadByte() // And LF
		}
		//		log.Debug(argsCount, args)
		if argsCount == len(args) {
			return NewRESPPacket(args[0], args[1:]), nil
		} else {
			return nil, fmt.Errorf("RESP format error")
		}
		//		}
	}
}

type RESPRESPPacket struct {
	replay interface{}
}

func (p *RESPRESPPacket) Serialize() []byte {

	return p.replay.([]byte)
}
func NewRESPRESPPacket(replay interface{}) *RESPRESPPacket {
	return &RESPRESPPacket{
		replay: replay,
	}
}
