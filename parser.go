package main

import (
	"bytes"
	"fmt"
	"regexp"
)

var verbMsgRegex = regexp.MustCompile("@?[^ ]* ?:(.*)!(.*)@([^ !]*) ([^ ]*) ([^ ]*) :?(.*)")

type IRCMessage interface {
	Raw() []byte
}

type RawMessage struct {
	raw []byte
}

func (rm RawMessage) Raw() []byte { return rm.raw }

type PingMessage struct {
	Payload string
}

type VerbMsg struct {
	Nick     string
	User     string
	Hostname string
	Verb     string
	To       string
	Message  string
}

func (vm VerbMsg) Raw() []byte {
	return fmt.Appendf(
		[]byte{},
		":%s!~%s@%s %s %s :%s",
		vm.Nick,
		vm.User,
		vm.Hostname,
		vm.Verb,
		vm.To,
		vm.Message,
	)
}

func (vm VerbMsg) IsPrivmsgToMe(me string) bool {
	return vm.Verb == "PRIVMSG" && vm.To == me
}

func (pm PingMessage) Raw() []byte { return fmt.Appendf([]byte{}, "PING %s", pm.Payload) }

func Parse(rawMsg []byte) IRCMessage {
	if pld, ok := bytes.CutPrefix(rawMsg, []byte("PING ")); ok {
		return PingMessage{Payload: string(pld)}
	}

	// attempt parsing as privmsg
	matches := verbMsgRegex.FindStringSubmatch(string(rawMsg))

	if len(matches) == 7 { // EMPYRICAL
		return VerbMsg{
			Nick:     matches[1],
			User:     matches[2],
			Hostname: matches[3],
			Verb:     matches[4],
			To:       matches[5],
			Message:  matches[6],
		}
	}

	return RawMessage{raw: rawMsg}
}
