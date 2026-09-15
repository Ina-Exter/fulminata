package main

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"
)

/*
This follows the pattern of:

- optional @ (tags) and tags
- :
- optional sender nick (group 1)
- !
- optional sender username (group 2)
- sender hostname or service name (group 3)
- if there is only a hostname, replace groups 1 and 2 by :
- space
- verb (or number) (group 4)
- space
- target nick (group 5)
- space
- :
- payload (group 6)
*/
var verbMsgRegex = regexp.MustCompile("(?:(?:@?[^ ]* ?:(.*)!(.*)@)|:)?([^ !]*) +([^ ]*) +([^ ]*) +:?(.*)")

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
	return strings.EqualFold(vm.Verb, "PRIVMSG") && strings.EqualFold(vm.To, me)
}

func (vm VerbMsg) SystemMessage() bool {
	return vm.Nick == "" && vm.User == ""
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
