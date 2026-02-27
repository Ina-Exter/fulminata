package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParse(t *testing.T) {
	for _, tt := range []struct {
		name    string
		haveMsg []byte
		wantMsg IRCMessage
	}{
		{
			name:    "PING",
			haveMsg: []byte("PING :wololo"),
			wantMsg: PingMessage{Payload: ":wololo"},
		},
		{
			name:    "privmsg without tags",
			haveMsg: []byte(":Exterminata!~Extermina@AFC539C.716A7309.A137196A.IP PRIVMSG Fulminata|Experiment :XDCC SEND 1"),
			wantMsg: VerbMsg{
				Nick:     "Exterminata",
				User:     "~Extermina",
				Hostname: "AFC539C.716A7309.A137196A.IP",
				Verb:     "PRIVMSG",
				To:       "Fulminata|Experiment",
				Message:  "XDCC SEND 1",
			},
		},
		{
			name:    "privmsg with tags",
			haveMsg: []byte("@id=234AB :dan!d@localhost PRIVMSG #chan :Hey what's up!"),
			wantMsg: VerbMsg{
				Nick:     "dan",
				User:     "d",
				Hostname: "localhost",
				Verb:     "PRIVMSG",
				To:       "#chan",
				Message:  "Hey what's up!",
			},
		},
		{
			name:    "notice",
			haveMsg: []byte(":Global!service@rizon.net NOTICE ayaya :[Logon News - Dec 16 2013] Own a large/active channel or plan to get one going? Please read http://s.rizon.net/authline"),
			wantMsg: VerbMsg{
				Nick:     "Global",
				User:     "service",
				Hostname: "rizon.net",
				Verb:     "NOTICE",
				To:       "ayaya",
				Message:  "[Logon News - Dec 16 2013] Own a large/active channel or plan to get one going? Please read http://s.rizon.net/authline",
			},
		},
		{
			name:    "raw",
			haveMsg: []byte(":irc.uworld.se 001 ayayaya :Welcome to the Rizon Internet Relay Chat Network ayayaya"),
			wantMsg: RawMessage{raw: []byte(":irc.uworld.se 001 ayayaya :Welcome to the Rizon Internet Relay Chat Network ayayaya")},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wantMsg, Parse(tt.haveMsg))
		})
	}

}
