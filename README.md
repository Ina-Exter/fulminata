# fulminata

An IRC bot for DCC modeled after Dinoex's iroffer. Use that instead. I reinvented the wheel for fun.
Containerized, few dependencies, straightforward (?), and relatively customizable without getting too complicated. Not written in C.

## TODO

- Transfer config to a file
- Web interface to track and start/stop the bot, refresh the packlist
- Track channel joiners
- Auto restart in code
- Tests tests tests
- XDCC helptext
- SDCC
- Seedship magic

## Architecture and design

The implementation is naive and attempts to be compliant with dinoex-iroffer so as to facilitate tracking of the packlist.

### Startup

Upon startup, the bot will discover its own public IP, target channel, nickname, available ports, and connect to the Rizon IRC server over SSL.

The following env vars configure the bot:
- `NICK`: bot nickname.
- `REGISTRATION_PASSWORD`: if your bot has a registered nick, this will be sent to NickServ with `PRIVMSG NickServ: IDENTIFY <REGISTRATION_PASSWORD>`. May be empty.
- `ROOT_FOLDER`: the parent folder. All files under it will be served (see [Filesystem](#_filesystem)).
- `CHANNEL`: channel to connect to.
- `LOCAL_IP`: IP address *on the local network* on which the bot will listen (so as not to bind to all interfaces). This **must** be filled manually.
- `PORTS`: Comma-separated string of ports to put in the pool (i.e. equal to the number of concurrent downloads). Ports below 0 and 65535 cause a panic, bind to a port below 9999 to your own risk.
- `DOWNLOADS_COUNTER_FILE_PATH`: path to the file that backs the download counters. Must be under `ROOT_FOLDER`.
- `RATE_LIMIT`: Maximum number of parallel requests per hostname.

Empty values use the default value instead. Invalid values panic.

The client then attempt connection and enters a reporting loop where it will print the in-flight requests to stdout every minute.
If a fatal IRC error is received, or the connection context is terminated, the loop will be exited and the program will shutdown. Orchestrators should take care to specify a restart policy in line with their expectations.

### Connection

Connecting starts two goroutines:
- One scanner goroutine that only reads from the conn and processes the message it scanned (see [IRC message parsing and handling](#_irc_message_parsing_and_handling))
- One download goroutine that listens on a channel and processes download requests (see [Download](#_download))

Both goroutines rely on a shared context and will terminate if that context is cancelled.

### IRC message parsing and handling

Messages are parsed mostly via regexp in [parser.go](parser.go):
- If a message starts with `PING `, consider it a ping message for a specific case
- Otherwise, a regexp is used to split the message into:
  - tags
  - sender nick
  - sender username
  - sender hostname
  - IRC verb
  - target nick
  - payload, i.e. contents of the message
  - Details of the regexp can be found in the code
- Messages are routed based on the IRC verb, and only PRIVMSG to the bot nick are handled
- From here on, the payload is used to interpret the response as a CTCP call, or is handled as a DCC instruction
- DCC instructions handlers are `func(writer, message)` and do their own decoding and lifecycles

The single entry point to the download flows is `enqueueDownloadRequests`, which is the one-stop shop to build a download request and dispatch it.

### Download

Downloads are split into two parts by `enqueueDownloadRequests`:
- the "processes", which are indexed by hostnames and represent active transers (i.e. for which a port has been bound and that are either awaiting a client TCP connection, are currently sending data, or have failed in-flight)
- the "queue", indexed by hostname, stores requests that are exceeding a given hostname's maximum number of in-flight requests

Note: when ports are saturated, requests stall at port creation time in the download worker, so they are still counted as in-flight.

Upon receiving a valid request, the download worker will open the file, pick and bind a port (via round-robin), then create a listener and wait for an incoming connection. All closable objects are registered to the download process for future cleanup. An abusive timeout of 60 min is also started, but as requests may be resumed, this is acceptable.

Once the initial `RECV` is accepted, if the client sends a `DCC RESUME`, the IRC handler will inject the resume offset in the download process. When a connection is established, the download worker will read the offset, then write the file (from the offset if existing, zero otherwise) to the conn. A two-seconds delay is kept for flushing once the copy is over, then the download is terminated via `terminateDownloadProcess`.

This function aims at extracting the process from the in-flight bookkeeping an properly clean its resouces, then give the port back to the pool. This is the only way to terminate an in-flight request, so `DCC CANCEL` also triggers it.

### Filesystem

All FS-related operations are ran through the `FileHandler` interface, which hides a very basic struct controlling a `fs.FS`. All files under the root directory are served, except the following extensions:
- `.ttf`
- `.md5`
- `.jpg`
- `.png`
- `.txt`
- `.json`

Files are indexed upon startup via `fs.WalkDir`. **This may produce two different pack lists on two different startups** if the offered packs are different. Lookups are done with map access so should be relatively fast on the read path. Tests were done with ~2500 packs.

Counts are saved to a json file and are peristed by a non-blocking process every four minutes. The file is read or created at startup and loaded into a map for quicker lookups.

For bigger pack lists, a possibility would be to build the entire list file in memory at startup rather than dynamically when receiving a pack list download request.
