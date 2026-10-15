//
// RQ_Preflight.h — fail fast when the storage listeners are not running.
// Created 9/22/26.
//
// Why this exists: ServerInitialization* forks one child per shard, and each
// child builds a NetIOConnector, whose connect() RETRIES FOREVER with no
// timeout.  If nothing is listening on the port, the child never reports ready,
// the parent blocks in waitForAllServersReady(), and the run just sits there
// with no output and no error — indistinguishable from a slow bulk upload.
//
// Call requireListeners() before the first ServerInitialization*.
//
// The probe must NOT connect.  Servers_MultiRingORAM reads with emp::NetIO,
// and emp's recv failure path calls error(), which does exit() — not an
// exception — so the listener's try/catch cannot save it.  A probe that
// connects and hangs up therefore KILLS the listener:
//     [Server port=8881] Client connected (conn #1)
//     net_recv_data                      <- emp error(), process exits
// (v1 of this header did exactly that, which is worse than the hang it was
// meant to diagnose.)
//
// Instead we try to bind() the port.  A port with a live listener answers
// EADDRINUSE; a free port binds (and we close it immediately).  No connection
// is ever made, so the listener never notices the check.
//
#ifndef RQ_PREFLIGHT_H
#define RQ_PREFLIGHT_H

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

inline bool portIsListening(const std::string& host, int port) {
    const int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return false;

    // Deliberately NO SO_REUSEADDR: we want the bind to fail while somebody
    // is listening.  With SO_REUSEADDR the probe could succeed against a
    // socket in TIME_WAIT and we would report a live listener as dead.
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(static_cast<uint16_t>(port));
    if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) <= 0) { close(fd); return false; }

    const bool bound = bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0;
    const int  err   = errno;
    close(fd);

    // bind succeeded  -> nothing was listening there
    // EADDRINUSE      -> a listener holds the port (what we want)
    // anything else   -> cannot tell; assume it is up rather than block the run
    if (bound) return false;
    return err == EADDRINUSE || err != EACCES;
}

// Exits the process with a usable message if any of the k ports is dead.
inline void requireListeners(const std::string& host, int base_port, uint32_t k,
                             const char* what = "this experiment") {
    std::vector<int> dead;
    for (uint32_t i = 0; i < k; ++i)
        if (!portIsListening(host, base_port + static_cast<int>(i)))
            dead.push_back(base_port + static_cast<int>(i));

    if (dead.empty()) {
        std::cout << "Listeners: " << k << " up on " << host << ":" << base_port
                  << ".." << base_port + static_cast<int>(k) - 1 << "\n";
        return;
    }

    std::cerr << "\nNo storage listener on " << host << ":";
    for (size_t i = 0; i < dead.size(); ++i) std::cerr << (i ? ", " : "") << dead[i];
    std::cerr << "\n"
              << what << " forks one child per shard and each child connects to its\n"
                 "port; NetIOConnector retries that connect forever, so the run would\n"
                 "hang with no output instead of failing.  Start them first:\n\n"
              << "    for p in $(seq " << base_port << " "
              << base_port + static_cast<int>(k) - 1
              << "); do ./build/bin/Servers_MultiRingORAM $p > /tmp/l_$p.log 2>&1 & done\n"
                 "    sleep 2\n\n"
                 "Check with:  lsof -nP -iTCP:" << base_port << "-"
              << base_port + static_cast<int>(k) - 1 << " -sTCP:LISTEN\n";
    std::exit(1);
}

#endif  // RQ_PREFLIGHT_H