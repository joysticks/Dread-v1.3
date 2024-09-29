#pragma once

#include <unistd.h>
#include <stdint.h>
#include <stdarg.h>

#define FALSE 0
#define TRUE 1
#define STDOUT 1

typedef char BOOL;

#define INET_ADDR(o1,o2,o3,o4) (htonl((o1 << 24) | (o2 << 16) | (o3 << 8) | (o4 << 0)))

//experimental

/*
#define CNC_DOMAIN INET_ADDR(1,1,1,1)
#define CNC_PORT 1337;
*/

typedef uint32_t ipv4_t;
typedef uint16_t port_t;
ipv4_t LOCAL_ADDR;
