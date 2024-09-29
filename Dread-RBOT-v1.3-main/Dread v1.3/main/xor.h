#pragma once
//i will add more to this in future updates
#include <unistd.h>
#include <stdint.h>
#include <stdarg.h>
struct table_value
{
    char *val;
    uint16_t val_len;
};

/* Defining Table Keys */

#define XOR_EXEC_SUCCESS 1
#define MAX_XOR_KEYS 1


void table_init(void);
void table_unlock_val(uint8_t);
void table_lock_val(uint8_t);
char *table_retrieve_val(int, int *);

static void add_entry(uint8_t, char *, int);
static void toggle_obf(uint8_t);
