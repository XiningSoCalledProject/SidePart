/*
 * This file is released under the terms of the Artistic License.
 * Please see the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2002 Open Source Development Labs, Inc.
 *               2002-2010 by anonymous person;
 *
 * Based on TPC-C Standard Specification Revision 5.0.
 */

#include <cstdlib>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <getopt.h>
#include <stdarg.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>

#include <iostream>
#include <sstream>
#include <vector>
#include "generate_data.h"

//#include <pthread.h>
//#include "common.h"
//#include "db.h"
//#include "dbc.h"
//#include "logging.h"

#define DATAFILE_EXT ".data"
#define ENV_DBCLIENT_NAME "DGEN_DBCLIENT_COMMAND"

using namespace std;

static __thread unsigned int seed = 0;
const char *c_last_syl[C_LAST_SYL_MAX];

//typedef union output_stream_t
//{
//    FILE *file;
//    struct loader_stream_t *stream;
//} output_stream;
//
//struct stream_operation_t
//{
//    output_stream (*open_stream)(int worker_id, const char *table_name);
//    int (*write_to_stream)(output_stream stream, const char *fmt, va_list ap);
//    void (*close_stream)(output_stream stream);
//};
//
//static output_stream open_file_stream(int worker_id, const char *table_name);
//static int write_to_file_stream(output_stream stream, const char *fmt, va_list ap);
//static void close_file_stream(output_stream stream);

//struct stream_operation_t stream_operation = {
//        open_file_stream,
//        write_to_file_stream,
//        close_file_stream
//};

int customers = CUSTOMER_CARDINALITY;
int orders = ORDER_CARDINALITY;

int jobs = 1;

char delimiter = ',';
char null_str[16] = "\"NULL\"";
char *dbclient_command = NULL;


int get_random(int max)
{
    return rand_r(&seed) % max;
}

void escape_me(char *str)
{
    /* Shouldn't need a buffer bigger than this. */
    char buffer[4096] = "";
    int i = 0;
    int j = 0;
    int k = 0;

    strcpy(buffer, str);
    i = strlen(buffer);
    for (k = 0; k <= i; k++) {
        if (buffer[k] == '\\') {
            str[j++] = '\\';
        }
        str[j++] = buffer[k];
    }
}

static void quickdie(const char *errmsg)
{
    printf("FATAL: %s\n", errmsg);
    exit(1);
}

static void ostprintf(stringstream& stream, const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);
    char buf[1024];
    sprintf(buf, fmt, args);

    stream << string(buf);

    va_end(args);
}

void print_timestamp(stringstream& stream, struct tm *date)
{
    ostprintf(stream, "%04d-%02d-%02d %02d:%02d:%02d",
              date->tm_year + 1900, date->tm_mon + 1, date->tm_mday,
              date->tm_hour, date->tm_min, date->tm_sec);
}

static void pr_start_4(const char *table, int worker_id, int start, int end)
{
    printf("Worker %d is generating %s table data "
           "for warehouse [%d, %d] ...\n", worker_id, table, start, end);
}

static void pr_start_1(const char *table)
{
    printf("Generating %s table data ...\n", table);
}

static void pr_end(const char *table)
{
    printf("Finished %s table data ...\n", table);
}

char output_path[256] = "";
char a_string_char[A_STRING_CHAR_LEN];
const char *n_string_char = "0123456789";
const char *l_string_char =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

/* Clause 4.3.2.2.  */
void get_a_string(char *a_string, int x, int y)
{
    int length;
    int i;

    length = x + get_random(y - x + 1) + 1;
    a_string[length - 1] = '\0';

    for (i = 0; i < length - 1; i++)
    {
        a_string[i] = a_string_char[get_random(A_STRING_CHAR_LEN - 1)];
    }

}

/* Clause 4.3.2.3 */
int get_c_last(char *c_last, int i)
{
    char tmp[4];

    c_last[0] = '\0';

    if (i < 0 || i > 999)
    {
        return ERROR;
    }

    /* Ensure the number is padded with leading 0's if it's less than 100. */
    sprintf(tmp, "%03d", i);

    strcat(c_last, c_last_syl[tmp[0] - '0']);
    strcat(c_last, c_last_syl[tmp[1] - '0']);
    strcat(c_last, c_last_syl[tmp[2] - '0']);
    return OK;
}

void get_l_string(char *a_string, int x, int y)
{
    int length;
    int i;

    length = x + get_random(y - x + 1) + 1;
    a_string[length - 1] = '\0';

    for (i = 0; i < length - 1; i++)
    {
        a_string[i] = l_string_char[get_random(L_STRING_CHAR_LEN - 1)];
    }

}

/* Clause 4.3.2.2.  */
void get_n_string(char *n_string, int x, int y)
{
    int length;
    int i;

    length = x + get_random(y - x + 1) + 1;
    n_string[length - 1] = '\0';

    for (i = 0; i < length - 1; i++)
    {
        n_string[i] = n_string_char[get_random(N_STRING_CHAR_LEN)];
    }

}

/* Clause 2.1.6 */
int get_nurand(int a, int x, int y)
{
    return ((get_random(a + 1) | (x + get_random(y + 1))) % (y - x + 1)) + x;
}

/* Return a number from 0 to max. */
double get_percentage()
{
    return (double) rand_r(&seed) / (double) RAND_MAX;
}

void set_random_seed(unsigned int s)
{
    seed = s;
}

/* Clause 4.3.3.1 */
void gen_customers(int worker_id, int start, int end, vector<string>& table_customer)
{
    const char *table = "customer";

    int i, j, k;
    char a_string[1024];
    struct tm *tm1;
    time_t t1;

    set_random_seed(0);
    pr_start_4(table, worker_id, start, end);

    for (i = start; i <= end; i++) {
        for (j = 0; j < DISTRICT_CARDINALITY; j++) {
            for (k = 0; k < customers; k++) {
                stringstream output;

                /* c_id */
                ostprintf(output, "%d", k + 1);
                ostprintf(output, "%c", delimiter);

                /* c_d_id */
                ostprintf(output, "%d", j + 1);
                ostprintf(output, "%c", delimiter);

                /* c_w_id */
                ostprintf(output, "%d", i);
                ostprintf(output, "%c", delimiter);

                /* c_first */
                get_a_string(a_string, 8, 16);
                escape_me(a_string);
                ostprintf(output, "%s", a_string);
                ostprintf(output, "%c", delimiter);

                /* c_middle */
                ostprintf(output, "OE");
                ostprintf(output, "%c", delimiter);

                /* c_last Clause 4.3.2.7 */
                if (k < 1000) {
                    get_c_last(a_string, k);
                } else {
                    get_c_last(a_string, get_nurand(255, 0, 999));
                }
                escape_me(a_string);
                ostprintf(output, "%s", a_string);
                ostprintf(output, "%c", delimiter);

                /* c_street_1 */
                get_a_string(a_string, 10, 20);
                escape_me(a_string);
                ostprintf(output, "%s", a_string);
                ostprintf(output, "%c", delimiter);

                /* c_street_2 */
                get_a_string(a_string, 10, 20);
                escape_me(a_string);
                ostprintf(output, "%s", a_string);
                ostprintf(output, "%c", delimiter);

                /* c_city */
                get_a_string(a_string, 10, 20);
                escape_me(a_string);
                ostprintf(output, "%s", a_string);
                ostprintf(output, "%c", delimiter);

                /* c_state */
                get_l_string(a_string, 2, 2);
                ostprintf(output, "%s", a_string);
                ostprintf(output, "%c", delimiter);

                /* c_zip */
                get_n_string(a_string, 4, 4);
                ostprintf(output, "%s11111", a_string);
                ostprintf(output, "%c", delimiter);

                /* c_phone */
                get_n_string(a_string, 16, 16);
                ostprintf(output, "%s", a_string);
                ostprintf(output, "%c", delimiter);

                /* c_since */
                /*
                 * Milliseconds are not calculated.  This
                 * should also be the time when the data is
                 * loaded, I think.
                 */
                time(&t1);
                tm1 = localtime(&t1);
                print_timestamp(output, tm1);
                ostprintf(output, "%c", delimiter);

                /* c_credit */
                if (get_percentage() < .10) {
                    ostprintf(output, "BC");
                } else {
                    ostprintf(output, "GC");
                }
                ostprintf(output, "%c", delimiter);

                /* c_credit_lim */
                ostprintf(output, "50000.00");
                ostprintf(output, "%c", delimiter);

                /* c_discount */
                ostprintf(output, "0.%04d", get_random(5000));
                ostprintf(output, "%c", delimiter);

                /* c_balance */
                ostprintf(output, "-10.00");
                ostprintf(output, "%c", delimiter);

                /* c_ytd_payment */
                ostprintf(output, "10.00");
                ostprintf(output, "%c", delimiter);

                /* c_payment_cnt */
                ostprintf(output, "1");
                ostprintf(output, "%c", delimiter);

                /* c_delivery_cnt */
                ostprintf(output, "0");
                ostprintf(output, "%c", delimiter);

                /* c_data */
                get_a_string(a_string, 300, 500);
                escape_me(a_string);
                ostprintf(output, "%s", a_string);

                ostprintf(output, "\n");

                table_customer.push_back(output.str());
            }
        }
    }

    pr_end(table);
}

/* Clause 4.3.3.1 */
void gen_orders(int worker_id, int start, int end, vector<string>& table_orders)
{
    const char *order_table = "orders";
    const char *order_line_table = "order_line";

    int i, j, k, l;
    char a_string[64];
    struct tm *tm1;
    time_t t1;

    struct node_t {
        int value;
        struct node_t *next;
    };
    struct node_t *head;
    struct node_t *current;
    struct node_t *prev;
    struct node_t *new_node;
    int iter;

    int o_ol_cnt;

    set_random_seed(0);
    pr_start_4("orders and order_line", worker_id, start, end);

    for (i = start; i <= end; i++) {
        for (j = 0; j < DISTRICT_CARDINALITY; j++) {
            /*
             * Create a random list of numbers from 1 to customers for o_c_id.
             */
            head = (struct node_t *) malloc(sizeof(struct node_t));
            head->value = 1;
            head->next = NULL;
            for (k = 2; k <= customers; k++) {
                current = prev = head;

                /* Find a random place in the list to insert a number. */
                iter = get_random(k - 1);
                while (iter > 0) {
                    prev = current;
                    current = current->next;
                    --iter;
                }

                /* Insert the number. */
                new_node = (struct node_t *) malloc(sizeof(struct node_t));
                if (current == prev) {
                    /* Insert at the head of the list. */
                    new_node->next = head;
                    head = new_node;
                } else if (current == NULL) {
                    /* Insert at the tail of the list. */
                    prev->next = new_node;
                    new_node->next = NULL;
                } else {
                    /* Insert somewhere in the middle of the list. */
                    prev->next = new_node;
                    new_node->next = current;
                }
                new_node->value = k;
            }

            current = head;
            for (k = 0; k < orders; k++) {
                stringstream order, order_line;

                /* o_id */
                ostprintf(order, "%d", k + 1);
                ostprintf(order, "%c", delimiter);

                /* o_d_id */
                ostprintf(order, "%d", j + 1);
                ostprintf(order, "%c", delimiter);

                /* o_w_id */
                ostprintf(order, "%d", i);
                ostprintf(order, "%c", delimiter);

                /* o_c_id */
                ostprintf(order, "%d", current->value);
                ostprintf(order, "%c", delimiter);
                current = current->next;

                /* o_entry_d */
                /*
                 * Milliseconds are not calculated.  This
                 * should also be the time when the data is
                 * loaded, I think.
                 */
                time(&t1);
                tm1 = localtime(&t1);
                print_timestamp(order, tm1);
                ostprintf(order, "%c", delimiter);

                if (k < 2101) {
                    ostprintf(order, "%d", get_random(9) + 1);
                } else {
                    ostprintf(order, "%s", null_str);
                }
                ostprintf(order, "%c", delimiter);

                /* o_ol_cnt */
                o_ol_cnt = get_random(10) + 5;
                ostprintf(order, "%d", o_ol_cnt);
                ostprintf(order, "%c", delimiter);

                /* o_all_local */
                ostprintf(order, "1");

                ostprintf(order, "\n");

                /*
                 * Generate data in the order-line table for
                 * this order.
                 */
                for (l = 0; l < o_ol_cnt; l++) {
                    /* ol_o_id */
                    ostprintf(order_line, "%d", k + 1);
                    ostprintf(order_line, "%c", delimiter);

                    /* ol_d_id */
                    ostprintf(order_line, "%d", j + 1);
                    ostprintf(order_line, "%c", delimiter);

                    /* ol_w_id */
                    ostprintf(order_line, "%d", i);
                    ostprintf(order_line, "%c", delimiter);

                    /* ol_number */
                    ostprintf(order_line, "%d", l + 1);
                    ostprintf(order_line, "%c", delimiter);

                    /* ol_i_id */
                    ostprintf(order_line, "%d",
                              get_random(ITEM_CARDINALITY - 1) + 1);
                    ostprintf(order_line, "%c", delimiter);

                    /* ol_supply_w_id */
                    ostprintf(order_line, "%d", i);
                    ostprintf(order_line, "%c", delimiter);

                    if (k < 2101) {
                        /*
                         * Milliseconds are not
                         * calculated.  This should
                         * also be the time when the
                         * data is loaded, I think.
                         */
                        time(&t1);
                        tm1 = localtime(&t1);
                        print_timestamp(order_line, tm1);
                    } else {
                        ostprintf(order_line, "%s", null_str);
                    }

                    ostprintf(order_line, "%c", delimiter);

                    /* ol_quantity */
                    ostprintf(order_line, "5");
                    ostprintf(order_line, "%c", delimiter);

                    /* ol_amount */
                    if (k < 2101) {
                        ostprintf(order_line, "0.00");
                    } else {
                        ostprintf(order_line, "%f",
                                  (double) (get_random(999998) + 1) / 100.0);
                    }
                    ostprintf(order_line, "%c", delimiter);

                    /* ol_dist_info */
                    get_l_string(a_string, 24, 24);
                    ostprintf(order_line, "%s", a_string);

                    ostprintf(order_line, "\n");
                }

                table_orders.push_back(order.str());
            }
            while (head != NULL) {
                current = head;
                head = head->next;
                free(current);
            }
        }
    }
    pr_end("orders and order_line");
}
