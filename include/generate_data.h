#ifndef SEAL_ORAM_NETIO_GENERATE_DATA_H
#define SEAL_ORAM_NETIO_GENERATE_DATA_H

#endif //SEAL_ORAM_NETIO_GENERATE_DATA_H

#define ERROR 0
#define OK 1
#define EXIT_CODE 2
#define ERROR_SOCKET_CLOSED 3
#define STATUS_ROLLBACK 4
#define ERROR_RECEIVE_TIMEOUT 5

#define A_STRING_CHAR_LEN 128
#define L_STRING_CHAR_LEN 52
#define N_STRING_CHAR_LEN 10
#define TIMESTAMP_LEN 28

#define CUSTOMER_CARDINALITY 3000
#define DISTRICT_CARDINALITY 10
#define ITEM_CARDINALITY 100000
#define ORDER_CARDINALITY 3000

#define C_ID_UNKNOWN 0
#define C_LAST_SYL_MAX 10
#define D_ID_MAX 10
#define O_OL_CNT_MAX 15
#define O_CARRIER_ID_MAX 10

// extern const char *c_last_syl[C_LAST_SYL_MAX];

#include <vector>
#include <string>
void gen_customers(int worker_id, int start, int end, std::vector<std::string>& table_customer);
void gen_orders(int worker_id, int start, int end, std::vector<std::string>& table_orders);