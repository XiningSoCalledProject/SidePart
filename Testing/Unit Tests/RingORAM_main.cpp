//
// RingORAM_main.cpp - FIXED VERSION
// Created by Xining Yuan on 6/12/25.
//

// #include "RingORAM_helper.cpp"
#include "RingORAM_helper.h"
#include "Config.h"
#include "TpccSchema.h"
#include "DataPath.h"

int main() {
    srand((uint32_t)time(NULL));
    std::cout << "[Start RingORAM Tests]" << std::endl;

    int port = 54325; // 54325; 8888;
    auto conn = new NetIOConnector(server_host, port, "RingORAM");

    // const std::string base_path = "../Testing/Unit Tests/my_tpcc_input/";
    const std::string base_path = departDataPath("Testing/Unit Tests/my_tpcc_input/");

    testTable(base_path + "warehouse.csv", Tpcc::schema_str_warehouse, conn);
    testTable(base_path + "district.csv", Tpcc::schema_str_district, conn);
    testTable(base_path + "customer.csv", Tpcc::schema_str_customers, conn);

    /*
    testTable(base_path + "history.csv", Tpcc::schema_str_history, conn);
    testTable(base_path + "order.csv", Tpcc::schema_str_orders, conn);
    testTable(base_path + "new_order.csv", Tpcc::schema_str_new_orders, conn);
    testTable(base_path + "order_line.csv", Tpcc::schema_str_order_line, conn);
    testTable(base_path + "stock.csv", Tpcc::schema_str_stock, conn);
    testTable(base_path + "item.csv", Tpcc::schema_str_item, conn);
    */

    delete conn;
    std::cout << "[RingORAM Tests Complete]" << std::endl;
    return 0;
}