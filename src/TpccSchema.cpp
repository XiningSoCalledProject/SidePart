#include "TpccSchema.h"

namespace Tpcc {
    std::string schema_str_customers = "customers.c_id:int32, "
                                       "customers.c_d_id:int8, "
                                       "customers.c_w_id:int16, "
                                       "customers.c_first:varchar(16), "
                                       "customers.c_middle:varchar(2), "
                                       "customers.c_last:varchar(16), "
                                       "customers.c_street_1:varchar(20), "
                                       "customers.c_street_2:varchar(20), "
                                       "customers.c_city:varchar(20), "
                                       "customers.c_state:varchar(2), "
                                       "customers.c_zip:varchar(9), "
                                       "customers.c_phone:varchar(16), "
                                       "customers.c_since:date, "
                                       "customers.c_credit:varchar(2), "
                                       "customers.c_credit_lim:numeric(12-2), "
                                       "customers.c_discount:numeric(4-2), "
                                       "customers.c_balance:numeric(12-2), "
                                       "customers.c_ytd_payment:numeric(12-2), "
                                       "customers.c_payment_cnt:numeric(4-0), "
                                       "customers.c_delivery_cnt:numeric(4-0), "
                                       "customers.c_data:varchar(500))";

    std::string schema_str_orders = "(orders.o_id:int32, "
                                    "orders.o_d_id:int8, "
                                    "orders.o_w_id:int16, "
                                    "orders.o_c_id:int32, "
                                    "orders.o_entry_d:date, "
                                    "orders.o_carrier_id:int8, "
                                    "orders.o_ol_cnt:int8, "
                                    "orders.o_all_local:int8)";

    std::string schema_str_warehouse = "(warehouse.w_id:int16,"
                                       "warehouse.w_name:varchar(10), "
                                       "warehouse.w_street_1:varchar(20), "
                                       "warehouse.w_street_2:varchar(20), "
                                       "warehouse.w_city:varchar(20), "
                                       "warehouse.w_state:char(2), "
                                       "warehouse.w_zip:char(9), "
                                       "warehouse.w_tax:numeric(4-2), "
                                       "warehouse.w_ytd:numeric(12-2))";

    std::string schema_str_district = "(district.d_id:int8, "
                                      "district.d_w_id:int16, "
                                      "district.d_name:varchar(10), "
                                      "district.d_street_1:varchar(20), "
                                      "district.d_street_2:varchar(20), "
                                      "district.d_city:varchar(20), "
                                      "district.d_state:char(2), "
                                      "district.d_zip:char(9), "
                                      "district.d_tax:numeric(4-4), "
                                      "district.d_ytd:numeric(12-2), "
                                      "district.d_next_o_id:int32)";

    std::string schema_str_history = "(history.h_c_id:int32, "
                                     "history.h_c_d_id:int8, "
                                     "history.h_c_w_id:int16, "
                                     "history.h_d_id:int8, "
                                     "history.h_w_id:int16, "
                                     "history.h_date:date, "
                                     "history.h_amount:numeric(6-2), "
                                     "history.h_data:varchar(24))";

    std::string schema_str_new_orders = "(new_orders.no_o_id:int32, "
                                        "new_orders.no_d_id:int8, "
                                        "new_orders.no_w_id:int16)";

    std::string schema_str_order_line = "(order_line.ol_o_id:int32, "
                                        "order_line.ol_d_id:int8, "
                                        "order_line.ol_w_id:int16, "
                                        "order_line.ol_number:int8, "
                                        "order_line.ol_i_id:int32, "
                                        "order_line.ol_supply_w_id:int16, "
                                        "order_line.ol_delivery_d:date, "
                                        "order_line.ol_quantity:int8, "
                                        "order_line.ol_amount:numeric(6-2), "
                                        "order_line.ol_dist_info:char(24))";

    std::string schema_str_item = "(item.i_id:int32, "
                                  "item.i_im_id:int32, "
                                  "item.i_name:varchar(24), "
                                  "item.i_price:numeric(5-2), "
                                  "item.i_data:varchar(50))";

    std::string  schema_str_stock = "(stock.s_i_id:int32, "
                                    "stock.s_w_id:int16, "
                                    "stock.s_quantity:int16, "
                                    "stock.s_dist_01:char(24), "
                                    "stock.s_dist_02:char(24), "
                                    "stock.s_dist_03:char(24), "
                                    "stock.s_dist_04:char(24), "
                                    "stock.s_dist_05:char(24), "
                                    "stock.s_dist_06:char(24), "
                                    "stock.s_dist_07:char(24), "
                                    "stock.s_dist_08:char(24), "
                                    "stock.s_dist_09:char(24), "
                                    "stock.s_dist_10:char(24), "
                                    "stock.s_ytd:numeric(8-0), "
                                    "stock.s_order_cnt:int16, "
                                    "stock.s_remote_cnt:int16, "
                                    "stock.s_data:varchar(50))";
}