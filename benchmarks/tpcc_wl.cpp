#include "global.h"
#include "helper.h"
#include "tpcc.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "index_hash.h"
#include "index_btree.h"
#include "tpcc_helper.h"
#include "row.h"
#include "query.h"
#include "txn.h"
#include "mem_alloc.h"
#include "tpcc_const.h"

RC tpcc_wl::init() {
	workload::init();
	string path = "./benchmarks/";
#if TPCC_SMALL
	path += "TPCC_short_schema.txt";
#else
	path += "TPCC_full_schema.txt";
#endif
	cout << "reading schema file: " << path << endl;
	init_schema( path.c_str() );
	cout << "TPCC schema initialized" << endl;
	init_table();
	next_tid = 0;
	return RCOK;
}

RC tpcc_wl::init_schema(const char * schema_file) {
	workload::init_schema(schema_file);
	t_warehouse = tables["WAREHOUSE"];
	t_district = tables["DISTRICT"];
	t_customer = tables["CUSTOMER"];
	t_history = tables["HISTORY"];
	t_neworder = tables["NEW-ORDER"];
	t_order = tables["ORDER"];
	t_orderline = tables["ORDER-LINE"];
	t_item = tables["ITEM"];
	t_stock = tables["STOCK"];

	i_item = indexes["ITEM_IDX"];
	i_warehouse = indexes["WAREHOUSE_IDX"];
	i_district = indexes["DISTRICT_IDX"];
	i_customer_id = indexes["CUSTOMER_ID_IDX"];
	i_customer_last = indexes["CUSTOMER_LAST_IDX"];
	i_stock = indexes["STOCK_IDX"];
	return RCOK;
}

RC tpcc_wl::init_table() {
	num_wh = g_num_wh;

/******** fill in data ************/
// data filling process:
//- item
//- wh
//	- stock
// 	- dist
//  	- cust
//	  	- hist
//		- order 
//		- new order
//		- order line
/**********************************/

#if PIM_ENABLE
	u_int32_t tables_cnt = 6;
	this->st = new storage();
	// there are 6 tables
	st->init(tables_cnt);

	init_table_size();
	init_st_wh();
	init_st_item();
	init_st_dist();
	init_st_stock();
	char last[] = "CUSTOMER_LAST"; 
	char id[] = "CUSTOMER_ID";
	init_st_cust(last, 14);
	init_st_cust(id, 12);
#endif

	tpcc_buffer = new drand48_data * [g_num_wh];
	pthread_t * p_thds = new pthread_t[g_num_wh - 1];
	for (uint32_t i = 0; i < g_num_wh - 1; i++) 
		pthread_create(&p_thds[i], NULL, threadInitWarehouse, this);
	threadInitWarehouse(this);
	for (uint32_t i = 0; i < g_num_wh - 1; i++) 
		pthread_join(p_thds[i], NULL);

	printf("TPCC Data Initialization Complete!\n");
	return RCOK;
}

#if PIM_ENABLE

RC tpcc_wl::init_table_size() {
	this->l_warehouse = g_num_wh;
	this->l_item = g_max_items;
	this->l_customer = g_cust_per_dist * DIST_PER_WARE;
	this->l_stock = g_max_items;
	this->l_district = DIST_PER_WARE;
	return RCOK;
}

RC tpcc_wl::init_st_wh() {
	u_int32_t row;
	u_int32_t col;
	u_int32_t bank_cnt;
	u_int32_t f_cnt = 9;

	bank_cnt = 1;
	row = DEVICE_CNT_PER_BANK;
	col = 3;
	char name[] = "WAREHOUSE";
	u_int32_t wh_i[bank_cnt][row][col] = {{{5,0,0},{5,4,0},{4,3,0},{3,0,0},{3,2,0},{7,9,0},{9,8,1},{1,6,0}}};
	u_int32_t wh_w[bank_cnt][row][col] = {{{14,0,0},{6,8,0},{12,2,0},{14,0,0},{4,10,0},{9,5,0},{3,8,3},{5,2,0}}};

	// 创建三级指针结构
    u_int32_t ***dynamic_wh_i = new u_int32_t **[bank_cnt];
	for (u_int32_t i = 0; i < bank_cnt; ++i) {
        dynamic_wh_i[i] = new u_int32_t *[row];
        for (u_int32_t j = 0; j < row; ++j) {
            dynamic_wh_i[i][j] = new u_int32_t[col];
            for (u_int32_t k = 0; k < col; ++k) {
                dynamic_wh_i[i][j][k] = wh_i[i][j][k];  // 复制数据
            }
        }
    }

	// 创建三级指针结构
    u_int32_t ***dynamic_wh_w = new u_int32_t **[bank_cnt];
	for (u_int32_t i = 0; i < bank_cnt; ++i) {
        dynamic_wh_w[i] = new u_int32_t *[row];
        for (u_int32_t j = 0; j < row; ++j) {
            dynamic_wh_w[i][j] = new u_int32_t[col];
            for (u_int32_t k = 0; k < col; ++k) {
                dynamic_wh_w[i][j][k] = wh_w[i][j][k];  // 复制数据
            }
        }
    }

	table_s * t = st->insert_table(name, sizeof(name) / sizeof(name[0]), dynamic_wh_i, dynamic_wh_w, bank_cnt, row, col, f_cnt);
	t->init_table_size(this->l_warehouse);

	// 释放内存
	free_arr(dynamic_wh_i, bank_cnt, row);
	free_arr(dynamic_wh_w, bank_cnt, row);

#if PIM_INIT_DEBUG
	printf("debug log:\r\n");
	printf("<=========table of warehouse=========>\r\n");
	u_int32_t device_width = 14;
	assert(t->fields_cnt == f_cnt);
	assert(t->parts[0]->width == device_width);
	for (u_int32_t f = 0; f < t->fields_cnt; f++) {
		u_int32_t part_sum = 0;
		for (u_int32_t fc = 0; fc < t->fields[f]->cnt; fc++) {
			part_sum += t->fields[f]->sizes[fc];
		}
		printf("%d ", part_sum);
	}
	printf("\r\ndebug end\r\n");
#endif

	return RCOK;
}

RC tpcc_wl::init_st_item() {
	u_int32_t row;
	u_int32_t col;
	u_int32_t bank_cnt;
	u_int32_t f_cnt = 5;

	bank_cnt = 2;
	row = DEVICE_CNT_PER_BANK;
	col = 2;
	char name[] = "ITEM";
	u_int32_t wh_i[bank_cnt][row][col] = {{{4,0},{1,0},{5,0},{5,0},{5,0},{5,0},{5,0},{5,0}},{{3,0},{3,0},{3,0},{3,0},{3,2},{2,0},{2,5},{0,0}}};
	u_int32_t wh_w[bank_cnt][row][col] = {{{8,0},{8,0},{8,0},{8,0},{8,0},{8,0},{8,0},{8,0}},{{5,0},{5,0},{5,0},{5,0},{4,1},{5,0},{2,2},{0,0}}};

	// 创建三级指针结构
    u_int32_t ***dynamic_item_i = new u_int32_t **[bank_cnt];
	for (u_int32_t i = 0; i < bank_cnt; ++i) {
        dynamic_item_i[i] = new u_int32_t *[row];
        for (u_int32_t j = 0; j < row; ++j) {
            dynamic_item_i[i][j] = new u_int32_t[col];
            for (u_int32_t k = 0; k < col; ++k) {
                dynamic_item_i[i][j][k] = wh_i[i][j][k];  // 复制数据
            }
        }
    }

	// 创建三级指针结构
    u_int32_t ***dynamic_item_w = new u_int32_t **[bank_cnt];
	for (u_int32_t i = 0; i < bank_cnt; ++i) {
        dynamic_item_w[i] = new u_int32_t *[row];
        for (u_int32_t j = 0; j < row; ++j) {
            dynamic_item_w[i][j] = new u_int32_t[col];
            for (u_int32_t k = 0; k < col; ++k) {
                dynamic_item_w[i][j][k] = wh_w[i][j][k];  // 复制数据
            }
        }
    }

	table_s * t = st->insert_table(name, sizeof(name) / sizeof(name[0]), dynamic_item_i, dynamic_item_w, bank_cnt, row, col, f_cnt);
	t->init_table_size(this->l_item);

	// 释放内存
	free_arr(dynamic_item_i, bank_cnt, row);
	free_arr(dynamic_item_w, bank_cnt, row);

	return RCOK;
}

RC tpcc_wl::init_st_dist() {
	u_int32_t row;
	u_int32_t col;
	u_int32_t bank_cnt;
	u_int32_t f_cnt = 11;

	bank_cnt = 1;
	row = DEVICE_CNT_PER_BANK;
	col = 3;
	char name[] = "DISTRICT";
	u_int32_t wh_i[bank_cnt][row][col] = {{{6,0,0},{6,5,0},{5,4,0},{4,3,0},{3,8,11},{11,10,9},{9,2,1},{1,7,0}}};
	u_int32_t wh_w[bank_cnt][row][col] = {{{16,0,0},{4,12,0},{8,8,0},{12,4,0},{6,9,1},{7,8,1},{7,8,1},{7,2,0}}};

	// 创建三级指针结构
    u_int32_t ***dynamic_dist_i = new u_int32_t **[bank_cnt];
	for (u_int32_t i = 0; i < bank_cnt; ++i) {
        dynamic_dist_i[i] = new u_int32_t *[row];
        for (u_int32_t j = 0; j < row; ++j) {
            dynamic_dist_i[i][j] = new u_int32_t[col];
            for (u_int32_t k = 0; k < col; ++k) {
                dynamic_dist_i[i][j][k] = wh_i[i][j][k];  // 复制数据
            }
        }
    }

	// 创建三级指针结构
    u_int32_t ***dynamic_dist_w = new u_int32_t **[bank_cnt];
	for (u_int32_t i = 0; i < bank_cnt; ++i) {
        dynamic_dist_w[i] = new u_int32_t *[row];
        for (u_int32_t j = 0; j < row; ++j) {
            dynamic_dist_w[i][j] = new u_int32_t[col];
            for (u_int32_t k = 0; k < col; ++k) {
                dynamic_dist_w[i][j][k] = wh_w[i][j][k];  // 复制数据
            }
        }
    }

	table_s * t = st->insert_table(name, sizeof(name) / sizeof(name[0]), dynamic_dist_i, dynamic_dist_w, bank_cnt, row, col, f_cnt);
	t->init_table_size(this->l_district);

	// 释放内存
	free_arr(dynamic_dist_i, bank_cnt, row);
	free_arr(dynamic_dist_w, bank_cnt, row);

	return RCOK;
}

RC tpcc_wl::init_st_stock() {
	u_int32_t row;
	u_int32_t col;
	u_int32_t bank_cnt;

	char name[] = "STOCK";
#if TPCC_SMALL
	bank_cnt = 1;
	row = DEVICE_CNT_PER_BANK;
	col = 1;

	u_int32_t f_cnt = 4;
	u_int32_t wh_i[bank_cnt][row][col] = {{{2},{1},{4},{3},{0},{0},{0},{0}}};
	u_int32_t wh_w[bank_cnt][row][col] = {{{8},{8},{8},{8},{0},{0},{0},{0}}};
#else
	bank_cnt = 2;
	row = DEVICE_CNT_PER_BANK;
	col = 5;

	u_int32_t f_cnt = 17;
	u_int32_t wh_i[bank_cnt][row][col] = {{{2,0,0,0,0},{1,0,0,0,0},{17,0,0,0,0},{17,0,0,0,0},{17,0,0,0,0},{17,0,0,0,0},{17,0,0,0,0},{17,0,0,0,0}},{{9,13,0,0,0},{13,12,0,0,0},{12,11,10,0,0},{10,8,0,0,0},{8,7,6,0,0},{6,5,0,0,0},{5,4,3,0,0},{3,15,14,16,17}}};
	u_int32_t wh_w[bank_cnt][row][col] = {{{8,0,0,0,0},{8,0,0,0,0},{8,0,0,0,0},{8,0,0,0,0},{8,0,0,0,0},{8,0,0,0,0},{8,0,0,0,0},{8,0,0,0,0}},{{24,11,0,0,0},{13,22,0,0,0},{2,24,9,0,0},{15,20,0,0,0},{4,24,7,0,0},{17,18,0,0,0},{6,24,5,0,0},{3,8,8,8,2}}};
#endif
	// 创建三级指针结构
    u_int32_t ***dynamic_stock_i = new u_int32_t **[bank_cnt];
	for (u_int32_t i = 0; i < bank_cnt; ++i) {
        dynamic_stock_i[i] = new u_int32_t *[row];
        for (u_int32_t j = 0; j < row; ++j) {
            dynamic_stock_i[i][j] = new u_int32_t[col];
            for (u_int32_t k = 0; k < col; ++k) {
                dynamic_stock_i[i][j][k] = wh_i[i][j][k];  // 复制数据
            }
        }
    }

	// 创建三级指针结构
    u_int32_t ***dynamic_stock_w = new u_int32_t **[bank_cnt];
	for (u_int32_t i = 0; i < bank_cnt; ++i) {
        dynamic_stock_w[i] = new u_int32_t *[row];
        for (u_int32_t j = 0; j < row; ++j) {
            dynamic_stock_w[i][j] = new u_int32_t[col];
            for (u_int32_t k = 0; k < col; ++k) {
                dynamic_stock_w[i][j][k] = wh_w[i][j][k];  // 复制数据
            }
        }
    }

	table_s * t = st->insert_table(name, sizeof(name) / sizeof(name[0]), dynamic_stock_i, dynamic_stock_w, bank_cnt, row, col, f_cnt);
	t->init_table_size(this->l_stock);

	// 释放内存
	free_arr(dynamic_stock_i, bank_cnt, row);
	free_arr(dynamic_stock_w, bank_cnt, row);

	return RCOK;
}

RC tpcc_wl::init_st_cust(char * name, u_int64_t name_len) {
	u_int32_t row;
	u_int32_t col;
	u_int32_t bank_cnt;
#if TPCC_SMALL
	u_int32_t f_cnt = 11;

	bank_cnt = 2;
	row = DEVICE_CNT_PER_BANK;
	col = 3;

	u_int32_t wh_i[bank_cnt][row][col] = {{{5,0,0},{9,3,0},{2,1,0},{11,10,0},{8,7,4},{0,0,0},{0,0,0},{0,0,0}},{{6,0,0},{0,0,0},{0,0,0},{0,0,0},{0,0,0},{0,0,0},{0,0,0},{0,0,0}}};
	u_int32_t wh_w[bank_cnt][row][col] = {{{16,0,0},{8,8,0},{8,8,0},{8,8,0},{8,2,2},{0,0,0},{0,0,0},{0,0,0}},{{2,0,0},{0,0,0},{0,0,0},{0,0,0},{0,0,0},{0,0,0},{0,0,0},{0,0,0}}};
#else
	u_int32_t f_cnt = 21;

	bank_cnt = 3;
	row = DEVICE_CNT_PER_BANK;
	col = 10;

	u_int32_t wh_i[bank_cnt][row][col] = {{{9,0,0,0,0,0,0,0,0,0},{12,21,0,0,0,0,0,0,0,0},{6,21,0,0,0,0,0,0,0,0},{17,3,21,0,0,0,0,0,0,0},{2,1,21,0,0,0,0,0,0,0},{21,0,0,0,0,0,0,0,0,0},{21,0,0,0,0,0,0,0,0,0},{21,0,0,0,0,0,0,0,0,0}},{{10,0,0,0,0,0,0,0,0,0},{21,0,0,0,0,0,0,0,0,0},{21,0,0,0,0,0,0,0,0,0},{21,0,0,0,0,0,0,0,0,0},{21,0,0,0,0,0,0,0,0,0},{21,0,0,0,0,0,0,0,0,0},{21,0,0,0,0,0,0,0,0,0},{21,0,0,0,0,0,0,0,0,0}},{{21,0,0,0,0,0,0,0,0,0},{21,0,0,0,0,0,0,0,0,0},{21,0,0,0,0,0,0,0,0,0},{21,0,0,0,0,0,0,0,0,0},{21,0,0,0,0,0,0,0,0,0},{21,0,0,0,0,0,0,0,0,0},{21,8,7,4,0,0,0,0,0,0},{4,11,16,15,13,20,18,19,14,5}}};
	u_int32_t wh_w[bank_cnt][row][col] = {{{20,0,0,0,0,0,0,0,0,0},{16,4,0,0,0,0,0,0,0,0},{16,4,0,0,0,0,0,0,0,0},{8,8,4,0,0,0,0,0,0,0},{8,8,4,0,0,0,0,0,0,0},{20,0,0,0,0,0,0,0,0,0},{20,0,0,0,0,0,0,0,0,0},{20,0,0,0,0,0,0,0,0,0}},{{2,0,0,0,0,0,0,0,0,0},{2,0,0,0,0,0,0,0,0,0},{2,0,0,0,0,0,0,0,0,0},{2,0,0,0,0,0,0,0,0,0},{2,0,0,0,0,0,0,0,0,0},{2,0,0,0,0,0,0,0,0,0},{2,0,0,0,0,0,0,0,0,0},{2,0,0,0,0,0,0,0,0,0}},{{66,0,0,0,0,0,0,0,0,0},{66,0,0,0,0,0,0,0,0,0},{66,0,0,0,0,0,0,0,0,0},{66,0,0,0,0,0,0,0,0,0},{66,0,0,0,0,0,0,0,0,0},{66,0,0,0,0,0,0,0,0,0},{14,20,20,12,0,0,0,0,0,0},{4,9,8,8,8,8,8,8,2,2}}};
#endif
	// 创建三级指针结构
    u_int32_t ***dynamic_cust_i = new u_int32_t **[bank_cnt];
	for (u_int32_t i = 0; i < bank_cnt; ++i) {
        dynamic_cust_i[i] = new u_int32_t *[row];
        for (u_int32_t j = 0; j < row; ++j) {
            dynamic_cust_i[i][j] = new u_int32_t[col];
            for (u_int32_t k = 0; k < col; ++k) {
                dynamic_cust_i[i][j][k] = wh_i[i][j][k];  // 复制数据
            }
        }
    }

	// 创建三级指针结构
    u_int32_t ***dynamic_cust_w = new u_int32_t **[bank_cnt];
	for (u_int32_t i = 0; i < bank_cnt; ++i) {
        dynamic_cust_w[i] = new u_int32_t *[row];
        for (u_int32_t j = 0; j < row; ++j) {
            dynamic_cust_w[i][j] = new u_int32_t[col];
            for (u_int32_t k = 0; k < col; ++k) {
                dynamic_cust_w[i][j][k] = wh_w[i][j][k];  // 复制数据
            }
        }
    }

	table_s * t = st->insert_table(name, name_len, dynamic_cust_i, dynamic_cust_w, bank_cnt, row, col, f_cnt);
	t->init_table_size(this->l_customer);

	// 释放内存
	free_arr(dynamic_cust_i, bank_cnt, row);
	free_arr(dynamic_cust_w, bank_cnt, row);

	return RCOK;
}

void tpcc_wl::free_arr(u_int32_t *** &tar, u_int32_t b_c, u_int32_t d_c) {
	// 释放内存
    for (u_int32_t i = 0; i < b_c; ++i) {
        for (u_int32_t j = 0; j < d_c; ++j) {
            delete[] tar[i][j];
        }
        delete[] tar[i];
    }
    delete[] tar;
}

#endif

RC tpcc_wl::get_txn_man(txn_man *& txn_manager, thread_t * h_thd) {
	txn_manager = (tpcc_txn_man *) _mm_malloc( sizeof(tpcc_txn_man), 64);
	new(txn_manager) tpcc_txn_man();
	txn_manager->init(h_thd, this, h_thd->get_thd_id());
	return RCOK;
}

// TODO ITEM table is assumed to be in partition 0
void tpcc_wl::init_tab_item() {
	void ** data = new void*[5];
	for (UInt32 i = 1; i <= g_max_items; i++) {
		row_t * row;
		uint64_t row_id;
		t_item->get_new_row(row, 0, row_id);
		row->set_primary_key(i);
		row->set_value(I_ID, i);
		u_int64_t i_im_id = URand(1L,10000L, 0);
		row->set_value(I_IM_ID, i_im_id);
		char name[24];
		MakeAlphaString(14, 24, name, 0);
		row->set_value(I_NAME, name);
		u_int64_t i_price = URand(1, 100, 0);
		row->set_value(I_PRICE, i_price);
		char i_data[50];
    	MakeAlphaString(26, 50, i_data, 0);
		// TODO in TPCC, "original" should start at a random position
		if (RAND(10, 0) == 0) 
			strcpy(i_data, "original");
		row->set_value(I_DATA, i_data);
		index_insert(i_item, i, row, 0);

#if PIM_ENABLE
		int idx = 0;
		data[idx++] = &i;
		data[idx++] = &i_im_id;
		data[idx++] = name;
		data[idx++] = &i_price;
		data[idx++] = i_data;

		table_s * t;
		u_int64_t storage_index = 0;
		char t_n[] = "ITEM";

		st->get_table(t_n, t);
		t->insert_tuple(data, storage_index);

#if DETLA_STORAGE_ENABLE
		row->init_detla_buffer(storage_index, t);
#endif
#endif
	}
	delete[] data;
}

void tpcc_wl::init_tab_wh(uint32_t wid) {
	void ** data = new void*[9];
	assert(wid >= 1 && wid <= g_num_wh);
	row_t * row;
	uint64_t row_id;
	t_warehouse->get_new_row(row, 0, row_id);
	row->set_primary_key(wid);
	row->set_value(W_ID, wid);
	char name[10];
    MakeAlphaString(6, 10, name, wid-1);
	row->set_value(W_NAME, name);
	char street1[20];
    MakeAlphaString(10, 20, street1, wid-1);
	row->set_value(W_STREET_1, street1);
	char street2[20];
    MakeAlphaString(10, 20, street2, wid-1);
	row->set_value(W_STREET_2, street2);
	char w_city[20];
    MakeAlphaString(10, 20, w_city, wid-1);
	row->set_value(W_CITY, w_city);
	char state[2];
	MakeAlphaString(2, 2, state, wid-1); /* State */
	row->set_value(W_STATE, state);
	char zip[9];
   	MakeNumberString(9, 9, zip, wid-1); /* Zip */
	row->set_value(W_ZIP, zip);
   	double tax = (double)URand(0L,200L,wid-1)/1000.0;
	row->set_value(W_TAX, tax);
   	double w_ytd=300000.00;
	row->set_value(W_YTD, w_ytd);
	index_insert(i_warehouse, wid, row, wh_to_part(wid));

#if PIM_ENABLE
	int idx = 0;
	data[idx++] = &wid;
	data[idx++] = name;
	data[idx++] = street1;
	data[idx++] = street2;
	data[idx++] = w_city;
	data[idx++] = state;
	data[idx++] = zip;
	data[idx++] = &tax;
	data[idx++] = &w_ytd;

	table_s * t;
	u_int64_t storage_index;
	char t_n[] = "WAREHOUSE";

	st->get_table(t_n, t);
	t->insert_tuple(data, storage_index);

#if DETLA_STORAGE_ENABLE
	row->init_detla_buffer(storage_index, t);
#endif
#endif
	delete[] data;
	return;
}

void tpcc_wl::init_tab_dist(uint64_t wid) {
	void ** data = new void*[11];
	for (uint64_t did = 1; did <= DIST_PER_WARE; did++) {
		row_t * row;
		uint64_t row_id;
		t_district->get_new_row(row, 0, row_id);
		row->set_primary_key(did);
		row->set_value(D_ID, did);
		row->set_value(D_W_ID, wid);
		char name[10];
		MakeAlphaString(6, 10, name, wid-1);
		row->set_value(D_NAME, name);
		char street1[20];
        MakeAlphaString(10, 20, street1, wid-1);
		row->set_value(D_STREET_1, street1);
		char street2[20];
        MakeAlphaString(10, 20, street2, wid-1);
		row->set_value(D_STREET_2, street2);
		char d_city[20];
        MakeAlphaString(10, 20, d_city, wid-1);
		row->set_value(D_CITY, d_city);
		char state[2];
		MakeAlphaString(2, 2, state, wid-1); /* State */
		row->set_value(D_STATE, state);
		char zip[9];
    	MakeNumberString(9, 9, zip, wid-1); /* Zip */
		row->set_value(D_ZIP, zip);
    	double tax = (double)URand(0L,200L,wid-1)/1000.0;
		row->set_value(D_TAX, tax);
    	double w_ytd=30000.00;
		row->set_value(D_YTD, w_ytd);
		u_int64_t d_next_o_id = 3001;
		row->set_value(D_NEXT_O_ID, d_next_o_id);
		
		index_insert(i_district, distKey(did, wid), row, wh_to_part(wid));
#if PIM_ENABLE
		int idx = 0;
		data[idx++] = &did;
		data[idx++] = &wid;
		data[idx++] = name;
		data[idx++] = street1;
		data[idx++] = street2;
		data[idx++] = d_city;
		data[idx++] = state;
		data[idx++] = zip;
		data[idx++] = &tax;
		data[idx++] = &w_ytd;
		data[idx++] = &d_next_o_id;

		table_s * t;
		u_int64_t storage_index;
		char t_n[] = "DISTRICT";

		st->get_table(t_n, t);
		t->insert_tuple(data, storage_index);

#if DETLA_STORAGE_ENABLE
		row->init_detla_buffer(storage_index, t);
#endif
#endif
	}
	delete[] data;
}

void tpcc_wl::init_tab_stock(uint64_t wid) {
	void ** data = new void*[17];
	for (UInt32 sid = 1; sid <= g_max_items; sid++) {
		row_t * row;
		uint64_t row_id;
		t_stock->get_new_row(row, 0, row_id);
		row->set_primary_key(sid);
		row->set_value(S_I_ID, sid);
		row->set_value(S_W_ID, wid);
		u_int64_t s_quantity = URand(10, 100, wid-1);
		row->set_value(S_QUANTITY, s_quantity);
		u_int64_t s_remote_cnt = 0;
		row->set_value(S_REMOTE_CNT, s_remote_cnt);
#if !TPCC_SMALL
		char s_dist[10][25];
		char row_name[10] = "S_DIST_";
		for (int i = 1; i <= 10; i++) {
			if (i < 10) {
				row_name[7] = '0';
				row_name[8] = i + '0';
			} else {
				row_name[7] = '1';
				row_name[8] = '0';
			}
			row_name[9] = '\0';
			MakeAlphaString(24, 24, s_dist[i], wid-1);
			row->set_value(row_name, s_dist[i]);
		}
		u_int64_t s_ytd = 0;
		row->set_value(S_YTD, s_ytd);
		u_int64_t s_order_cnt = 0;
		row->set_value(S_ORDER_CNT, 0);
		char s_data[50];
		int len = MakeAlphaString(26, 50, s_data, wid-1);
		if (rand() % 100 < 10) {
			int idx = URand(0, len - 8, wid-1);
			strcpy(&s_data[idx], "original");
		}
		row->set_value(S_DATA, s_data);
#endif
		index_insert(i_stock, stockKey(sid, wid), row, wh_to_part(wid));

#if PIM_ENABLE
		int idx = 0;
		data[idx++] = &sid;
		data[idx++] = &wid;
		data[idx++] = &s_quantity;
		data[idx++] = &s_remote_cnt;
#if !TPCC_SMALL
		for (int i = 1; i <= 10; i++) {
			data[idx++] = s_dist[i];
		}
		data[idx++] = &s_ytd;
		data[idx++] = &s_order_cnt;
		data[idx++] = s_data;
#endif
		table_s * t;
		u_int64_t storage_index;
		char t_n[] = "STOCK";

		st->get_table(t_n, t);
		t->insert_tuple(data, storage_index);

#if DETLA_STORAGE_ENABLE
		row->init_detla_buffer(storage_index, t);
#endif
#endif
	}
	delete[] data;
}

void tpcc_wl::init_tab_cust(uint64_t did, uint64_t wid) {
	void ** data = new void*[21];
	assert(g_cust_per_dist >= 1000);
	for (UInt32 cid = 1; cid <= g_cust_per_dist; cid++) {
		row_t * row;
		uint64_t row_id;
		t_customer->get_new_row(row, 0, row_id);
		row->set_primary_key(cid);

		row->set_value(C_ID, cid);
		row->set_value(C_D_ID, did);
		row->set_value(C_W_ID, wid);
#if !TPCC_SMALL
		char c_first[FIRSTNAME_LEN];
		MakeAlphaString(FIRSTNAME_MINLEN, sizeof(c_first), c_first, wid-1);
		row->set_value(C_FIRST, c_first);
#endif
		char tmp[3] = "OE";
		row->set_value(C_MIDDLE, tmp);
		char c_last[LASTNAME_LEN];
		if (cid <= 1000)
			Lastname(cid - 1, c_last);
		else
			Lastname(NURand(255,0,999,wid-1), c_last);
		row->set_value(C_LAST, c_last);
#if !TPCC_SMALL
		char street1[20];
        MakeAlphaString(10, 20, street1, wid-1);
		row->set_value(C_STREET_1, street1);
		char street2[20];
        MakeAlphaString(10, 20, street2, wid-1);
		row->set_value(C_STREET_2, street2);
		char c_city[20];
        MakeAlphaString(10, 20, c_city, wid-1);
		row->set_value(C_CITY, c_city); 
#endif
		char state[2];
		MakeAlphaString(2, 2, state, wid-1); /* State */
		row->set_value(C_STATE, state);
#if !TPCC_SMALL
		char zip[9];
    	MakeNumberString(9, 9, zip, wid-1); /* Zip */
		row->set_value(C_ZIP, zip);
		char phone[16];
  		MakeNumberString(16, 16, phone, wid-1); /* Zip */
		row->set_value(C_PHONE, phone);
		u_int64_t c_since = 0;
		row->set_value(C_SINCE, c_since);
#endif
		char * c_credit = new char[2];
		if (RAND(10, wid-1) == 0) {
			memcpy(c_credit, "GC", 2);
		} else {
			memcpy(c_credit, "BC", 2);
		}
		row->set_value(C_CREDIT, c_credit);
#if !TPCC_SMALL
		u_int64_t c_credit_lim = 50000;
		row->set_value(C_CREDIT_LIM, c_credit_lim);
#endif
		double c_discount = (double)RAND(5000,wid-1) / 10000;
		row->set_value(C_DISCOUNT, c_discount);
		double c_balance = -10.0;
		row->set_value(C_BALANCE, c_balance);
		double c_ytd_payment = 10.0;
		row->set_value(C_YTD_PAYMENT, c_ytd_payment);

		u_int64_t c_payment_cnt = 1;
		row->set_value(C_PAYMENT_CNT, c_payment_cnt);
#if !TPCC_SMALL
		u_int64_t c_delivery_cnt = 0;
		row->set_value(C_DELIVERY_CNT, c_delivery_cnt);
		char c_data[500];
        MakeAlphaString(300, 500, c_data, wid-1);
		row->set_value(C_DATA, c_data);
#endif

		uint64_t key;
		key = custNPKey(c_last, did, wid);
		index_insert(i_customer_last, key, row, wh_to_part(wid));
		key = custKey(cid, did, wid);
		index_insert(i_customer_id, key, row, wh_to_part(wid));

#if PIM_ENABLE
		int idx = 0;
		data[idx++] = &cid;
		data[idx++] = &did;
		data[idx++] = &wid;
#if !TPCC_SMALL
		data[idx++] = c_first;
#endif
		data[idx++] = tmp;
		data[idx++] = c_last;
#if !TPCC_SMALL
		data[idx++] = street1;
		data[idx++] = street2;
		data[idx++] = c_city;
#endif
		data[idx++] = state;
#if !TPCC_SMALL
		data[idx++] = zip;
		data[idx++] = phone;
		data[idx++] = &c_since;
#endif
		data[idx++] = c_credit;
#if !TPCC_SMALL
		data[idx++] = &c_credit_lim;
#endif
		data[idx++] = &c_discount;
		data[idx++] = &c_balance;
		data[idx++] = &c_ytd_payment;
		data[idx++] = &c_payment_cnt;
#if !TPCC_SMALL
		data[idx++] = &c_delivery_cnt;
		data[idx++] = c_data;
#endif

		table_s * t_last;
		table_s * t_id;
		u_int64_t storage_index_last;
		u_int64_t storage_index_id;
		char t_n_last[] = "CUSTOMER_LAST";
		char t_n_id[] = "CUSTOMER_ID";

		st->get_table(t_n_last, t_last);
		st->get_table(t_n_id, t_id);

		t_last->insert_tuple(data, storage_index_last);
		t_id->insert_tuple(data, storage_index_id);
		assert(storage_index_last == storage_index_id);

#if DETLA_STORAGE_ENABLE
		row->init_detla_buffer(storage_index_last, t_last);
		row->init_detla_buffer(storage_index_id, t_id);
#endif
#endif
	}
	delete[] data;
}

void tpcc_wl::init_tab_hist(uint64_t c_id, uint64_t d_id, uint64_t w_id) {
	row_t * row;
	uint64_t row_id;
	t_history->get_new_row(row, 0, row_id);
	row->set_primary_key(0);
	row->set_value(H_C_ID, c_id);
	row->set_value(H_C_D_ID, d_id);
	row->set_value(H_D_ID, d_id);
	row->set_value(H_C_W_ID, w_id);
	row->set_value(H_W_ID, w_id);
	row->set_value(H_DATE, 0);
	row->set_value(H_AMOUNT, 10.0);
#if !TPCC_SMALL
	char h_data[24];
	MakeAlphaString(12, 24, h_data, w_id-1);
	row->set_value(H_DATA, h_data);
#endif

}

void tpcc_wl::init_tab_order(uint64_t did, uint64_t wid) {
	uint64_t perm[g_cust_per_dist]; 
	init_permutation(perm, wid); /* initialize permutation of customer numbers */
	for (UInt32 oid = 1; oid <= g_cust_per_dist; oid++) {
		row_t * row;
		uint64_t row_id;
		t_order->get_new_row(row, 0, row_id);
		row->set_primary_key(oid);
		uint64_t o_ol_cnt = 1;
		uint64_t cid = perm[oid - 1]; //get_permutation();
		row->set_value(O_ID, oid);
		row->set_value(O_C_ID, cid);
		row->set_value(O_D_ID, did);
		row->set_value(O_W_ID, wid);
		uint64_t o_entry = 2013;
		row->set_value(O_ENTRY_D, o_entry);
		if (oid < 2101)
			row->set_value(O_CARRIER_ID, URand(1, 10, wid-1));
		else 
			row->set_value(O_CARRIER_ID, 0);
		o_ol_cnt = URand(5, 15, wid-1);
		row->set_value(O_OL_CNT, o_ol_cnt);
		row->set_value(O_ALL_LOCAL, 1);
		
		// ORDER-LINE	
#if !TPCC_SMALL
		for (uint32_t ol = 1; ol <= o_ol_cnt; ol++) {
			t_orderline->get_new_row(row, 0, row_id);
			row->set_value(OL_O_ID, oid);
			row->set_value(OL_D_ID, did);
			row->set_value(OL_W_ID, wid);
			row->set_value(OL_NUMBER, ol);
			row->set_value(OL_I_ID, URand(1, 100000, wid-1));
			row->set_value(OL_SUPPLY_W_ID, wid);
			if (oid < 2101) {
				row->set_value(OL_DELIVERY_D, o_entry);
				row->set_value(OL_AMOUNT, 0);
			} else {
				row->set_value(OL_DELIVERY_D, 0);
				row->set_value(OL_AMOUNT, (double)URand(1, 999999, wid-1)/100);
			}
			row->set_value(OL_QUANTITY, 5);
			char ol_dist_info[24];
	        MakeAlphaString(24, 24, ol_dist_info, wid-1);
			row->set_value(OL_DIST_INFO, ol_dist_info);
		}
#endif
		// NEW ORDER
		if (oid > 2100) {
			t_neworder->get_new_row(row, 0, row_id);
			row->set_value(NO_O_ID, oid);
			row->set_value(NO_D_ID, did);
			row->set_value(NO_W_ID, wid);
		}
	}
}

/*==================================================================+
| ROUTINE NAME
| InitPermutation
+==================================================================*/

void 
tpcc_wl::init_permutation(uint64_t * perm_c_id, uint64_t wid) {
	uint32_t i;
	// Init with consecutive values
	for(i = 0; i < g_cust_per_dist; i++) 
		perm_c_id[i] = i+1;

	// shuffle
	for(i=0; i < g_cust_per_dist-1; i++) {
		uint64_t j = URand(i+1, g_cust_per_dist-1, wid-1);
		uint64_t tmp = perm_c_id[i];
		perm_c_id[i] = perm_c_id[j];
		perm_c_id[j] = tmp;
	}
}


/*==================================================================+
| ROUTINE NAME
| GetPermutation
+==================================================================*/

void * tpcc_wl::threadInitWarehouse(void * This) {
	tpcc_wl * wl = (tpcc_wl *) This;
	int tid = ATOM_FETCH_ADD(wl->next_tid, 1);
	uint32_t wid = tid + 1;
	tpcc_buffer[tid] = (drand48_data *) _mm_malloc(sizeof(drand48_data), 64);
	assert((uint64_t)tid < g_num_wh);
	srand48_r(wid, tpcc_buffer[tid]);
	
	if (tid == 0)
		wl->init_tab_item();
	wl->init_tab_wh( wid );
	wl->init_tab_dist( wid );
	wl->init_tab_stock( wid );
	for (uint64_t did = 1; did <= DIST_PER_WARE; did++) {
		wl->init_tab_cust(did, wid);
		wl->init_tab_order(did, wid);
		for (uint64_t cid = 1; cid <= g_cust_per_dist; cid++) 
			wl->init_tab_hist(cid, did, wid);
	}
	return NULL;
}
