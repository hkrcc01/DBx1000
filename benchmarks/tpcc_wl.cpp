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

#if TPCC_SMALL
	char file_path[] = "./tools/mem_alloc/tpcc_short_allocs.txt";
#elif
	char file_path[] = "./tools/mem_alloc/tpcc_full_allocs.txt";
#endif

	char cmd[100] = "python3 ./tools/mem_alloc/main4.py";
	char para[] = " -dn ";
	strcat(cmd, para);

	char devices_cnt[5];
	snprintf(devices_cnt, sizeof(devices_cnt), "%d", DEVICE_CNT_PER_BANK);
	strcat(cmd, devices_cnt);

	int returnCode = system(cmd);

    if (returnCode == -1) {
		assert(false);
    }

	u_int32_t tables_cnt = 6;
	this->st = new storage();
	// there are 6 tables
	st->init(tables_cnt);

	init_table_size();

	char wh[] = "WAREHOUSE"; 
	init_table_st(file_path, wh, 10, 9, this->l_warehouse);
	char item[] = "ITEM";
	init_table_st(file_path, item, 5, 5, this->l_item);
	char dist[] = "DISTRICT";
	init_table_st(file_path, dist, 9, 11, this->l_district);
	char stock[] = "STOCK";
	char last[] = "CUSTOMER_LAST"; 
	char id[] = "CUSTOMER_ID";
#if TPCC_SMALL
	init_table_st(file_path, stock, 5, 4, this->l_stock);
	init_table_st(file_path, last, 14, 11, this->l_customer);
	init_table_st(file_path, id, 12, 11, this->l_customer);
#else
	init_table_st(file_path, stock, 5, 17, this->l_stock);
	init_table_st(file_path, last, 14, 21, this->l_customer);
	init_table_st(file_path, id, 12, 21, this->l_customer);
#endif

	// init_st_wh();
	// init_st_item();
	// init_st_dist();
	// init_st_stock();
	// init_st_cust(last, 14);
	// init_st_cust(id, 12);
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

RC tpcc_wl::init_table_st(char * file_path, char * table_name, u_int32_t name_size, u_int32_t f_cnt, u_int32_t table_size) {

	u_int32_t bank_cnt;
	u_int32_t row = DEVICE_CNT_PER_BANK;
	u_int32_t col;
	u_int32_t *** wh_i = GetAllocArr(table_name, file_path, bank_cnt, col, '1');
	u_int32_t *** wh_w = GetAllocArr(table_name, file_path, bank_cnt, col, '0');

	table_s * t = st->insert_table(table_name, name_size, wh_i, wh_w, bank_cnt, row, col, f_cnt);
	t->init_table_size(table_size);

	free_arr(wh_i, bank_cnt, row);
	free_arr(wh_w, bank_cnt, row);
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
#if TABLE_DATA_DEBUG
	bool flag = true;
#endif
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
		u_int32_t storage_index;
		char t_n[] = "ITEM";

		st->get_table(t_n, t);
		t->insert_tuple(data, storage_index);

#if DETLA_STORAGE_ENABLE
		row->init_detla_buffer(storage_index, t);
#endif
#if TABLE_DATA_DEBUG
		if (flag) {
			t->print_line(storage_index);
			flag = false;
		}
#endif
#endif
	}
	delete[] data;
}

void tpcc_wl::init_tab_wh(uint64_t wid) {
	void ** data = new void*[9];
	assert(wid >= 1 && wid <= g_num_wh);
#if TABLE_DATA_DEBUG
	bool flag = true;
#endif
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
	u_int32_t storage_index;
	char t_n[] = "WAREHOUSE";

	st->get_table(t_n, t);
	t->insert_tuple(data, storage_index);

#if DETLA_STORAGE_ENABLE
	row->init_detla_buffer(storage_index, t);
#endif
#if TABLE_DATA_DEBUG
	if (flag) {
		t->print_line(storage_index);
		flag = false;
	}
	// void * value = _mm_malloc(500, 1);
	// t->get_value(0, 0, value);
#endif
#endif
	delete[] data;
	return;
}

void tpcc_wl::init_tab_dist(uint64_t wid) {
	void ** data = new void*[11];
#if TABLE_DATA_DEBUG
	bool flag = true;
#endif
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
		u_int32_t storage_index;
		char t_n[] = "DISTRICT";

		st->get_table(t_n, t);
		t->insert_tuple(data, storage_index);

#if DETLA_STORAGE_ENABLE
		row->init_detla_buffer(storage_index, t);
#endif
#if TABLE_DATA_DEBUG
		if (flag) {
			t->print_line(storage_index);
			flag = false;
		}
#endif
#endif
	}
	delete[] data;
}

void tpcc_wl::init_tab_stock(uint64_t wid) {
	void ** data = new void*[17];
#if TABLE_DATA_DEBUG
	bool flag = true;
#endif
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
		u_int32_t storage_index;
		char t_n[] = "STOCK";

		st->get_table(t_n, t);
		t->insert_tuple(data, storage_index);

#if DETLA_STORAGE_ENABLE
		row->init_detla_buffer(storage_index, t);
#endif
#if TABLE_DATA_DEBUG
		if (flag) {
			t->print_line(storage_index);
			flag = false;
		}
#endif
#endif
	}
	delete[] data;
}

void tpcc_wl::init_tab_cust(uint64_t did, uint64_t wid) {
	void ** data = new void*[21];
	assert(g_cust_per_dist >= 1000);
#if TABLE_DATA_DEBUG
	bool flag = true;
#endif
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
		u_int32_t storage_index_last;
		u_int32_t storage_index_id;
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
#if TABLE_DATA_DEBUG
		if (flag) {
			t_last->print_line(storage_index_last);
			t_id->print_line(storage_index_id);
			flag = false;
		}
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
	
	wl->init_tab_wh( wid );
	if (tid == 0)
		wl->init_tab_item();
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
