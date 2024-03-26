import numpy as np

device_num = 8
key_col_thresh = 0.3

important_keys = ["c_id", "c_d_id", "c_w_id", "c_last", "c_city", "c_state", "c_phone", "c_balance", "c_n_nationkey",
                    "no_o_id", "no_d_id", "no_w_id",
                    "o_id", "o_d_id", "o_w_id", "o_c_id", "o_entry_d", "o_carrier_id", "o_ol_cnt",
                    "ol_o_id", "ol_d_id", "ol_w_id", "ol_number", "ol_i_id", "ol_supply_w_id", "ol_delivery_d", "ol_quantity", "ol_amount",
                    "i_id", "i_price",
                    "s_i_id", "s_w_id", "s_quantity  s_su_suppkey",
                    "n_nationkey", "n_name", "n_regionkey",
                    "su_suppkey", "su_name", "su_nationkey",
                    "r_regionkey", "r_name"]

class Table:
    def __init__(self, name):
        self.name = name
        self.items = {}
        self.keys = []

    def add_field(self, name, length):
        self.keys.append(name)
        self.items[name] = length

def add_zero_element(arr, value):

    max_size = 0;
    for e1 in arr:
        for e2 in e1:
            if len(e2) > max_size:
                max_size = len(e2)
    
    for e1 in arr:
        for e2 in e1:
            for i in range(0, max_size-len(e2)):
                e2.append(value)

    return arr


def print_alloc(tab):
    col_widths = [tab.items[x] for x in tab.keys]
    col_widths_sort = np.argsort(col_widths)
    
    #print(col_widths)
    #print(np.sort(col_widths))
    
    total_width = 0
    device_col_map = []
    device_col_width = []
    
    #print(col_widths)

    while len(col_widths_sort):
        arranged_col = [[] for i in range(device_num)]
        arranged_col_width = [[] for i in range(device_num)]
        arranged_width = [0 for i in range(device_num)]
        main_col = -1
       
         # find main col, which is the widest important col
        for c in col_widths_sort[-1::-1]:
            if tab.keys[c] in important_keys:
                main_col = c
                break
        if main_col != -1:
            # main col found, arrange it to the first device
            main_width = col_widths[main_col]
            arranged_col[0].append(main_col)
            arranged_col_width[0].append(main_width)
            arranged_width[0] += main_width
            col_widths[main_col] = 0
        else:
            main_width = int(np.ceil(np.sum(col_widths) / device_num))
            
        # add key cols that are wider than threshold
        for c in col_widths_sort[-1::-1]:
            if tab.keys[c] not in important_keys:
                continue
            else:
                if col_widths[c] < key_col_thresh * main_width: # means this column is to narrow so that cause much waste
                    continue
                for i in range(device_num):
                    if not col_widths[c]: # means this column have been arranged away
                        break
                    if (main_width - arranged_width[i] >= col_widths[c]):
                        arranged_col[i].append(c)
                        argd_w = col_widths[c]
                        arranged_col_width[i].append(argd_w)
                        arranged_width[i] += argd_w
                        col_widths[c] -= argd_w
        
        # fill other bytes
        for c in col_widths_sort[-1::-1]:
            if tab.keys[c] in important_keys: # means this column can be devided
                continue
            for i in range(device_num):
                if not col_widths[c]: # means this column have been arranged away
                    break
                if (main_width - arranged_width[i] > 0):
                    arranged_col[i].append(c)
                    argd_w = np.minimum(main_width - arranged_width[i], col_widths[c])
                    arranged_col_width[i].append(argd_w)
                    arranged_width[i] += argd_w
                    col_widths[c] -= argd_w

        device_col_map.append(arranged_col)
        device_col_width.append(arranged_col_width)
        #update col width sort
        col_widths_sort = np.argsort(col_widths)
        col_widths_sort = [x for x in col_widths_sort if col_widths[x]]
        total_width += main_width * np.sum([x>0 for x in arranged_width])
    
    
    col_widths = [tab.items[x] for x in tab.keys]
    utilization = np.sum(col_widths) / total_width
    # get pim usage
    total_key_width = 0
    round_key_width = 0
    for bnk, bk in enumerate(device_col_map):
        for dev in bk:
            for col in dev:
                if tab.keys[col] in important_keys:
                    total_key_width += col_widths[col]
                    round_key_width += np.sum(device_col_width[bnk][0])
    
    # # it represents original field width, index of mapping to banks and mapping layout, respectively
    # return ([tab.items[x] for x in tab.keys], device_col_map, device_col_width)
    
    # print('col width:', col_widths)
                    
    # device_col_map = add_zero_element(device_col_map, -1)
    # device_col_width = add_zero_element(device_col_width, 0)

    print("table name:", tab.name, end=' ')
    print('column width: ', end=' ')
    print('[', end='')
    for i in range(len(col_widths)):
        print(col_widths[i], end='')
        if tab.keys[i] in important_keys:
            print('*', end='')
        print(', ', end='')
    print(']')
    
    print("table column mapped to device:", device_col_map)

    print('{', end='')
    for devices in device_col_map:
        print('{', end='')
        for indexs in devices:
            print('{', end='')
            for i in range(len(indexs)-1):
                print(indexs[i]+1, end=',')
            if len(indexs) > 0:
                print(indexs[-1]+1, end='')
            print('},', end='')
        print('},', end='')
    print('}')

    print("bytes mapped to this device", device_col_width)

    print('{', end='')
    for devices in device_col_width:
        print('{', end='')
        for indexs in devices:
            print('{', end='')
            for i in range(len(indexs)-1):
                print(indexs[i], end=',')
            if len(indexs) > 0:
                print(indexs[-1], end='')
            print('},', end='')
        print('},', end='')
    print('}')

    print("usage: ", utilization)
    if round_key_width:
        pim_usage = total_key_width / round_key_width
        print("pim_usage: ", pim_usage)
    
    print("\r\n")

def schema_reader(file_name):

    tables = []

    with open(file_name, 'r', encoding='utf-8') as file:
        for line in file:
            if line.startswith('TABLE'):
                table_name = line.split('=')[1]
                table = Table(table_name)
                tables.append(table)
            elif line.startswith('\t'):
                raw_value = line.split(',')
                table.add_field(raw_value[2][:-1].lower(), int(raw_value[0]))

    return tables

def main():
    full_name = 'TPCC_full_schema.txt'
    short_name = 'TPCC_short_schema.txt'
    tables = schema_reader(short_name)

    for table in tables:
        print_alloc(table)

if __name__ == "__main__":
    main()