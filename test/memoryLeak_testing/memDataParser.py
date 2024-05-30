import datetime
import dolphindb
import os
import pandas as pd
import dolphindb.settings as keys

def search_files_with_extension(directory, extension):
    res = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(extension):
                file_path = os.path.join(root, file)
                res.append(file_path)
    if len(res) == 1:
        return res[0]
    return res


local_file = search_files_with_extension(os.getcwd(), ".dat")


with open(local_file, 'r') as file:
    lines = file.readlines()

with open(local_file, 'w+') as file:
    for line in lines:
        try:
            parts = line.strip().split(' ')
            if len(parts) == 3:
                timestamp = float(parts[2])
                datetime_obj = datetime.datetime.fromtimestamp(timestamp)
                formatted_time = datetime_obj.strftime("%Y.%m.%dT%H:%M:%S.%f")
                print(formatted_time)
                updated_line = f"{parts[1]} {formatted_time}\n"
                file.write(updated_line)
        except:
            continue

conn = dolphindb.session("192.168.100.10", 13848, "admin", "123456")
data = pd.read_csv(local_file, header=None, delimiter=' ')
data.columns = ["col0", "col1"]
data.__DolphinDB_Type__ = {
    "col0": keys.DT_DOUBLE,
    "col1": keys.DT_STRING,
}
print(data)
conn.upload({"data": data})
conn.run("""
        delete from loadTable('dfs://api_perf_data', `api_py_mem);go;
        tmp = select nanotimestamp(col1) as time, col0 as mem from data;
        loadTable('dfs://api_perf_data', `api_py_mem).append!(tmp)
    """)

conn.close()

