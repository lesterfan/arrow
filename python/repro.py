import pyarrow.parquet as pq

filepath = "~/parquet_files/1730130291978266733.parquet"
ree_col = "sched_instance"
dict_col = "host_name"
pqfile = pq.ParquetFile(filepath, 
                        read_ree=[ree_col],
                        read_dictionary=[dict_col])
print(pqfile)
for batch in pqfile.iter_batches():
    print(batch)
    break