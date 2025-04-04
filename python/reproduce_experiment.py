#!/opt/salt/bin/python

import pyarrow as pa
import pyarrow.parquet as pq

def generate_field_data(num_rows: int, field):
    t = field.type
    if pa.types.is_int64(t):
        return pa.array([555555] * num_rows, type=pa.int64())
    elif pa.types.is_string(t):
        # return pa.array(["a" * 1000] * num_rows, type=pa.string())
        return pa.array(["a"] * num_rows, type=pa.string())
    else:
        raise NotImplementedError("Synthetic data not implemented for type: " + str(t))

def repro():
    file_name = "20250407_synthetic_py.parquet"
    total_rows = 3_000_000_000
    chunk_size = 1_000_000
    num_int_columns = 50
    num_string_columns = 50
    fields = [
        *[pa.field("int_column_" + str(i), pa.int64()) for i in range(num_int_columns)],
        *[pa.field("str_column_" + str(i), pa.string()) for i in range(num_string_columns)]
    ]
    schema = pa.schema(fields)
    writer = pq.ParquetWriter(file_name, schema, 
                            #   write_statistics=False
                              )
    chunk_count = 0

    arrays = [generate_field_data(chunk_size, field) for field in schema]
    batch = pa.RecordBatch.from_arrays(arrays, schema=schema)

    for row_index in range(0, total_rows, chunk_size):
        writer.write(batch)
        print(f"{row_index = }")
        chunk_count += 1
    writer.close()

    print(f"Finished writing {total_rows} rows in {chunk_count} chunks to {file_name}")

def main():
    try:
        repro()
        print("OK")
    except Exception as e:
        print("Error:", e)
        exit(1)

if __name__ == "__main__":
    main()
