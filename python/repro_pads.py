import pyarrow.dataset as pads 
import pyarrow.fs as pafs 
import pyarrow as pa
import pyarrow.parquet as pq

filename = "/Users/lester/parquet_files/20250411_subset.parquet"
data_paths = [filename]
ree_column = "sched_instance"
schema = pq.read_schema(filename)
print(f"{schema = }")
filtered_dataset = pads.FileSystemDataset.from_paths(
    paths=data_paths,
    schema=schema,
    format=pads.ParquetFileFormat(
        read_options=pads.ParquetReadOptions(ree_columns=[ree_column])
    ),
    filesystem=pafs.LocalFileSystem(),
)
filtered_dataset = pads.FileSystemDataset(
    filtered_dataset.get_fragments(),
    schema,
    filtered_dataset.format,
)
batch = next(filtered_dataset.to_batches(use_threads=True))
print(f"{batch = }")
