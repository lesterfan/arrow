#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/pretty_print.h>
#include <arrow/result.h>
#include <gflags/gflags.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <iostream>
#include <memory>

#include <iostream>

DEFINE_string(filename, "", "The name of the parquet file to parse.");
DEFINE_string(column, "", "The name of the column to read from the parquet file.");
DEFINE_int32(row_group, -1, "The row group to read from the parquet file.");
DEFINE_bool(ree, true, "Read run-end-encoded");

arrow::Status parseRowGroupOfParquetFile(int row_group_num, const std::string& filename,
                                         const std::string& column,
                                         bool read_run_end_encoded) {
  std::cout << "Parsing " << filename << " to read row group " << row_group_num
            << ", column " << column << "\n";
  std::shared_ptr<arrow::io::ReadableFile> infile;
  ARROW_ASSIGN_OR_RAISE(
      infile, arrow::io::ReadableFile::Open(filename, arrow::default_memory_pool()));

  parquet::ArrowReaderProperties properties = parquet::default_arrow_reader_properties();
  properties.set_read_parquet_rle_cols_to_arrow_ree(read_run_end_encoded);

  std::unique_ptr<parquet::arrow::FileReader> reader;
  parquet::arrow::FileReaderBuilder builder;
  builder.properties(properties);
  RETURN_NOT_OK(builder.Open(infile));
  PARQUET_THROW_NOT_OK(builder.memory_pool(arrow::default_memory_pool())->Build(&reader));

  std::shared_ptr<arrow::Schema> arrow_schema;
  PARQUET_THROW_NOT_OK(reader->GetSchema(&arrow_schema));

  int col_index = arrow_schema->GetFieldIndex(column);

  std::shared_ptr<parquet::arrow::RowGroupReader> row_group_reader =
      reader->RowGroup(row_group_num);
  std::shared_ptr<parquet::arrow::ColumnChunkReader> column_chunk_reader =
      row_group_reader->Column(col_index);

  std::shared_ptr<arrow::ChunkedArray> chunked_array;
  PARQUET_THROW_NOT_OK(column_chunk_reader->Read(&chunked_array));

  std::cout << "Read chunked array (length: " << chunked_array->length()
            << "): " << chunked_array->ToString() << "\n";

  return arrow::Status::OK();
}

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_filename.empty()) {
    std::cerr << "Must specify filename\n";
    return EXIT_SUCCESS;
  }

  if (FLAGS_column.empty()) {
    std::cerr << "Must specify column name\n";
    return EXIT_SUCCESS;
  }

  if (FLAGS_row_group < 0) {
    std::cerr << "Must specify non-negative row group number \n";
    return EXIT_SUCCESS;
  }
  arrow::Status st = parseRowGroupOfParquetFile(FLAGS_row_group, FLAGS_filename,
                                                FLAGS_column, FLAGS_ree);
  if (!st.ok()) {
    std::cout << st << std::endl;
    return 1;
  }

  return 0;
}
