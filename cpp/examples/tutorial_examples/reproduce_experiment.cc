#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <gflags/gflags.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

#include <iostream>

DEFINE_string(filename, "", "The name of the parquet file to parse.");
DEFINE_string(column, "", "The name of the column to read from the parquet file.");

arrow::Status parseFirstRowGroupOfParquetFile(const std::string& filename,
                                              const std::string& column) {
  std::cout << "Parsing " << filename << " to read row group 0, column " << column
            << "\n";
  std::shared_ptr<arrow::io::ReadableFile> infile;
  ARROW_ASSIGN_OR_RAISE(
      infile, arrow::io::ReadableFile::Open(filename, arrow::default_memory_pool()));

  std::unique_ptr<parquet::arrow::FileReader> reader;
  PARQUET_THROW_NOT_OK(
      parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));

  std::shared_ptr<arrow::Schema> arrow_schema;
  PARQUET_THROW_NOT_OK(reader->GetSchema(&arrow_schema));

  int col_index = arrow_schema->GetFieldIndex(column);

  std::shared_ptr<parquet::arrow::RowGroupReader> row_group_reader = reader->RowGroup(0);
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

  arrow::Status st = parseFirstRowGroupOfParquetFile(FLAGS_filename, FLAGS_column);
  if (!st.ok()) {
    std::cout << st << std::endl;
    return 1;
  }
  return 0;
}
