#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <gflags/gflags.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

#include <iostream>

DEFINE_string(filename, "", "The name of the parquet file to parse.");

arrow::Status parseFirstRowGroupOfParquetFile(const std::string& filename) {
  std::cout << "Hello world from Lester. We want to parse " << filename << "\n";
  std::shared_ptr<arrow::io::ReadableFile> infile;
  ARROW_ASSIGN_OR_RAISE(
      infile, arrow::io::ReadableFile::Open(filename, arrow::default_memory_pool()));
  std::unique_ptr<parquet::arrow::FileReader> reader;
  PARQUET_THROW_NOT_OK(
      parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
  int num_row_groups = reader->num_row_groups();
  std::cout << "Number of row groups: " << num_row_groups << "\n";
  if (num_row_groups > 0) {
    std::shared_ptr<arrow::RecordBatchReader> batch_reader;
    PARQUET_THROW_NOT_OK(
        reader->GetRecordBatchReader(std::vector<int>{0}, &batch_reader));
    int total_rows = 0, total_cols = 0;
    std::shared_ptr<arrow::RecordBatch> batch;
    while (true) {
      ARROW_ASSIGN_OR_RAISE(batch, batch_reader->Next());
      if (!batch) {
        break;
      }
      total_rows += batch->num_rows();
      total_cols = batch->num_columns();
      std::cout << "Intermediate step. total_rows = " << total_rows << "\n";
    }
    std::cout << "Total rows in first row group: " << total_rows << "\n";
    std::cout << "Number of columns: " << total_cols << "\n";
  } else {
    std::cout << "No row groups in the file.\n";
  }
  return arrow::Status::OK();
}

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  if (FLAGS_filename.empty()) {
    std::cerr << "Must specify filename\n";
    return EXIT_SUCCESS;
  }
  arrow::Status st = parseFirstRowGroupOfParquetFile(FLAGS_filename);
  if (!st.ok()) {
    std::cerr << st << std::endl;
    return 1;
  }
  return 0;
}