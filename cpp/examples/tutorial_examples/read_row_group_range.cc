#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/pretty_print.h>
#include <arrow/result.h>
#include <gflags/gflags.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/schema.h>
#include <parquet/arrow/writer.h>
#include <iostream>
#include <memory>

#include <iostream>

DEFINE_string(filename, "", "The name of the parquet file to parse.");
DEFINE_string(column, "", "The name of the column to read from the parquet file.");
DEFINE_string(ree_encoded_columns_str, "",
              "Comma-separated list of columns to directly read as REE encoded (e.g., "
              "'apple,banana,cherry')");
DEFINE_int32(row_group_start, -1,
             "The row group to start reading from the parquet file.");
DEFINE_int32(row_group_end, -1, "The row group to end reading from the parquet file.");

std::vector<std::string> get_ree_encoded_column_names(
    const std::string& ree_encded_columns_str) {
  std::vector<std::string> result;
  std::istringstream iss(ree_encded_columns_str);
  std::string token;
  while (std::getline(iss, token, ',')) {
    result.push_back(token);
  }
  return result;
}

arrow::Status get_column_indices(const std::string& filename,
                                 const std::vector<std::string>& ree_encoded_column_names,
                                 std::vector<int>* indices) {
  std::shared_ptr<arrow::io::ReadableFile> infile;
  ARROW_ASSIGN_OR_RAISE(
      infile, arrow::io::ReadableFile::Open(filename, arrow::default_memory_pool()));

  std::unique_ptr<parquet::ParquetFileReader> parquet_reader =
      parquet::ParquetFileReader::Open(infile);
  std::shared_ptr<parquet::FileMetaData> metadata = parquet_reader->metadata();

  std::shared_ptr<arrow::Schema> arrow_schema;
  auto status = parquet::arrow::FromParquetSchema(metadata->schema(), &arrow_schema);
  if (!status.ok()) throw std::runtime_error(status.ToString());

  for (const auto& col_name : ree_encoded_column_names) {
    int idx = arrow_schema->GetFieldIndex(col_name);
    if (idx == -1) throw std::runtime_error("Column not found: " + col_name);
    indices->push_back(idx);
  }
  return arrow::Status::OK();
}

arrow::Status readColumnOfParquetFile(
    const std::string& filename, const std::string& column,
    const std::vector<std::string>& ree_encoded_column_names, int row_group_start,
    int row_group_end) {
  std::vector<int> ree_encoded_column_indices;
  RETURN_NOT_OK(get_column_indices(filename, ree_encoded_column_names,
                                   &ree_encoded_column_indices));
  std::cout << "Parsing " << filename << ", column " << column
            << ". The following columns should be read as REE encoded: [";
  for (size_t i = 0; i < ree_encoded_column_names.size(); ++i) {
    std::cout << "(column_name: '" << ree_encoded_column_names[i] << "', "
              << "column_index: " << ree_encoded_column_indices[i] << "), ";
  }
  std::cout << "]\n";

  std::shared_ptr<arrow::io::ReadableFile> infile;
  ARROW_ASSIGN_OR_RAISE(
      infile, arrow::io::ReadableFile::Open(filename, arrow::default_memory_pool()));
  parquet::ArrowReaderProperties properties = parquet::default_arrow_reader_properties();
  for (int ree_encoded_column_index : ree_encoded_column_indices) {
    properties.set_read_ree(ree_encoded_column_index, true);
  }

  std::unique_ptr<parquet::arrow::FileReader> reader;
  parquet::arrow::FileReaderBuilder builder;
  builder.properties(properties);
  RETURN_NOT_OK(builder.Open(infile));
  PARQUET_THROW_NOT_OK(builder.memory_pool(arrow::default_memory_pool())->Build(&reader));

  std::shared_ptr<arrow::Schema> arrow_schema;
  PARQUET_THROW_NOT_OK(reader->GetSchema(&arrow_schema));

  int col_index = arrow_schema->GetFieldIndex(column);
  std::vector<int> row_groups;
  for (int rg = row_group_start; rg <= row_group_end; ++rg) {
    row_groups.push_back(rg);
  }
  std::shared_ptr<arrow::Table> table;
  RETURN_NOT_OK(reader->ReadRowGroups(row_groups, {col_index}, &table));
  std::shared_ptr<arrow::ChunkedArray> chunked_array = table->column(0);

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

  if (FLAGS_row_group_start < 0 || FLAGS_row_group_end < 0) {
    std::cerr << "Must specify non-negative row group start and end\n";
    return EXIT_SUCCESS;
  }

  std::vector<std::string> ree_encoded_column_names =
      get_ree_encoded_column_names(FLAGS_ree_encoded_columns_str);

  arrow::Status st =
      readColumnOfParquetFile(FLAGS_filename, FLAGS_column, ree_encoded_column_names,
                              FLAGS_row_group_start, FLAGS_row_group_end);
  if (!st.ok()) {
    std::cout << st << std::endl;
    return 1;
  }

  return 0;
}
