#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>
#include <sys/resource.h>
#include <algorithm>
#include <iostream>
#include <vector>

// Helper function: returns OS-reported memory usage in bytes (ru_maxrss is kilobytes on
// Linux)
size_t GetOSMemoryUsage() {
  struct rusage usage;
  getrusage(RUSAGE_SELF, &usage);
  return usage.ru_maxrss * 1024;  // convert kilobytes to bytes
}

arrow::Status generate_field_data(std::shared_ptr<arrow::Array>& out, int64_t num_rows,
                                  const std::shared_ptr<arrow::Field>& field) {
  if (field->type()->id() == arrow::Type::INT64) {
    arrow::Int64Builder builder;
    int64_t value = 555555;
    std::vector<int64_t> values(num_rows, value);
    ARROW_RETURN_NOT_OK(builder.AppendValues(values));
    return builder.Finish(&out);
  } else if (field->type()->id() == arrow::Type::STRING) {
    arrow::StringBuilder builder;
    std::string value = std::string(1000, 'a');
    std::vector<std::string> values(num_rows, value);
    ARROW_RETURN_NOT_OK(builder.AppendValues(values));
    return builder.Finish(&out);
  }
  return arrow::Status::NotImplemented("Synthetic data not implemented for type: " +
                                       field->type()->ToString());
}

arrow::Status repro() {
  const std::string file_name = "synthetic.parquet";
  const int64_t total_rows = 1'000'000'000;
  const int64_t chunk_size = 8000;
  int num_int_columns = 10;
  int num_string_columns = 10;
  std::vector<std::shared_ptr<arrow::Field>> field_vector;
  for (int i = 0; i < num_int_columns; ++i) {
    field_vector.push_back(
        arrow::field("int_column_" + std::to_string(i), arrow::int64()));
  }
  for (int i = 0; i < num_string_columns; ++i) {
    field_vector.push_back(
        arrow::field("str_column_" + std::to_string(i), arrow::utf8()));
  }
  auto schema = arrow::schema(field_vector);

  ARROW_ASSIGN_OR_RAISE(auto outfile, arrow::io::FileOutputStream::Open(file_name));
  ARROW_ASSIGN_OR_RAISE(auto writer, parquet::arrow::FileWriter::Open(
                                         *schema, arrow::default_memory_pool(), outfile));
  int chunk_count = 0;

  int64_t arrow_allocated = 0;
  size_t os_memory_usage = 0;
  for (int64_t row_index = 0; row_index < total_rows;
       row_index += chunk_size, ++chunk_count) {
    int64_t current_chunk_size = std::min(chunk_size, total_rows - row_index);

    std::vector<std::shared_ptr<arrow::Array>> arrays(schema->num_fields());
    for (int field_index = 0; field_index < schema->num_fields(); ++field_index) {
      ARROW_RETURN_NOT_OK(generate_field_data(arrays[field_index], current_chunk_size,
                                              schema->field(field_index)));
    }
    auto batch = arrow::RecordBatch::Make(schema, current_chunk_size, arrays);
    // printf("Writing batch: %s\n", batch->ToString().c_str());
    ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
    ARROW_RETURN_NOT_OK(writer->NewBufferedRowGroup());

    int64_t new_arrow_allocated = arrow::default_memory_pool()->bytes_allocated();
    size_t new_os_memory_usage = GetOSMemoryUsage();

    if (new_arrow_allocated != arrow_allocated ||
        new_os_memory_usage != os_memory_usage) {
      arrow_allocated = new_arrow_allocated;
      os_memory_usage = new_os_memory_usage;
      printf("row_index: %lld, arrow_allocated: %lld, os_memory: %zu\n", row_index,
             arrow_allocated, os_memory_usage);
    }
  }
  printf("Finished writing %lld rows in %d chunks to %s\n", total_rows, chunk_count,
         file_name.c_str());
  return arrow::Status::OK();
}

int main() {
  if (auto repro_status = repro(); !repro_status.ok()) {
    printf("Error: %s\n", repro_status.ToString().c_str());
    return 1;
  }
  printf("OK\n");
}
