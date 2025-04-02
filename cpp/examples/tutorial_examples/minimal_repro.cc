#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>
#include <sys/resource.h>
#include <algorithm>
#include <iostream>
#include <vector>

arrow::Status repro() {
  const std::string file_name = "minimal.parquet";
  auto schema = arrow::schema({arrow::field("col", arrow::utf8())});
  int current_chunk_size = 1;
  ARROW_ASSIGN_OR_RAISE(auto outfile, arrow::io::FileOutputStream::Open(file_name));
  ARROW_ASSIGN_OR_RAISE(auto writer, parquet::arrow::FileWriter::Open(
                                         *schema, arrow::default_memory_pool(), outfile));
  std::shared_ptr<arrow::Array> data;
  {
    arrow::StringBuilder builder;
    for (int64_t i = 0; i < current_chunk_size; ++i) {
      ARROW_RETURN_NOT_OK(builder.Append(std::string(2, 'a')));
    }
    ARROW_RETURN_NOT_OK(builder.Finish(&data));
  }
  auto batch = arrow::RecordBatch::Make(schema, current_chunk_size, {data});
  printf("Writing batch: %s\n", batch->ToString().c_str());
  ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
  ARROW_RETURN_NOT_OK(writer->Close());
  writer.reset();
  ARROW_RETURN_NOT_OK(outfile->Close());
  outfile.reset();
  printf("Finished writing a chunk of size %d to %s\n", current_chunk_size,
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
