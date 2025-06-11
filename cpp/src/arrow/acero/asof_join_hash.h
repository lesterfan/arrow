// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <cstdint>
#include <vector>
#include <array>

#include "arrow/type_traits.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"
#include "arrow/compute/light_array_internal.h"

namespace arrow {
namespace acero {

class ARROW_EXPORT KeyColumnArray {
  KeyColumnArray(bool is_binary_like, uint32_t index_length, uint32_t value_length, uint64_t length,
                 const uint8_t* value_validity_buffer, const uint8_t* value_fixed_length_buffer,
                 const uint8_t* value_var_length_buffer, uint64_t dict_length,
                 const uint8_t* dict_validity_buffer, const uint8_t* dict_fixed_length_buffer,
                 const uint8_t* dict_var_length_buffer) :
      buffers_{value_validity_buffer, value_fixed_length_buffer, value_var_length_buffer},
      dictionary_buffers_{dict_validity_buffer, dict_fixed_length_buffer, dict_var_length_buffer},
      is_binary_like_(is_binary_like),
      index_length_(index_length),
      value_length_(value_length),
      length_(length),
      dictionary_length_(dict_length) {}
 public:
  static KeyColumnArray Make(const std::shared_ptr<ArrayData>& data, int64_t start_row, int64_t num_rows) {
    std::optional<KeyColumnArray> column_array;
    if (data->type->id() == Type::DICTIONARY) {
      auto& dict_type =
          internal::checked_cast<const DictionaryType&>(*data->type);
      DCHECK(data->buffers[2] == NULLPTR);
      DCHECK(data->dictionary != NULLPTR);
      column_array = KeyColumnArray(
          is_binary_like(dict_type.value_type()->id()),
          dict_type.index_type()->bit_width() / 8,
          dict_type.value_type()->bit_width() / 8,
          data->offset + start_row + num_rows,
          data->buffers[0] != NULLPTR ? data->buffers[0]->data() : NULLPTR,
          data->buffers[1]->data(),
          NULLPTR,
          data->dictionary->length,
          data->dictionary->buffers[0] != NULLPTR ? data->dictionary->buffers[0]->data() : NULLPTR,
          data->dictionary->buffers[1]->data(),
          data->dictionary->buffers.size() > 2 && data->dictionary->buffers[2] != NULLPTR
              ? data->dictionary->buffers[2]->data()
              : NULLPTR
      );
    } else if (is_binary_like(data->type->id())) {
      DCHECK(data->buffers.size() > 2 && data->buffers[2] != NULLPTR);
      column_array = KeyColumnArray(
          true, 0, sizeof(uint32_t), data->offset + start_row + num_rows,
          data->buffers[0] != NULLPTR ? data->buffers[0]->data() : NULLPTR,
          data->buffers[1]->data(),
          data->buffers[2]->data(),
          0, NULLPTR, NULLPTR, NULLPTR);
    } else if (is_large_binary_like(data->type->id())) {
      DCHECK(data->buffers.size() > 2 && data->buffers[2] != NULLPTR);
      column_array = KeyColumnArray(
          true, 0, sizeof(uint64_t), data->offset + start_row + num_rows,
          data->buffers[0] != NULLPTR ? data->buffers[0]->data() : NULLPTR,
          data->buffers[1]->data(),
          data->buffers[2]->data(),
          0, NULLPTR, NULLPTR, NULLPTR);
    } else {
      DCHECK(is_integer(data->type->id()) || is_temporal(data->type->id()));
      DCHECK(data->buffers.size() <= 2 || data->buffers[2] == NULLPTR);
      column_array = KeyColumnArray(
          false, 0, data->type->bit_width() / 8, data->offset + start_row + num_rows,
          data->buffers[0] != NULLPTR ? data->buffers[0]->data() : NULLPTR,
          data->buffers[1]->data(),
          NULLPTR, 0, NULLPTR, NULLPTR, NULLPTR);
    }
    return column_array.value().Slice(data->offset + start_row, num_rows);
  }
  KeyColumnArray Slice(int64_t offset, int64_t length) const {
    KeyColumnArray sliced = *this;
    // TODO
    return sliced;
  }
  void CheckAll() {
    DCHECK(buffers_[0]);
    DCHECK(dictionary_buffers_[0]);
    DCHECK(is_binary_like_ || index_length_ > 0 || value_length_ > 0 || length_ >= 0 || dictionary_length_ >= 0);
  }
 private:
  static constexpr int kMaxBuffers = 3;
  std::array<const uint8_t*, kMaxBuffers> buffers_;
  std::array<const uint8_t*, kMaxBuffers> dictionary_buffers_;

  /// \brief True if the value type is a string or binary type
  bool is_binary_like_;

  /// \brief The number of bytes in each index of the array or 0
  /// if it's not dictionary-encoded
  uint32_t index_length_;

  /// \brief Number of bytes for each underlying value in the array.
  /// If is_binary_like_ is true, this is the number of bytes in each
  /// offset
  uint32_t value_length_;
  
  /// \brief The number of rows in the array
  int64_t length_;

  /// \brief The number of entries in the dictionary or 0 if the
  /// array is not dictionary-encoded
  int64_t dictionary_length_;
};

// static void HashMultiColumn(const std::vector<KeyColumnArray>& cols, compute::LightContext* ctx, uint64_t* hashes) {
    // TODO
// }

}
}