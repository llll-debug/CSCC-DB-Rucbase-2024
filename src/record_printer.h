/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL
v2. You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <cassert>
#include <iomanip>
#include <iostream>
#include <string>

#include "common/config.h"
#include "common/context.h"

#define RECORD_COUNT_LENGTH 40

class RecordPrinter {
  static constexpr size_t COL_WIDTH = 16;
  size_t num_cols;

 public:
  RecordPrinter(size_t num_cols_) : num_cols(num_cols_) {
    assert(num_cols_ > 0);
  }

  void print_separator(Context* context) const {
    for (size_t i = 0; i < num_cols; i++) {
      // std::cout << '+' << std::string(COL_WIDTH + 2, '-');
      std::string str = "+" + std::string(COL_WIDTH + 2, '-');
      if (context->ellipsis_ == false &&
          *context->offset_ + RECORD_COUNT_LENGTH + str.length() <
              BUFFER_LENGTH) {
        memcpy(context->data_send_ + *(context->offset_), str.c_str(),
               str.length());
        *(context->offset_) = *(context->offset_) + str.length();
      } else {
        context->ellipsis_ = true;
      }
    }
    std::string str = "+\n";
    if (context->ellipsis_ == false &&
        *context->offset_ + RECORD_COUNT_LENGTH + str.length() <
            BUFFER_LENGTH) {
      memcpy(context->data_send_ + *(context->offset_), str.c_str(),
             str.length());
      *(context->offset_) = *(context->offset_) + str.length();
    } else {
      context->ellipsis_ = true;
    }
  }

  void print_record(const std::vector<std::string>& rec_str,
                    Context* context) const {
    assert(rec_str.size() == num_cols);
    std::stringstream ss;

    for (auto col : rec_str) {
      if (col.size() > COL_WIDTH) {
        col = col.substr(0, COL_WIDTH - 3) + "...";
      }
      ss << "| " << std::setw(COL_WIDTH) << col << " ";
    }
    ss << "|\n";

    std::string output = ss.str();
    size_t output_length = output.length();

    if (context->ellipsis_ == false &&
        *context->offset_ + RECORD_COUNT_LENGTH + output_length <
            BUFFER_LENGTH) {
      memcpy(context->data_send_ + *(context->offset_), output.c_str(),
             output_length);
      *(context->offset_) += output_length;
    } else {
      context->ellipsis_ = true;
    }

    // assert(rec_str.size() == num_cols);
    // for (auto col: rec_str) {
    //     if (col.size() > COL_WIDTH) {
    //         col = col.substr(0, COL_WIDTH - 3) + "...";
    //     }
    //     // std::cout << "| " << std::setw(COL_WIDTH) << col << ' ';
    //     std::stringstream ss;
    //     ss << "| " << std::setw(COL_WIDTH) << col << " ";
    //     if (context->ellipsis_ == false && *context->offset_ +
    //     RECORD_COUNT_LENGTH + ss.str().length() <
    //         BUFFER_LENGTH) {
    //         memcpy(context->data_send_ + *(context->offset_),
    //         ss.str().c_str(), ss.str().length());
    //         *(context->offset_) = *(context->offset_) + ss.str().length();
    //     } else {
    //         context->ellipsis_ = true;
    //     }
    // }
    // // std::cout << "|\n";
    // std::string str = "|\n";
    // if (context->ellipsis_ == false && *context->offset_ +
    // RECORD_COUNT_LENGTH + str.length() < BUFFER_LENGTH) {
    //     memcpy(context->data_send_ + *(context->offset_), str.c_str(),
    //     str.length());
    //     *(context->offset_) = *(context->offset_) + str.length();
    // }
  }

  void print_indexs(const std::vector<std::string>& indexs,
                    Context* context) const {
    assert(indexs.size() == num_cols);

    // 预分配足够的空间
    std::string buffer;
    buffer.reserve(BUFFER_LENGTH);

    // 拷贝
    auto prev_offset = *context->offset_;
    for (const auto& col : indexs) {
      std::string ss = "| " + col + " ";
      if (!context->ellipsis_ &&
          *context->offset_ + RECORD_COUNT_LENGTH + ss.length() <
              BUFFER_LENGTH) {
        buffer.append(ss);
        *context->offset_ += ss.length();
      } else {
        context->ellipsis_ = true;
      }
    }

    std::string end_str = "|\n";
    if (!context->ellipsis_ &&
        *context->offset_ + RECORD_COUNT_LENGTH + end_str.length() <
            BUFFER_LENGTH) {
      buffer.append(end_str);
      *context->offset_ += end_str.length();
    }

    // 一次性拷贝整个字符串
    if (!buffer.empty()) {
      memcpy(context->data_send_ + prev_offset, buffer.c_str(),
             buffer.length());
    }
  }

  static void print_record_count(size_t num_rec, Context* context) {
    // std::cout << "Total record(s): " << num_rec << '\n';
    std::string str = "";
    if (context->ellipsis_ == true) {
      str = "... ...\n";
    }
    str += "Total record(s): " + std::to_string(num_rec) + '\n';
    memcpy(context->data_send_ + *(context->offset_), str.c_str(),
           str.length());
    *(context->offset_) = *(context->offset_) + str.length();
  }
};
