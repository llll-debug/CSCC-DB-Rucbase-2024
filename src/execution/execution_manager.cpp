/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL
v2. You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "execution_manager.h"

#include "executor_delete.h"
#include "executor_index_scan.h"
#include "executor_insert.h"
#include "executor_nestedloop_join.h"
#include "executor_projection.h"
#include "executor_seq_scan.h"
#include "executor_sortmerge_join.h"
#include "executor_update.h"
#include "index/ix.h"
#include "record_printer.h"

const char* help_info =
    "Supported SQL syntax:\n"
    "  command ;\n"
    "command:\n"
    "  CREATE TABLE table_name (column_name type [, column_name type ...])\n"
    "  DROP TABLE table_name\n"
    "  CREATE INDEX table_name (column_name)\n"
    "  DROP INDEX table_name (column_name)\n"
    "  INSERT INTO table_name VALUES (value [, value ...])\n"
    "  DELETE FROM table_name [WHERE where_clause]\n"
    "  UPDATE table_name SET column_name = value [, column_name = value ...] "
    "[WHERE where_clause]\n"
    "  SELECT selector FROM table_name [WHERE where_clause]\n"
    "type:\n"
    "  {INT | FLOAT | CHAR(n)}\n"
    "where_clause:\n"
    "  condition [AND condition ...]\n"
    "condition:\n"
    "  column op {column | value}\n"
    "column:\n"
    "  [table_name.]column_name\n"
    "op:\n"
    "  {= | <> | < | > | <= | >=}\n"
    "selector:\n"
    "  {* | column [, column ...]}\n";

constexpr const char* digits = "0123456789abcdefghijklmnopqrstuvwxyz";

char* my_itoa(int value, char* buff, int radix) {
  assert(buff != NULL);
  assert(radix >= 2 && radix <= 36);  // Ensure radix is within valid range

  char* ptr = buff;
  char* ptr1 = buff;
  char tmp_char;
  int tmp_value;

  // Handle negative numbers for base 10
  if (value < 0 && radix == 10) {
    *ptr++ = '-';
    value = -value;
  }

  // Convert integer to string
  do {
    tmp_value = value;
    value /= radix;
    *ptr++ = digits[abs(tmp_value - value * radix)];
  } while (value);

  // Null-terminate the string
  *ptr-- = '\0';

  // Reverse the string
  if (*buff == '-') {
    ptr1++;  // Skip the minus sign for reversal
  }

  while (ptr1 < ptr) {
    tmp_char = *ptr;
    *ptr-- = *ptr1;
    *ptr1++ = tmp_char;
  }

  return buff;
}

// 主要负责执行DDL语句
void QlManager::run_mutli_query(std::shared_ptr<Plan>& plan, Context* context) {
  if (auto x = std::dynamic_pointer_cast<DDLPlan>(plan)) {
    switch (x->tag) {
      case T_CreateTable: {
        sm_manager_->create_table(x->tab_name_, x->cols_, context);
        break;
      }
      case T_DropTable: {
        sm_manager_->drop_table(x->tab_name_, context);
        break;
      }
      case T_CreateIndex: {
        sm_manager_->create_index(x->tab_name_, x->tab_col_names_, context);
        break;
      }
      case T_DropIndex: {
        sm_manager_->drop_index(x->tab_name_, x->tab_col_names_, context);
        break;
      }
      default:
        throw InternalError("Unexpected field type");
    }
  }
}

// 执行help; show tables; desc table; begin; commit; abort;语句
void QlManager::run_cmd_utility(std::shared_ptr<Plan>& plan,
                                const txn_id_t* txn_id,
                                Context* context) const {
  if (auto x = std::dynamic_pointer_cast<OtherPlan>(plan)) {
    switch (x->tag) {
      case T_Help: {
        memcpy(context->data_send_ + *(context->offset_), help_info,
               strlen(help_info));
        *(context->offset_) = strlen(help_info);
        break;
      }
      case T_ShowTable: {
        sm_manager_->show_tables(context);
        break;
      }
      case T_ShowIndex: {
        sm_manager_->show_indexs(x->tab_name_, context);
        break;
      }
      case T_DescTable: {
        sm_manager_->desc_table(x->tab_name_, context);
        break;
      }
      case T_Transaction_begin: {
        // 显示开启一个事务
        context->txn_->set_txn_mode(true);
        break;
      }
      case T_Transaction_commit: {
        context->txn_ = txn_mgr_->get_transaction(*txn_id);
        txn_mgr_->commit(context->txn_, context->log_mgr_);
        break;
      }
      case T_Transaction_rollback: {
        context->txn_ = txn_mgr_->get_transaction(*txn_id);
        txn_mgr_->abort(context->txn_, context->log_mgr_);
        break;
      }
      case T_Transaction_abort: {
        context->txn_ = txn_mgr_->get_transaction(*txn_id);
        txn_mgr_->abort(context->txn_, context->log_mgr_);
        break;
      }
      default:
        throw InternalError("Unexpected field type");
    }
  } else if (auto x = std::dynamic_pointer_cast<SetKnobPlan>(plan)) {
    switch (x->set_knob_type_) {
      case ast::SetKnobType::EnableOutputFile: {
        planner_->enable_output_file = x->bool_value_;
        break;
      }
      case ast::SetKnobType::EnableNestLoop: {
        planner_->set_enable_nestedloop_join(x->bool_value_);
        break;
      }
      case ast::SetKnobType::EnableSortMerge: {
        planner_->set_enable_sortmerge_join(x->bool_value_);
        break;
      }
      default: {
        throw RMDBError("Not implemented!\n");
      }
    }
  } else if (auto x = std::dynamic_pointer_cast<StaticCheckpointPlan>(plan)) {
    // TODO
    // 这里静态检查点作了优化，直接把检查点之前的日志清空，这样有检查点地址一定是日志文件头，且是最后一个检查点
    // 先提交了再刷盘
    // 1.停止接收新事务和正在运行事务
    // 2.将仍保留在日志缓冲区中的内容写到日志文件中
    // 如果是隐式事务执行，这里设置为显式，这样就不会多写一条 commit
    // log，保证日志纯净
    context->txn_->set_txn_mode(true);
    context->txn_ = txn_mgr_->get_transaction(*txn_id);
    txn_mgr_->commit(context->txn_, context->log_mgr_);

    // 3.在日志文件中写入一个“检查点记录” 忽略
    // auto *static_checkpoint_log_record = new
    // StaticCheckpointLogRecord(context->txn_->get_transaction_id());
    // static_checkpoint_log_record->prev_lsn_ = context->txn_->get_prev_lsn();
    // context->txn_->set_prev_lsn(context->log_mgr_->add_log_to_buffer(static_checkpoint_log_record));
    // log_manager->flush_log_to_disk();

    // 4.将当前数据库缓冲区中的内容写到磁盘中
    sm_manager_->flush_meta();

    for (auto& [_, fh] : sm_manager_->fhs_) {
      std::ignore = _;
      sm_manager_->get_rm_manager()->flush_file(fh.get());
    }
    for (auto& [_, ih] : sm_manager_->ihs_) {
      std::ignore = _;
      sm_manager_->get_ix_manager()->flush_index(ih.get());
    }
    // 直接把日志清空
    // 如果日志文件已经开启，先关闭
    auto disk_manager = sm_manager_->get_disk_manager();
    auto log_fd = disk_manager->GetLogFd();
    if (log_fd != -1) {
      disk_manager->close_file(log_fd);
      sm_manager_->get_disk_manager()->SetLogFd(-1);
    }

    std::ofstream ofs(LOG_FILE_NAME, std::ios::trunc);
    ofs.close();

    // exit(1);
    // 5.把日志文件中检查点记录的地址写到“重新启动文件”中 忽略
  }
}

// 执行select语句，select语句的输出除了需要返回客户端外，还需要写入output.txt文件中
void QlManager::select_from(std::unique_ptr<AbstractExecutor>& executorTreeRoot,
                            std::vector<TabCol>& sel_cols, Context* context) {
  std::vector<std::string> captions;
  captions.reserve(sel_cols.size());
  for (auto& sel_col : sel_cols) {
    captions.emplace_back(std::move(sel_col.col_name));
  }

  // Print header into buffer
  RecordPrinter rec_printer(sel_cols.size());
  rec_printer.print_separator(context);
  rec_printer.print_record(captions, context);
  rec_printer.print_separator(context);
  // print header into file
  std::fstream outfile;
  if (planner_->enable_output_file) {
    outfile.open("output.txt", std::ios::out | std::ios::app);
    outfile << "|";
    for (size_t i = 0; i < captions.size(); ++i) {
      outfile << " " << captions[i] << " |";
    }
    outfile << "\n";
  }

  // Print records
  size_t num_rec = 0;
  std::vector<std::string> columns;
  // 预留空间
  columns.reserve(executorTreeRoot->cols().size());
  // 执行query_plan
  for (executorTreeRoot->beginTuple(); !executorTreeRoot->is_end();
       executorTreeRoot->nextTuple()) {
    columns.clear();
    auto Tuple = executorTreeRoot->Next();
    for (auto& col : executorTreeRoot->cols()) {
      std::string col_str;
      char* rec_buf = Tuple->data + col.offset;
      if (col.type == TYPE_INT) {
        col_str = std::to_string(*(int*)rec_buf);
      } else if (col.type == TYPE_FLOAT) {
        col_str = std::to_string(*(float*)rec_buf);
      } else if (col.type == TYPE_STRING) {
        col_str = std::string((char*)rec_buf, col.len);
        col_str.resize(strlen(col_str.c_str()));
      }
      // 移动语义
      columns.emplace_back(std::move(col_str));
    }
    // print record into buffer
    rec_printer.print_record(columns, context);
    // print record into file
    if (planner_->enable_output_file) {
      outfile << "|";
      for (size_t i = 0; i < columns.size(); ++i) {
        outfile << " " << columns[i] << " |";
      }
      outfile << "\n";
    }
    num_rec++;
  }
  outfile.close();
  // Print footer into buffer
  rec_printer.print_separator(context);
  // Print record count into buffer
  RecordPrinter::print_record_count(num_rec, context);
}

// 执行select语句，select语句的输出除了需要返回客户端外，还需要写入output.txt文件中
void QlManager::select_fast_count_star(int count, std::string& sel_col,
                                       Context* context) {
  std::vector<std::string> captions;
  captions.emplace_back(std::move(sel_col));

  // Print header into buffer
  RecordPrinter rec_printer(1);
  rec_printer.print_separator(context);
  rec_printer.print_record(captions, context);
  rec_printer.print_separator(context);
  // print header into file
  std::fstream outfile;
  if (planner_->enable_output_file) {
    outfile.open("output.txt", std::ios::out | std::ios::app);
    outfile << "|";
    for (size_t i = 0; i < captions.size(); ++i) {
      outfile << " " << captions[i] << " |";
    }
    outfile << "\n";
  }

  // Print records
  size_t num_rec = 1;
  std::vector<std::string> columns;
  char buffer[20]{};
  columns.emplace_back(my_itoa(count, buffer, 10));

  // print record into buffer
  rec_printer.print_record(columns, context);
  // print record into file
  if (planner_->enable_output_file) {
    outfile << "|";
    for (size_t i = 0; i < columns.size(); ++i) {
      outfile << " " << columns[i] << " |";
    }
    outfile << "\n";
  }
  outfile.close();
  // Print footer into buffer
  rec_printer.print_separator(context);
  // Print record count into buffer
  RecordPrinter::print_record_count(num_rec, context);
}

// 执行DML语句
void QlManager::run_dml(std::unique_ptr<AbstractExecutor>& exec) {
  exec->Next();
}
