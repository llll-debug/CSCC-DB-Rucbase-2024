/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "index/ix_index_handle.h"
#include "system/sm.h"
#include <float.h>
#include <limits.h>

class IndexScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;                      // 表名称
    TabMeta tab_;                               // 表的元数据
    std::vector<Condition> conds_;              // 扫描条件
    RmFileHandle *fh_;                          // 表的数据文件句柄
    std::vector<ColMeta> cols_;                 // 需要读取的字段
    size_t len_;                                // 选取出来的一条记录的长度
    std::vector<Condition> fed_conds_;          // 扫描条件，和conds_字段相同

    std::vector<std::string> index_col_names_;  // index scan涉及到的索引包含的字段
    IndexMeta index_meta_;                      // index scan涉及到的索引元数据

    Rid rid_;
    std::unique_ptr<RecScan> scan_;
    std::unique_ptr<RmRecord> record_;

    SmManager *sm_manager_;

    constexpr static int int_min_ = INT32_MIN;
    constexpr static int int_max_ = INT32_MAX;
    constexpr static float float_min_ = FLT_MIN;
    constexpr static float float_max_ = FLT_MAX;

   public:
    IndexScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, std::vector<std::string> index_col_names,
                    Context *context) {
        sm_manager_ = sm_manager;
        context_ = context;
        tab_name_ = std::move(tab_name);
        tab_ = sm_manager_->db_.get_table(tab_name_);
        conds_ = std::move(conds);
        // index_no_ = index_no;
        index_col_names_ = index_col_names; 
        // index_meta_ = *(tab_.get_index_meta(index_col_names_));
        index_meta_ = tab_.get_index_meta(index_col_names_);
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab_.cols;
        len_ = cols_.back().offset + cols_.back().len;
        std::map<CompOp, CompOp> swap_op = {
            {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
        };

        for (auto &cond : conds_) {
            if (cond.lhs_col.tab_name != tab_name_) {
                // lhs is on other table, now rhs must be on this table
                assert(!cond.is_rhs_val && cond.rhs_col.tab_name == tab_name_);
                // swap lhs and rhs
                std::swap(cond.lhs_col, cond.rhs_col);
                cond.op = swap_op.at(cond.op);
            }
        }
        fed_conds_ = conds_;
    }

    // void beginTuple() override {
    //     auto index_name = sm_manager_->get_ix_manager()->get_index_name(tab_name_, index_col_names_);
    //     auto *ih = sm_manager_->ihs_[index_name].get();
    
    //     Iid lower = ih->leaf_begin();
    //     Iid upper = ih->leaf_end();
    //     std::unique_ptr<char[]> key(new char[index_meta_.col_tot_len]());
    //     int key_pos = 0, eq_count = 0;
    
    //     // 1. 拼接所有等值条件（前缀）
    //     for (size_t i = 0; i < conds_.size(); ++i) {
    //         const auto &cond = conds_[i];
    //         if (cond.op == OP_EQ) {
    //             memcpy(key.get() + key_pos, cond.rhs_val.raw->data, cond.rhs_val.raw->size);
    //             key_pos += cond.rhs_val.raw->size;
    //             eq_count++;
    //         } else {
    //             break;
    //         }
    //     }
    
    //     // 2. 只用第一个范围条件裁剪区间
    //     bool has_range = false;
    //     for (size_t i = eq_count; i < conds_.size(); ++i) {
    //         const auto &cond = conds_[i];
    //         if (cond.op == OP_GT || cond.op == OP_GE || cond.op == OP_LT || cond.op == OP_LE) {
    //             std::unique_ptr<char[]> bound_key(new char[index_meta_.col_tot_len]());
    //             memcpy(bound_key.get(), key.get(), key_pos); // 复制等值前缀
    //             memcpy(bound_key.get() + key_pos, cond.rhs_val.raw->data, cond.rhs_val.raw->size);
    
    //             switch (cond.op) {
    //                 case OP_GT:
    //                     lower = ih->upper_bound(bound_key.get());
    //                     break;
    //                 case OP_GE:
    //                     lower = ih->lower_bound(bound_key.get());
    //                     break;
    //                 case OP_LT:
    //                     upper = ih->lower_bound(bound_key.get());
    //                     break;
    //                 case OP_LE:
    //                     upper = ih->upper_bound(bound_key.get());
    //                     break;
    //                 default:
    //                     break;
    //             }
    //             has_range = true;
    //             break; // 只用第一个范围条件
    //         }
    //     }
    
    //     // 3. 如果所有条件都是等值
    //     if (!has_range && eq_count == conds_.size()) {
    //         lower = ih->lower_bound(key.get());
    //         upper = ih->upper_bound(key.get());
    //     }
    
    //     // std::cout << "IndexScanExecutor: lower bound = " << lower.page_no << ", " << lower.slot_no
    //     //           << ", upper bound = " << upper.page_no << ", " << upper.slot_no << std::endl;
    
    //     scan_ = std::make_unique<IxScan>(ih, lower, upper, sm_manager_->get_bpm());
    //     while (!scan_->is_end()) {
    //         rid_ = scan_->rid();
    //         record_ = fh_->get_record(rid_, context_);
    //         if (check_conds(record_.get(), cols_, fed_conds_)) {
    //             break;
    //         }
    //         scan_->next();
    //     }
    // }

    void beginTuple() override {
        auto index_name = sm_manager_->get_ix_manager()->get_index_name(tab_name_, index_col_names_);
        auto *ih = sm_manager_->ihs_[index_name].get();

        Iid lower = ih->leaf_begin();
        Iid upper = ih->leaf_end();
        
        // 构建索引key
        std::unique_ptr<char[]> key(new char[index_meta_.col_tot_len]());
        int key_pos = 0, eq_count = 0;

        // 1. 拼接所有等值条件（前缀）
        for (size_t i = 0; i < conds_.size(); ++i) {
            const auto &cond = conds_[i];
            if (cond.op == OP_EQ) {
                memcpy(key.get() + key_pos, cond.rhs_val.raw->data, cond.rhs_val.raw->size);
                key_pos += cond.rhs_val.raw->size;
                eq_count++;
            } else {
                break;
            }
        }

        // 2. 处理范围条件
        bool has_range = false;
        for (size_t i = eq_count; i < conds_.size(); ++i) {
            const auto &cond = conds_[i];
            if (cond.op == OP_GT || cond.op == OP_GE || cond.op == OP_LT || cond.op == OP_LE) {
                std::unique_ptr<char[]> bound_key(new char[index_meta_.col_tot_len]());
                memcpy(bound_key.get(), key.get(), key_pos); // 复制等值前缀
                memcpy(bound_key.get() + key_pos, cond.rhs_val.raw->data, cond.rhs_val.raw->size);

                switch (cond.op) {
                    case OP_GT:
                        lower = ih->upper_bound(bound_key.get());
                        break;
                    case OP_GE:
                        lower = ih->lower_bound(bound_key.get());
                        break;
                    case OP_LT:
                        upper = ih->lower_bound(bound_key.get());
                        break;
                    case OP_LE:
                        upper = ih->upper_bound(bound_key.get());
                        break;
                    default:
                        break;
                }
                has_range = true;
                break; // 只用第一个范围条件
            }
        }

        // 3. 如果有等值条件（无论是否还有其他条件）
        if (!has_range && eq_count > 0) {
            // 构建下界和上界key
            std::unique_ptr<char[]> lower_key(new char[index_meta_.col_tot_len]());
            std::unique_ptr<char[]> upper_key(new char[index_meta_.col_tot_len]());
            
            // 复制等值前缀
            memcpy(lower_key.get(), key.get(), key_pos);
            memcpy(upper_key.get(), key.get(), key_pos);
            
            // 填充剩余字段：下界用最小值，上界用最大值
            char *lower_ptr = lower_key.get();
            char *upper_ptr = upper_key.get();
            set_remaining_all_min(key_pos, eq_count, lower_ptr);
            set_remaining_all_max(key_pos, eq_count, upper_ptr);
            
            lower = ih->lower_bound(lower_key.get());
            upper = ih->upper_bound(upper_key.get());
        }

        // std::cout << "IndexScanExecutor: lower bound = " << lower.page_no << ", " << lower.slot_no
        //           << ", upper bound = " << upper.page_no << ", " << upper.slot_no << std::endl;

        scan_ = std::make_unique<IxScan>(ih, lower, upper, sm_manager_->get_bpm());
        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            record_ = fh_->get_record(rid_, context_);
            if (check_conds(record_.get(), cols_, fed_conds_)) {
                break;
            }
            scan_->next();
        }
    }
    
    void nextTuple() override {
        if (scan_->is_end()) return;
        scan_->next();
        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            record_ = fh_->get_record(rid_, context_);
            if (check_conds(record_.get(), cols_, fed_conds_)) {
                break;
            }
            scan_->next();
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        return std::move(record_);
    }

    Rid &rid() override { return rid_; }

    bool is_end() const { return scan_->is_end(); }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    size_t tupleLen() const override { return len_; }

    // 注意索引是按多列联合排序的
    // 下界（lower）：用最小值补全，确保从第一个可能的key开始
    // 上界（upper）：用最大值补全，确保到最后一个可能的key结束。

    // 根据不同的列值类型设置不同的最大值
    // int   类型范围 int_min_ ~ int_max_
    // float 类型范围 float_min_ ~ float_max_
    // char  类型范围 0 ~ 255
    void set_remaining_all_max(int offset, int last_idx, char *&key) {
        for (size_t i = last_idx; i < index_meta_.cols.size(); ++i) {
            const ColMeta &col = index_meta_.cols[i];
            switch (col.type) {
                case TYPE_INT: {
                    int val = int_max_;
                    memcpy(key + offset, &val, sizeof(int));
                    offset += sizeof(int);
                    break;
                }
                case TYPE_FLOAT: {
                    float val = float_max_;
                    memcpy(key + offset, &val, sizeof(float));
                    offset += sizeof(float);
                    break;
                }
                case TYPE_STRING: {
                    std::memset(key + offset, 0xff, col.len);
                    offset += col.len;
                    break;
                }
                default:
                    throw InternalError("Unexpected data type!");
            }
        }
    }

    // 根据不同的列值类型设置不同的最小值
    // int   类型范围 int_min_ ~ int_max_
    // float 类型范围 float_min_ ~ float_max_
    // char  类型范围 0 ~ 255
    void set_remaining_all_min(int offset, int last_idx, char *&key) {
        for (size_t i = last_idx; i < index_meta_.cols.size(); ++i) {
            const ColMeta &col = index_meta_.cols[i];
            switch (col.type) {
                case TYPE_INT: {
                    int val = int_min_;
                    memcpy(key + offset, &val, sizeof(int));
                    offset += sizeof(int);
                    break;
                }
                case TYPE_FLOAT: {
                    float val = float_min_;
                    memcpy(key + offset, &val, sizeof(float));
                    offset += sizeof(float);
                    break;
                }
                case TYPE_STRING: {
                    std::memset(key + offset, 0, col.len);
                    offset += col.len;
                    break;
                }
                default:
                    throw InternalError("Unexpected data type!");
            }
        }
    }

    static inline int comp(const char *ldata, const char *rdata, int len, ColType lhs_type, ColType rhs_type) {
        // 支持 int/float 混合比较
        if ((lhs_type == TYPE_INT && rhs_type == TYPE_FLOAT) || (lhs_type == TYPE_FLOAT && rhs_type == TYPE_INT)) {
            float lval = (lhs_type == TYPE_INT) ? static_cast<float>(*reinterpret_cast<const int *>(ldata)) : *reinterpret_cast<const float *>(ldata);
            float rval = (rhs_type == TYPE_INT) ? static_cast<float>(*reinterpret_cast<const int *>(rdata)) : *reinterpret_cast<const float *>(rdata);
            return (lval < rval) ? -1 : (lval > rval) ? 1 : 0;
        }
        if (lhs_type != rhs_type) {
            throw IncompatibleTypeError(coltype2str(lhs_type), coltype2str(rhs_type));
        }
        switch (lhs_type) {
            case TYPE_INT: {
                const int lval = *reinterpret_cast<const int *>(ldata);
                const int rval = *reinterpret_cast<const int *>(rdata);
                return (lval < rval) ? -1 : (lval > rval) ? 1 : 0;
            }
            case TYPE_FLOAT: {
                const float lval = *reinterpret_cast<const float *>(ldata);
                const float rval = *reinterpret_cast<const float *>(rdata);
                return (lval < rval) ? -1 : (lval > rval) ? 1 : 0;
            }
            case TYPE_STRING: {
                return memcmp(ldata, rdata, len);
            }
            default:
                throw IncompatibleTypeError(coltype2str(lhs_type), "unknown");
        }
    }

    // 检查单个条件
    bool check_cond(const RmRecord *rec, const std::vector<ColMeta> &cols, const Condition &cond) {
        const auto &lhs_meta = get_col(cols, cond.lhs_col);   // 指向 cols_中对应的字段元数据
        const char *lhs_data = rec->data + lhs_meta->offset;  // 获取左侧字段的数据
        const char *rhs_data;
        ColType rhs_type;
        ColType lhs_type = lhs_meta->type;

        // 右值有可能是数据，也有可能是字段
        if (!cond.is_rhs_val) {
            // 右值是记录中的字段，获取元数据
            const auto &rhs_meta = get_col(cols, cond.rhs_col);
            rhs_type = rhs_meta->type;
            rhs_data = rec->data + rhs_meta->offset;
        } else {
            // 右值是数据
            rhs_type = cond.rhs_val.type;
            rhs_data = cond.rhs_val.raw->data;
        }

        int cmp = comp(lhs_data, rhs_data, lhs_meta->len, lhs_type, rhs_type);
        switch (cond.op) {
            case OP_EQ:
                return cmp == 0;
            case OP_NE:
                return cmp != 0;
            case OP_LT:
                return cmp < 0;
            case OP_GT:
                return cmp > 0;
            case OP_LE:
                return cmp <= 0;
            case OP_GE:
                return cmp >= 0;
            default:
                throw InternalError("Unknown comparison operator");
        }
    }

    // 检查所有条件
    bool check_conds(const RmRecord *rec, const std::vector<ColMeta> &cols, const std::vector<Condition> &conds) {
        for (const auto &cond : conds) {
            if (!check_cond(rec, cols, cond)) {
                return false;
            }
        }
        return true;
    }
};