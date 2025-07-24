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
#include "system/sm.h"

class NestedLoopJoinExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> left_;    // 左儿子节点（需要join的表）
    std::unique_ptr<AbstractExecutor> right_;   // 右儿子节点（需要join的表）
    size_t len_;                                // join后获得的每条记录的长度
    std::vector<ColMeta> cols_;                 // join后获得的记录的字段

    std::vector<Condition> fed_conds_;          // join条件
    std::unique_ptr<RmRecord> join_record_;
    std::unique_ptr<RmRecord> lrecord_;
    bool isend;

   public:
    NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right, 
                            std::vector<Condition> conds) {
        left_ = std::move(left);
        right_ = std::move(right);
        len_ = left_->tupleLen() + right_->tupleLen();
        cols_ = left_->cols();
        auto right_cols = right_->cols();
        for (auto &col : right_cols) {
            col.offset += left_->tupleLen();
        }

        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
        isend = false;
        fed_conds_ = std::move(conds);

    }

    void beginTuple() override {
        left_->beginTuple();
        right_->beginTuple();

        while (!left_->is_end()) {
            lrecord_ = left_->Next();
            while (!right_->is_end()) {
                auto &&rrecord = right_->Next();
                if (check_conds(lrecord_.get(), rrecord.get(), cols_, fed_conds_)) {
                    join_record_ = std::make_unique<RmRecord>(len_);
                    memcpy(join_record_->data, lrecord_->data, left_->tupleLen());
                    memcpy(join_record_->data + left_->tupleLen(), rrecord->data, right_->tupleLen());
                    // 得到第一个满足条件的tuple，返回
                    return;
                }
                // 不满足条件，继续在右表找
                right_->nextTuple();
            }
            // 没有找到，从左表下一个记录开始，重置右表
            left_->nextTuple();
            right_->beginTuple();
        }
    }

    void nextTuple() override {
        if (left_->is_end()) {
            return;
        }

        right_->nextTuple();

        while (!left_->is_end()) {
            while (!right_->is_end()) {
                auto &&rrecord = right_->Next();
                if (check_conds(lrecord_.get(), rrecord.get(), cols_, fed_conds_)) {
                    join_record_ = std::make_unique<RmRecord>(len_);
                    memcpy(join_record_->data, lrecord_->data, left_->tupleLen());
                    memcpy(join_record_->data + left_->tupleLen(), rrecord->data, right_->tupleLen());
                    return;
                }
                right_->nextTuple();
            }
            left_->nextTuple();
            if (left_->is_end()) {
                break;
            }
            lrecord_ = left_->Next();
            right_->beginTuple();
        }
    }


    // 获取当前指向的那条记录，并返回其内容
    // 构造并返回一条记录
    // 只是读取当前内容
    std::unique_ptr<RmRecord> Next() override {
        return std::move(join_record_);
    }

    bool is_end() const override {
        return left_->is_end();
    }

    Rid &rid() override { return _abstract_rid; }

    const std::vector<ColMeta> &cols() const override {
        return cols_;
    }

    size_t tupleLen() const override {
        return len_;
    }

    static inline int comp(const char *ldata, const char *rdata, int len, ColType lhs_type, ColType rhs_type) {
        // 支持 int/float 混合比较
        if ((lhs_type == TYPE_INT && rhs_type == TYPE_FLOAT) || (lhs_type == TYPE_FLOAT && rhs_type == TYPE_INT)) {
            // std::cout << "Comparing int and float types" << std::endl;
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
    bool check_cond(const RmRecord *lrecord, const RmRecord *rrecord, const std::vector<ColMeta> &cols, const Condition &cond) {
        const auto &lhs_meta = get_col(cols, cond.lhs_col);   // 指向 cols_中对应的字段元数据
        const char *lhs_data = lrecord->data + lhs_meta->offset;  // 获取左侧字段的数据
        const char *rhs_data;
        ColType rhs_type;
        ColType lhs_type = lhs_meta->type;

        // 右值有可能是数据，也有可能是字段
        if (!cond.is_rhs_val) {
            // 右值是记录中的字段，获取元数据
            const auto &rhs_meta = get_col(cols, cond.rhs_col);
            rhs_type = rhs_meta->type;
            rhs_data = rrecord->data + rhs_meta->offset - left_->tupleLen();
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
    bool check_conds(const RmRecord *lrecord, const RmRecord *rrecord, const std::vector<ColMeta> &cols, const std::vector<Condition> &conds) {
        for (const auto &cond : conds) {
            if (!check_cond(lrecord, rrecord, cols, cond)) {
                return false;  // 如果有一个条件不满足，则返回false
            }
        }
        return true;
    }
};