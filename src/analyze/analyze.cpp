/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "analyze.h"

/**
 * @description: 分析器，进行语义分析和查询重写，需要检查不符合语义规定的部分
 * @param {shared_ptr<ast::TreeNode>} parse parser生成的结果集
 * @return {shared_ptr<Query>} Query 
 */
std::shared_ptr<Query> Analyze::do_analyze(std::shared_ptr<ast::TreeNode> parse)
{
    // query语句声明
    std::shared_ptr<Query> query = std::make_shared<Query>();
    // x指向不同类型的parse语法树
    // query根据x处理不同的语句
    
    if (auto x = std::dynamic_pointer_cast<ast::ExplainStmt>(parse)) {
        // 处理EXPLAIN语句，分析内部的SELECT语句
        auto inner_select = x->inner_stmt;
        
        // 处理表名
        query->tables = std::move(inner_select->tabs);
        /** TODO: 检查表是否存在 */
        for (auto &table : query->tables) {
            if (!sm_manager_->db_.is_table(table)) {
                throw TableNotFoundError(table);
            }
        }
        
        // 建立别名到表名的映射
        std::map<std::string, std::string> alias_to_table;
        std::map<std::string, std::string> table_to_alias;  // 表名到别名的反向映射
        for (size_t i = 0; i < query->tables.size() && i < inner_select->tab_aliases.size(); i++) {
            if (!inner_select->tab_aliases[i].empty()) {
                alias_to_table[inner_select->tab_aliases[i]] = query->tables[i];
                table_to_alias[query->tables[i]] = inner_select->tab_aliases[i];
            }
        }
        // 保存别名映射到Query对象中
        query->alias_to_table = alias_to_table;
        
        // 处理target list
        for (auto &sv_sel_col : inner_select->cols) {
            TabCol sel_col = {.tab_name = sv_sel_col->tab_name, .col_name = sv_sel_col->col_name};
            
            // 保存原始别名（如果有的话）
            std::string original_alias = sel_col.tab_name;
            
            // 如果使用了别名，转换为实际表名
            if (!sel_col.tab_name.empty() && alias_to_table.find(sel_col.tab_name) != alias_to_table.end()) {
                sel_col.tab_name = alias_to_table[sel_col.tab_name];
                sel_col.alias = original_alias;  // 保存别名
            } else if (!sel_col.tab_name.empty()) {
                // 如果没有使用别名，检查是否有为该表设置的别名
                if (table_to_alias.find(sel_col.tab_name) != table_to_alias.end()) {
                    sel_col.alias = table_to_alias[sel_col.tab_name];
                }
            }
            
            query->cols.push_back(sel_col);
        }
        
        std::vector<ColMeta> all_cols;
        get_all_cols(query->tables, all_cols);
        if (query->cols.empty()) {
            // select all columns - 这是SELECT *的情况
            query->is_select_all = true;
            for (auto &col : all_cols) {
                TabCol sel_col = {.tab_name = col.tab_name, .col_name = col.name};
                // 为SELECT *的列也添加别名信息
                if (table_to_alias.find(col.tab_name) != table_to_alias.end()) {
                    sel_col.alias = table_to_alias[col.tab_name];
                }
                query->cols.push_back(sel_col);
            }
        } else {
            // infer table name from column name
            for (auto &sel_col : query->cols) {
                sel_col = check_column(all_cols, sel_col);  // 列元数据校验
            }
        }

        //处理where条件
        get_clause(inner_select->conds, query->conds, alias_to_table);
        check_clause(query->tables, query->conds);
        
        //处理JOIN条件（分开存储，不混入WHERE条件）
        for (auto &join_expr : inner_select->jointree) {
            std::vector<Condition> join_conds;
            get_clause(join_expr->conds, join_conds, alias_to_table);
            check_clause(query->tables, join_conds);
            // 将JOIN条件单独存储
            query->join_conds.insert(query->join_conds.end(), join_conds.begin(), join_conds.end());
        }
        
    } else if (auto x = std::dynamic_pointer_cast<ast::SelectStmt>(parse))
    {
        // 处理表名
        query->tables = std::move(x->tabs);
        /** TODO: 检查表是否存在 */
        for (auto &table : query->tables) {
            if (!sm_manager_->db_.is_table(table)) {
                throw TableNotFoundError(table);
            }
        }

        // 建立别名到表名的映射
        std::map<std::string, std::string> alias_to_table;
        std::map<std::string, std::string> table_to_alias;  // 表名到别名的反向映射
        for (size_t i = 0; i < query->tables.size() && i < x->tab_aliases.size(); i++) {
            if (!x->tab_aliases[i].empty()) {
                alias_to_table[x->tab_aliases[i]] = query->tables[i];
                table_to_alias[query->tables[i]] = x->tab_aliases[i];
            }
        }
        // 保存别名映射到Query对象中
        query->alias_to_table = alias_to_table;

        // 处理target list，再target list中添加上表名，例如 a.id
        for (auto &sv_sel_col : x->cols) {
            TabCol sel_col = {.tab_name = sv_sel_col->tab_name, .col_name = sv_sel_col->col_name};
            
            // 保存原始别名（如果有的话）
            std::string original_alias = sel_col.tab_name;
            
            // 如果使用了别名，转换为实际表名
            if (!sel_col.tab_name.empty() && alias_to_table.find(sel_col.tab_name) != alias_to_table.end()) {
                sel_col.tab_name = alias_to_table[sel_col.tab_name];
                sel_col.alias = original_alias;  // 保存别名
            } else if (!sel_col.tab_name.empty()) {
                // 如果没有使用别名，检查是否有为该表设置的别名
                if (table_to_alias.find(sel_col.tab_name) != table_to_alias.end()) {
                    sel_col.alias = table_to_alias[sel_col.tab_name];
                }
            }
            
            query->cols.push_back(sel_col);
        }
        
        std::vector<ColMeta> all_cols;
        get_all_cols(query->tables, all_cols);
        if (query->cols.empty()) {
            // select all columns - 这是SELECT *的情况
            query->is_select_all = true;
            for (auto &col : all_cols) {
                TabCol sel_col = {.tab_name = col.tab_name, .col_name = col.name};
                // 为SELECT *的列也添加别名信息
                if (table_to_alias.find(col.tab_name) != table_to_alias.end()) {
                    sel_col.alias = table_to_alias[col.tab_name];
                }
                query->cols.push_back(sel_col);
            }
        } else {
            // infer table name from column name
            for (auto &sel_col : query->cols) {
                sel_col = check_column(all_cols, sel_col);  // 列元数据校验
            }
        }
        //处理where条件
        get_clause(x->conds, query->conds, alias_to_table);
        check_clause(query->tables, query->conds);
        
        //处理JOIN条件
        for (auto &join_expr : x->jointree) {
            std::vector<Condition> join_conds;
            get_clause(join_expr->conds, join_conds, alias_to_table);
            check_clause(query->tables, join_conds);
            // 将JOIN条件单独存储
            query->join_conds.insert(query->join_conds.end(), join_conds.begin(), join_conds.end());
        }
    } else if (auto x = std::dynamic_pointer_cast<ast::UpdateStmt>(parse)) {
        /** TODO: */
        // 首先提取set语句
        for (auto &set: x->set_clauses) {
            query->set_clauses.emplace_back(SetClause{
                TabCol{"", set->col_name, ""},   // 列名（表名为空，别名也为空）
                convert_sv_value(set->val)   // 值
            });
        }

        // 提取完set后检查左右值类型
        auto &table_meta = sm_manager_->db_.get_table(x->tab_name);
        for (auto &set: query->set_clauses) {
            auto col_meta = table_meta.get_col(set.lhs.col_name);
            if (col_meta->type != set.rhs.type) {
                if (col_meta->type == TYPE_FLOAT && set.rhs.type == TYPE_INT) {
                    set.rhs.set_float(static_cast<float>(set.rhs.int_val));
                } else {
                    throw IncompatibleTypeError(coltype2str(col_meta->type), coltype2str(set.rhs.type));
                }
            }
            // 一定要init_raw不然无法更新
            set.rhs.init_raw(col_meta->len);  // 初始化raw
        }

        // 处理where条件
        get_clause(x->conds, query->conds);
        check_clause({x->tab_name}, query->conds);
    } else if (auto x = std::dynamic_pointer_cast<ast::DeleteStmt>(parse)) {
        //处理where条件
        get_clause(x->conds, query->conds);
        check_clause({x->tab_name}, query->conds);        
    } else if (auto x = std::dynamic_pointer_cast<ast::InsertStmt>(parse)) {
        // 处理insert 的values值
        for (auto &sv_val : x->vals) {
            query->values.push_back(convert_sv_value(sv_val));
        }
    } else {
        // do nothing
    }
    query->parse = std::move(parse);
    return query;
}


TabCol Analyze::check_column(const std::vector<ColMeta> &all_cols, TabCol target) {
    if (target.tab_name.empty()) {
        // Table name not specified, infer table name from column name
        std::string tab_name;
        for (auto &col : all_cols) {
            if (col.name == target.col_name) {
                if (!tab_name.empty()) {
                    throw AmbiguousColumnError(target.col_name);
                }
                tab_name = col.tab_name;
            }
        }
        if (tab_name.empty()) {
            throw ColumnNotFoundError(target.col_name);
        }
        target.tab_name = tab_name;
    } else {
        /** TODO: Make sure target column exists */
        bool flag = true;
        for (auto &col : all_cols) {
            if (col.tab_name == target.tab_name && col.name == target.col_name) {
                flag = false;
                break;
            }
        }
        if (flag) {
            throw ColumnNotFoundError(target.col_name);
        }
    }
    return target;
}

void Analyze::get_all_cols(const std::vector<std::string> &tab_names, std::vector<ColMeta> &all_cols) {
    for (auto &sel_tab_name : tab_names) {
        // 这里db_不能写成get_db(), 注意要传指针
        const auto &sel_tab_cols = sm_manager_->db_.get_table(sel_tab_name).cols;
        all_cols.insert(all_cols.end(), sel_tab_cols.begin(), sel_tab_cols.end());
    }
}

void Analyze::get_clause(const std::vector<std::shared_ptr<ast::BinaryExpr>> &sv_conds, std::vector<Condition> &conds, const std::map<std::string, std::string> &alias_to_table) {
    conds.clear();
    for (auto &expr : sv_conds) {
        Condition cond;
        cond.lhs_col = {.tab_name = expr->lhs->tab_name, .col_name = expr->lhs->col_name};
        
        // 保存原始别名信息
        std::string original_left_alias = cond.lhs_col.tab_name;
        
        // 如果使用了别名，转换为实际表名但保留别名
        if (!cond.lhs_col.tab_name.empty() && alias_to_table.find(cond.lhs_col.tab_name) != alias_to_table.end()) {
            cond.lhs_col.alias = original_left_alias;  // 保存别名
            cond.lhs_col.tab_name = alias_to_table.at(cond.lhs_col.tab_name);  // 转换为表名
        }
        
        cond.op = convert_sv_comp_op(expr->op);
        if (auto rhs_val = std::dynamic_pointer_cast<ast::Value>(expr->rhs)) {
            cond.is_rhs_val = true;
            cond.rhs_val = convert_sv_value(rhs_val);
        } else if (auto rhs_col = std::dynamic_pointer_cast<ast::Col>(expr->rhs)) {
            cond.is_rhs_val = false;
            cond.rhs_col = {.tab_name = rhs_col->tab_name, .col_name = rhs_col->col_name};
            
            // 保存原始别名信息
            std::string original_right_alias = cond.rhs_col.tab_name;
            
            // 如果使用了别名，转换为实际表名但保留别名
            if (!cond.rhs_col.tab_name.empty() && alias_to_table.find(cond.rhs_col.tab_name) != alias_to_table.end()) {
                cond.rhs_col.alias = original_right_alias;  // 保存别名
                cond.rhs_col.tab_name = alias_to_table.at(cond.rhs_col.tab_name);  // 转换为表名
            }
        }
        conds.push_back(cond);
    }
}

void Analyze::check_clause(const std::vector<std::string> &tab_names, std::vector<Condition> &conds) {
    // auto all_cols = get_all_cols(tab_names);
    std::vector<ColMeta> all_cols;
    get_all_cols(tab_names, all_cols);
    // Get raw values in where clause
    for (auto &cond : conds) {
        // Infer table name from column name
        cond.lhs_col = check_column(all_cols, cond.lhs_col);
        if (!cond.is_rhs_val) {
            cond.rhs_col = check_column(all_cols, cond.rhs_col);
        }
        TabMeta &lhs_tab = sm_manager_->db_.get_table(cond.lhs_col.tab_name);
        auto lhs_col = lhs_tab.get_col(cond.lhs_col.col_name);
        ColType lhs_type = lhs_col->type;
        ColType rhs_type;
        if (cond.is_rhs_val) {
            cond.rhs_val.init_raw(lhs_col->len);
            rhs_type = cond.rhs_val.type;
        } else {
            TabMeta &rhs_tab = sm_manager_->db_.get_table(cond.rhs_col.tab_name);
            auto rhs_col = rhs_tab.get_col(cond.rhs_col.col_name);
            rhs_type = rhs_col->type;
        }
        if (lhs_type != rhs_type) {
            if (!(lhs_type == TYPE_FLOAT && rhs_type == TYPE_INT) &&
                !(lhs_type == TYPE_INT && rhs_type == TYPE_FLOAT)) {
                throw IncompatibleTypeError(coltype2str(lhs_type), coltype2str(rhs_type));
            }
        }
    }
}


Value Analyze::convert_sv_value(const std::shared_ptr<ast::Value> &sv_val) {
    Value val;
    if (auto int_lit = std::dynamic_pointer_cast<ast::IntLit>(sv_val)) {
        val.set_int(int_lit->val);
    } else if (auto float_lit = std::dynamic_pointer_cast<ast::FloatLit>(sv_val)) {
        val.set_float(float_lit->val);
    } else if (auto str_lit = std::dynamic_pointer_cast<ast::StringLit>(sv_val)) {
        val.set_str(str_lit->val);
    } else {
        throw InternalError("Unexpected sv value type");
    }
    return val;
}

CompOp Analyze::convert_sv_comp_op(ast::SvCompOp op) {
    std::map<ast::SvCompOp, CompOp> m = {
        {ast::SV_OP_EQ, OP_EQ}, {ast::SV_OP_NE, OP_NE}, {ast::SV_OP_LT, OP_LT},
        {ast::SV_OP_GT, OP_GT}, {ast::SV_OP_LE, OP_LE}, {ast::SV_OP_GE, OP_GE},
    };
    return m.at(op);
}
