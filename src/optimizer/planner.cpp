/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "planner.h"

#include <memory>
#include <set>

#include "execution/executor_delete.h"
#include "execution/executor_index_scan.h"
#include "execution/executor_insert.h"
#include "execution/executor_nestedloop_join.h"
#include "execution/executor_projection.h"
#include "execution/executor_seq_scan.h"
#include "execution/executor_update.h"
#include "index/ix.h"
#include "record_printer.h"
#include "query_optimizer.h"

// TOBEDONE
// 目前的索引匹配规则为：完全匹配索引字段，且全部为单点查询，不会自动调整where条件的顺序
bool Planner::get_index_cols(std::string &tab_name, std::vector<Condition> &curr_conds,
                             std::vector<std::string> &index_col_names) {
    // index_col_names.clear();
    // for (auto &cond: curr_conds) {
    //     if (cond.is_rhs_val && cond.op == OP_EQ && cond.lhs_col.tab_name == tab_name)
    //         index_col_names.push_back(cond.lhs_col.col_name);
    // }

    if (curr_conds.empty()) {
        return false;
    }

    TabMeta &tab = sm_manager_->db_.get_table(tab_name);
    // TODO 优化：减少索引文件名长度，提高匹配效率
    // conds重复去重

    std::set<std::string> index_set; // 快速查找
    std::unordered_map<std::string, int> conds_map; // 列名 -> Cond
    std::unordered_map<std::string, int> repelicate_conds_map;
    for (std::size_t i = 0; i < curr_conds.size(); ++i) {
        auto &col_name = curr_conds[i].lhs_col.col_name;
        if (index_set.count(col_name) == 0) {
            index_set.emplace(col_name);
            conds_map.emplace(col_name, i);
        } else {
            repelicate_conds_map.emplace(col_name, i);
        }
    }

    int max_len = 0, max_equals = 0, cur_len = 0, cur_equals = 0;
    for (auto &[index_name, index] : tab.indexes) {
        cur_len = cur_equals = 0;
        auto &cols = index.cols;
        for (auto &col : index.cols) {
            if (index_set.count(col.name) == 0) {
                break;
            }
            if (curr_conds[conds_map[col.name]].op == OP_EQ) {
                ++cur_equals;
            }
            ++cur_len;
        }
        // 如果有 where a = 1, b = 1, c > 1;
        // index(a, b, c), index(a, b, c, d);
        // 应该匹配最合适的，避免索引查询中带来的额外拷贝开销
        if (cur_len > max_len && cur_len < curr_conds.size()) {
            // 匹配最长的
            max_len = cur_len;
            index_col_names.clear();
            for (int i = 0; i < index.cols.size(); ++i) {
                index_col_names.emplace_back(index.cols[i].name);
            }
        } else if (cur_len == curr_conds.size()) {
            max_len = cur_len;
            // 最长前缀相等选择等号多的
            if (index_col_names.empty()) {
                for (int i = 0; i < index.cols.size(); ++i) {
                    index_col_names.emplace_back(index.cols[i].name);
                }

            } else if (cur_equals > max_equals) {
                max_equals = cur_equals;
                // cur_len >= cur_equals;
                index_col_names.clear();
                for (int i = 0; i < index.cols.size(); ++i) {
                    index_col_names.emplace_back(index.cols[i].name);
                }
                // for (int i = 0; i < cur_len; ++i) {
                //     index_col_names.emplace_back(index.cols[i].name);
                // }
            }
        }
    }

    // 没有索引
    if (index_col_names.empty()) {
        return false;
    }

    std::vector<Condition> fed_conds; // 理想谓词

    // 连接剩下的非索引列
    // 先清除已经在set中的
    for (auto &index_name : index_col_names) {
        if (index_set.count(index_name)) {
            index_set.erase(index_name);
            fed_conds.emplace_back(std::move(curr_conds[conds_map[index_name]]));
        }
    }

    // 连接 set 中剩下的
    for (auto &index_name : index_set) {
        fed_conds.emplace_back(std::move(curr_conds[conds_map[index_name]]));
    }

    // 连接重复的，如果有
    for (auto &[index_name, idx] : repelicate_conds_map) {
        fed_conds.emplace_back(std::move(curr_conds[repelicate_conds_map[index_name]]));
    }

    curr_conds = std::move(fed_conds);

    // 检查正确与否
    // for (auto &index_name : index_col_names) {
    //     std::cout << index_name << ",";
    // }
    // std::cout << "\n";

    // if (tab.is_index(index_col_names)) return true;
    return true;
}

/**
 * @brief 表算子条件谓词生成
 *
 * @param conds 条件
 * @param tab_names 表名
 * @return std::vector<Condition>
 */
// 筛选出与指定表相关的条件
std::vector<Condition> pop_conds(std::vector<Condition> &conds, std::string tab_names) {
    std::vector<Condition> solved_conds;
    auto it = conds.begin();
    while (it != conds.end()) {
        // 单表条件：左列属于目标表且右边是值，或者左右列都属于同一个表
        if ((tab_names.compare(it->lhs_col.tab_name) == 0 && it->is_rhs_val) || 
            (!it->is_rhs_val && it->lhs_col.tab_name.compare(it->rhs_col.tab_name) == 0 && it->lhs_col.tab_name.compare(tab_names) == 0)) {
            solved_conds.emplace_back(std::move(*it));
            it = conds.erase(it);
        } else {
            it++;
        }
    }
    return solved_conds;
}

int push_conds(Condition *cond, std::shared_ptr<Plan> plan)
{
    if(auto x = std::dynamic_pointer_cast<ScanPlan>(plan))
    {
        if(x->tab_name_.compare(cond->lhs_col.tab_name) == 0) {
            return 1;
        } else if(x->tab_name_.compare(cond->rhs_col.tab_name) == 0){
            return 2;
        } else {
            return 0;
        }
    }
    else if(auto x = std::dynamic_pointer_cast<JoinPlan>(plan))
    {
        int left_res = push_conds(cond, x->left_);
        // 条件已经下推到左子节点
        if(left_res == 3){
            return 3;
        }
        int right_res = push_conds(cond, x->right_);
        // 条件已经下推到右子节点
        if(right_res == 3){
            return 3;
        }
        // 左子节点或右子节点有一个没有匹配到条件的列
        if(left_res == 0 || right_res == 0) {
            return left_res + right_res;
        }
        // 左子节点匹配到条件的右边
        if(left_res == 2) {
            // 需要将左右两边的条件变换位置
            std::map<CompOp, CompOp> swap_op = {
                {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
            };
            std::swap(cond->lhs_col, cond->rhs_col);
            cond->op = swap_op.at(cond->op);
        }
        x->conds_.emplace_back(std::move(*cond));
        return 3;
    }
    return false;
}

// 找到表名为table的ScanPlan，如果找到就将其标记为已扫描，并将其添加到joined_tables中
std::shared_ptr<Plan> pop_scan(int *scantbl, std::string table, std::vector<std::string> &joined_tables, 
                std::vector<std::shared_ptr<Plan>> plans)
{
    for (size_t i = 0; i < plans.size(); i++) {
        auto x = std::dynamic_pointer_cast<ScanPlan>(plans[i]);
        if(x->tab_name_.compare(table) == 0)
        {
            scantbl[i] = 1;
            joined_tables.emplace_back(x->tab_name_);
            return plans[i];
        }
    }
    return nullptr;
}


std::shared_ptr<Query> Planner::logical_optimization(std::shared_ptr<Query> query, Context *context)
{
    
    //TODO 实现逻辑优化规则

    return query;
}

std::shared_ptr<Plan> Planner::physical_optimization(std::shared_ptr<Query> query, Context *context)
{
    std::shared_ptr<Plan> plan = make_one_rel(query);
    
    // 其他物理优化

    // 处理orderby
    plan = generate_sort_plan(query, std::move(plan)); 

    return plan;
}

// **多表查询**
// SELECT * FROM A, B, C WHERE A.id = B.id AND B.x = C.x AND A.y > 10;
// 对每个表生成ScanPlan，并把相关条件分配给对应的ScanPlan，A.y > 10分配给A，B和C没有单表条件
// 处理连接条件，构建JoinPlan
// 首先处理第一个连接条件 A.id == B.id，用pop_scan找到A和B的ScanPlan，标记 scantbl[0]=1, scantbl[1]=1，joined_tables = [A, B]
// 生成 JoinPlan1 = JoinPlan(ScanPlan(A, [A.y > 10]), ScanPlan(B, []), [A.id = B.id])
// 再处理下一个连接条件 B.x == C.x，发现C还没参与连接，pop_scan 找到 C 的 ScanPlan，标记 scantbl[2]=1，joined_tables = [A, B, C]
// 生成 JoinPlan2 = JoinPlan(JoinPlan1, ScanPlan(C, []), [B.x = C.x])
// 检查是否有未加入 join 的表，scantbl = [1, 1, 1]，所有表都已加入 join，无需补全。
std::shared_ptr<Plan> Planner::make_one_rel(std::shared_ptr<Query> query)
{
    auto x = std::dynamic_pointer_cast<ast::SelectStmt>(query->parse);
    std::vector<std::string> tables = query->tables;
    
    // 生成表扫描计划，只处理能推下去的WHERE条件（单表条件）
    std::vector<std::shared_ptr<Plan>> table_scan_executors(tables.size());
    for (size_t i = 0; i < tables.size(); i++) {
        auto curr_conds = pop_conds(query->conds, tables[i]);
        std::vector<std::string> index_col_names;
        bool index_exist = get_index_cols(tables[i], curr_conds, index_col_names);
        
        if (index_exist == false) {
            index_col_names.clear();
            table_scan_executors[i] = 
                std::make_shared<ScanPlan>(T_SeqScan, sm_manager_, tables[i], curr_conds, index_col_names);
        } else {
            table_scan_executors[i] =
                std::make_shared<ScanPlan>(T_IndexScan, sm_manager_, tables[i], curr_conds, index_col_names);
        }
    }
    
    // 如果只有一个表，直接返回扫描计划
    if(tables.size() == 1) {
        return table_scan_executors[0];
    }
    
    // 获取JOIN条件和剩余的WHERE条件
    auto join_conds = std::move(query->join_conds);  // JOIN ON条件
    auto where_conds = std::move(query->conds);      // 剩余的WHERE条件
    
    std::shared_ptr<Plan> result_plan;
    
    // 处理JOIN操作
    if (join_conds.size() >= 1) {
        // 有JOIN ON条件，按照JOIN条件构建连接
    int scantbl[tables.size()];
        for(size_t i = 0; i < tables.size(); i++) {
        scantbl[i] = -1;
    }
        
        std::vector<std::string> joined_tables;
        
        // 处理第一个JOIN条件
        auto it = join_conds.begin();
        std::shared_ptr<Plan> left, right;
            left = pop_scan(scantbl, it->lhs_col.tab_name, joined_tables, table_scan_executors);
            right = pop_scan(scantbl, it->rhs_col.tab_name, joined_tables, table_scan_executors);
        
        std::vector<Condition> single_join_cond{*it};
        
        // 创建第一个JOIN
            if(enable_nestedloop_join && enable_sortmerge_join) {
            result_plan = std::make_shared<JoinPlan>(T_NestLoop, std::move(left), std::move(right), single_join_cond);
            } else if(enable_nestedloop_join) {
            result_plan = std::make_shared<JoinPlan>(T_NestLoop, std::move(left), std::move(right), single_join_cond);
            } else if(enable_sortmerge_join) {
            result_plan = std::make_shared<JoinPlan>(T_SortMerge, std::move(left), std::move(right), single_join_cond);
            } else {
                throw RMDBError("No join executor selected!");
            }

        it = join_conds.erase(it);
        
        // 处理剩余的JOIN条件
        while (it != join_conds.end()) {
            std::shared_ptr<Plan> next_table = nullptr;
            
            // 找到下一个需要JOIN的表
            if (std::find(joined_tables.begin(), joined_tables.end(), it->lhs_col.tab_name) == joined_tables.end()) {
                next_table = pop_scan(scantbl, it->lhs_col.tab_name, joined_tables, table_scan_executors);
            } else if (std::find(joined_tables.begin(), joined_tables.end(), it->rhs_col.tab_name) == joined_tables.end()) {
                next_table = pop_scan(scantbl, it->rhs_col.tab_name, joined_tables, table_scan_executors);
                // 如果需要，调整条件的左右顺序
                    std::map<CompOp, CompOp> swap_op = {
                        {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
                    };
                    std::swap(it->lhs_col, it->rhs_col);
                    it->op = swap_op.at(it->op);
            }
            
            if (next_table != nullptr) {
                std::vector<Condition> next_join_cond{*it};
                result_plan = std::make_shared<JoinPlan>(T_NestLoop, std::move(next_table), std::move(result_plan), next_join_cond);
            }
            
            it = join_conds.erase(it);
        }
        
        // 连接剩余没有参与JOIN的表（笛卡尔积）
        for (size_t i = 0; i < tables.size(); i++) {
            if(scantbl[i] == -1) {
                result_plan = std::make_shared<JoinPlan>(T_NestLoop, std::move(result_plan), 
                                                        std::move(table_scan_executors[i]), std::vector<Condition>());
            }
        }
    } else {
        // 没有JOIN ON条件，但有多个表，需要从WHERE条件中提取连接条件
        result_plan = table_scan_executors[0];
        std::vector<std::string> joined_tables = {tables[0]};
        
        for (size_t i = 1; i < tables.size(); i++) {
            // 为当前要加入的表查找连接条件
            std::vector<Condition> current_join_conds;
            auto it = where_conds.begin();
            while (it != where_conds.end()) {
                if (!it->is_rhs_val) { // 只考虑列-列比较
                    bool left_in_joined = false, right_in_new = false;
                    
                    // 检查左列是否在已连接的表中
                    for (const auto& joined_table : joined_tables) {
                        if (it->lhs_col.tab_name == joined_table) {
                            left_in_joined = true;
                            break;
                        }
                    }
                    
                    // 检查右列是否是新加入的表
                    if (it->rhs_col.tab_name == tables[i]) {
                        right_in_new = true;
    }

                    // 如果条件连接已连接的表和新表，则是连接条件
                    if (left_in_joined && right_in_new) {
                        current_join_conds.push_back(*it);
                        it = where_conds.erase(it);
                        continue;
                    }
                    
                    // 反向检查：右列在已连接表中，左列是新表
                    bool right_in_joined = false, left_in_new = false;
                    for (const auto& joined_table : joined_tables) {
                        if (it->rhs_col.tab_name == joined_table) {
                            right_in_joined = true;
                            break;
                        }
                    }
                    if (it->lhs_col.tab_name == tables[i]) {
                        left_in_new = true;
                    }
                    
                    if (right_in_joined && left_in_new) {
                        // 需要交换左右列的顺序
                        std::map<CompOp, CompOp> swap_op = {
                            {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, 
                            {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
                        };
                        std::swap(it->lhs_col, it->rhs_col);
                        it->op = swap_op.at(it->op);
                        current_join_conds.push_back(*it);
                        it = where_conds.erase(it);
                        continue;
                    }
                }
                ++it;
            }
            
            // 创建连接（可能是空条件的笛卡尔积）
            result_plan = std::make_shared<JoinPlan>(T_NestLoop, std::move(result_plan), 
                                                    std::move(table_scan_executors[i]), current_join_conds);
            joined_tables.push_back(tables[i]);
        }
    }
    
    // 在JOIN之后添加WHERE过滤
    if (!where_conds.empty()) {
        result_plan = std::make_shared<FilterPlan>(T_Filter, std::move(result_plan), std::move(where_conds));
    }

    return result_plan;
}


std::shared_ptr<Plan> Planner::generate_sort_plan(std::shared_ptr<Query> query, std::shared_ptr<Plan> plan)
{
    auto x = std::dynamic_pointer_cast<ast::SelectStmt>(query->parse);
    if(!x->has_sort) {
        return plan;
    }
    std::vector<std::string> tables = query->tables;
    std::vector<ColMeta> all_cols;
    for (auto &sel_tab_name : tables) {
        // 这里db_不能写成get_db(), 注意要传指针
        const auto &sel_tab_cols = sm_manager_->db_.get_table(sel_tab_name).cols;
        all_cols.insert(all_cols.end(), sel_tab_cols.begin(), sel_tab_cols.end());
    }
    TabCol sel_col;
    for (auto &col : all_cols) {
        if(col.name.compare(x->order->cols->col_name) == 0 )
        sel_col = {.tab_name = col.tab_name, .col_name = col.name};
    }
    return std::make_shared<SortPlan>(T_Sort, std::move(plan), sel_col, 
                                    x->order->orderby_dir == ast::OrderBy_DESC);
}


/**
 * @brief select plan 生成
 *
 * @param sel_cols select plan 选取的列
 * @param tab_names select plan 目标的表
 * @param conds select plan 选取条件
 */
std::shared_ptr<Plan> Planner::generate_select_plan(std::shared_ptr<Query> query, Context *context) {
    // std::cout << "generate_select_plan" << std::endl;
    //逻辑优化
    query = logical_optimization(std::move(query), context);

    //物理优化
    auto sel_cols = query->cols;
    std::shared_ptr<Plan> plannerRoot = physical_optimization(query, context);
    plannerRoot = std::make_shared<ProjectionPlan>(T_Projection, std::move(plannerRoot), 
                                                        std::move(sel_cols));

    return plannerRoot;
}

// 生成DDL语句和DML语句的查询执行计划
std::shared_ptr<Plan> Planner::do_planner(std::shared_ptr<Query> query, Context *context)
{
    std::shared_ptr<Plan> plannerRoot;
    if (auto x = std::dynamic_pointer_cast<ast::CreateTable>(query->parse)) {
        // create table;
        std::vector<ColDef> col_defs;
        for (auto &field : x->fields) {
            if (auto sv_col_def = std::dynamic_pointer_cast<ast::ColDef>(field)) {
                ColDef col_def = {.name = sv_col_def->col_name,
                                  .type = interp_sv_type(sv_col_def->type_len->type),
                                  .len = sv_col_def->type_len->len};
                col_defs.push_back(col_def);
            } else {
                throw InternalError("Unexpected field type");
            }
        }
        plannerRoot = std::make_shared<DDLPlan>(T_CreateTable, x->tab_name, std::vector<std::string>(), col_defs);
    } else if (auto x = std::dynamic_pointer_cast<ast::DropTable>(query->parse)) {
        // drop table;
        plannerRoot = std::make_shared<DDLPlan>(T_DropTable, x->tab_name, std::vector<std::string>(), std::vector<ColDef>());
    } else if (auto x = std::dynamic_pointer_cast<ast::CreateIndex>(query->parse)) {
        // create index;
        plannerRoot = std::make_shared<DDLPlan>(T_CreateIndex, x->tab_name, x->col_names, std::vector<ColDef>());
    } else if (auto x = std::dynamic_pointer_cast<ast::DropIndex>(query->parse)) {
        // drop index
        plannerRoot = std::make_shared<DDLPlan>(T_DropIndex, x->tab_name, x->col_names, std::vector<ColDef>());
    } else if (auto x = std::dynamic_pointer_cast<ast::InsertStmt>(query->parse)) {
        // insert;
        plannerRoot = std::make_shared<DMLPlan>(T_Insert, std::shared_ptr<Plan>(),  x->tab_name,  
                                                    query->values, std::vector<Condition>(), std::vector<SetClause>());
    } else if (auto x = std::dynamic_pointer_cast<ast::DeleteStmt>(query->parse)) {
        // delete;
        // 生成表扫描方式
        std::shared_ptr<Plan> table_scan_executors;
        // 只有一张表，不需要进行物理优化了
        // int index_no = get_indexNo(x->tab_name, query->conds);
        std::vector<std::string> index_col_names;
        bool index_exist = get_index_cols(x->tab_name, query->conds, index_col_names);
        
        if (index_exist == false) {  // 该表没有索引
            index_col_names.clear();
            table_scan_executors = 
                std::make_shared<ScanPlan>(T_SeqScan, sm_manager_, x->tab_name, query->conds, index_col_names);
        } else {  // 存在索引
            table_scan_executors =
                std::make_shared<ScanPlan>(T_IndexScan, sm_manager_, x->tab_name, query->conds, index_col_names);
        }

        plannerRoot = std::make_shared<DMLPlan>(T_Delete, table_scan_executors, x->tab_name,  
                                                std::vector<Value>(), query->conds, std::vector<SetClause>());
    } else if (auto x = std::dynamic_pointer_cast<ast::UpdateStmt>(query->parse)) {
        // update;
        // 生成表扫描方式
        std::shared_ptr<Plan> table_scan_executors;
        // 只有一张表，不需要进行物理优化了
        // int index_no = get_indexNo(x->tab_name, query->conds);
        std::vector<std::string> index_col_names;
        bool index_exist = get_index_cols(x->tab_name, query->conds, index_col_names);
        // std::cout << "Add Scan planner" << std::endl;
        if (index_exist == false) {  // 该表没有索引
            index_col_names.clear();
            table_scan_executors = 
                std::make_shared<ScanPlan>(T_SeqScan, sm_manager_, x->tab_name, query->conds, index_col_names);
        } else {  // 存在索引
            table_scan_executors =
                std::make_shared<ScanPlan>(T_IndexScan, sm_manager_, x->tab_name, query->conds, index_col_names);
        }
        plannerRoot = std::make_shared<DMLPlan>(T_Update, table_scan_executors, x->tab_name,
                                                     std::vector<Value>(), query->conds, 
                                                     query->set_clauses);
    } else if (auto x = std::dynamic_pointer_cast<ast::SelectStmt>(query->parse)) {
        std::shared_ptr<plannerInfo> root = std::make_shared<plannerInfo>(x);
        // 生成select语句的查询执行计划
        std::shared_ptr<Plan> projection = generate_select_plan(std::move(query), context);
        plannerRoot = std::make_shared<DMLPlan>(T_select, projection, std::string(), std::vector<Value>(),
                                                    std::vector<Condition>(), std::vector<SetClause>());
    } else if (auto x = std::dynamic_pointer_cast<ast::ExplainStmt>(query->parse)) {
        // 处理EXPLAIN语句
        QueryOptimizer optimizer(sm_manager_, this);
        auto optimized_plan_tree = optimizer.optimize(query);
        
        // 创建EXPLAIN计划 - 只需要计划树用于显示
        plannerRoot = std::make_shared<ExplainPlan>(T_Explain, optimized_plan_tree);
    } else {
        throw InternalError("Unexpected AST root");
    }
    return plannerRoot;
}