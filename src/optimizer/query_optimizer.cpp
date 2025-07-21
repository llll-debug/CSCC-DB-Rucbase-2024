#include "query_optimizer.h"
#include <algorithm>
#include <set>
#include <sstream>
#include <regex>
#include <map>
#include <climits>
#include <iomanip>
#include <cstdio>
#include <cctype>
#include "record/rm_scan.h"
#include "plan.h"
#include "planner.h"

std::shared_ptr<PlanTreeNode> QueryOptimizer::optimize(std::shared_ptr<Query> query) {

    alias_to_table_map_ = query->alias_to_table;
    
    std::shared_ptr<PlanTreeNode> plan;
    
    if (query->tables.size() == 1) {

        plan = std::make_shared<ScanNode>(query->tables[0]);
        

        plan = applyPredicatePushdown(plan, query->conds);
    } else {

        std::vector<Condition> join_conditions;
        std::vector<Condition> filter_conditions;

        if (query->join_conds.empty()) {

            join_conditions = {};  
            filter_conditions = query->conds;  
        } else {
            join_conditions = query->join_conds;
            filter_conditions = query->conds;
        }
        
        plan = buildOptimalJoinOrder(query->tables, join_conditions);
        
        plan = applyPredicatePushdown(plan, filter_conditions);
    }
    
    if (!query->is_select_all) {
        plan = applyProjectionPushdown(plan, query->cols);
    }
    
    if (query->is_select_all) {
        plan = std::make_shared<ProjectNode>(plan, std::vector<std::string>(), true);
    } else {
        std::vector<std::string> user_ordered_columns;
        
        for (const auto& col : query->cols) {
            std::string full_col_name;
            
            std::string prefix = !col.alias.empty() ? col.alias : col.tab_name;
            
            if (!prefix.empty()) {
                full_col_name = prefix + "." + col.col_name;
            } else {
                full_col_name = col.col_name;
                for (const auto& table : query->tables) {
                    try {
                        auto& table_meta = sm_manager_->db_.get_table(table);
                        if (table_meta.is_col(col.col_name)) {
                            full_col_name = table + "." + col.col_name;
                            break;
                        }
                    } catch (...) {
                        continue;
                    }
                }
            }
            user_ordered_columns.push_back(full_col_name);
        }
        
        plan = std::make_shared<ProjectNode>(plan, user_ordered_columns, false);
    }
    
    return plan;
}

std::shared_ptr<PlanTreeNode> QueryOptimizer::optimizeJoinOrder(std::shared_ptr<Query> query) {
    return buildOptimalJoinOrder(query->tables, query->join_conds);
}

std::shared_ptr<PlanTreeNode> QueryOptimizer::buildOptimalJoinOrder(
    const std::vector<std::string>& tables, const std::vector<Condition>& conditions) {
    
    if (tables.empty()) return nullptr;
    if (tables.size() == 1) {
        return std::make_shared<ScanNode>(tables[0]);
    }
    
    std::map<std::string, size_t> table_cardinalities;
    for (const auto& table : tables) {
        table_cardinalities[table] = getTableCardinality(table);
    }
    
    std::vector<std::string> sorted_tables = tables;
    std::sort(sorted_tables.begin(), sorted_tables.end(),
              [&](const std::string& a, const std::string& b) {
                  return table_cardinalities[a] < table_cardinalities[b];
              });
    
    std::shared_ptr<PlanTreeNode> result = std::make_shared<ScanNode>(sorted_tables[0]);
    
    size_t i = 1;
    while (i < sorted_tables.size()) {
        std::string next_table = sorted_tables[i];
        auto next_scan = std::make_shared<ScanNode>(next_table);
        
        auto current_tables = result->getOutputTables();
        std::vector<std::string> next_tables = {next_table};
        auto join_conditions = extractJoinConditions(conditions, current_tables, next_tables);
        
        if (join_conditions.empty() && i < sorted_tables.size() - 1) {
            bool found_connectable = false;
            size_t j = i + 1;
            while (j < sorted_tables.size()) {
                std::string candidate_table = sorted_tables[j];
                std::vector<std::string> candidate_tables = {candidate_table};
                auto candidate_conditions = extractJoinConditions(conditions, current_tables, candidate_tables);
                
                if (!candidate_conditions.empty()) {
                    std::swap(sorted_tables[i], sorted_tables[j]);
                    next_table = sorted_tables[i];
                    next_scan = std::make_shared<ScanNode>(next_table);
                    next_tables = {next_table};
                    join_conditions = candidate_conditions;
                    found_connectable = true;
                    break;
                }
                ++j;
            }
        }
        
        result = std::make_shared<JoinNode>(result, next_scan, join_conditions);
        ++i;
    }
    
    return result;
}

size_t QueryOptimizer::estimateJoinResultSize(std::shared_ptr<PlanTreeNode> left, const std::string& right_table) {
    size_t left_size = 1;
    auto left_tables = left->getOutputTables();
    for (const auto& table : left_tables) {
        left_size *= getTableCardinality(table);
    }
    
    size_t right_size = getTableCardinality(right_table);
    
    double selectivity = 0.1; 
    try {
        size_t max_size = std::max(left_size, right_size);
        if (max_size > 0) {
            selectivity = std::min(0.5, 100.0 / max_size); // 自适应选择性
        }
    } catch (...) {
        selectivity = 0.1; // 回退到默认值
    }
    

    size_t estimated_size = static_cast<size_t>((left_size * right_size) * selectivity);
    
    return std::max(estimated_size, std::max(left_size, right_size));
}

std::shared_ptr<PlanTreeNode> QueryOptimizer::applyPredicatePushdown(
    std::shared_ptr<PlanTreeNode> root, const std::vector<Condition>& conditions) {
    
    std::vector<std::string> condition_strings;
    for (const auto& cond : conditions) {
        condition_strings.push_back(conditionToString(cond));
    }
    
    auto result = pushPredicatesDown(root, condition_strings);
    
    if (!condition_strings.empty()) {
        result = std::make_shared<FilterNode>(result, condition_strings);
    }
    
    return result;
}

std::shared_ptr<PlanTreeNode> QueryOptimizer::pushPredicatesDown(
    std::shared_ptr<PlanTreeNode> node, std::vector<std::string>& remaining_conditions) {
    
    if (auto join = std::dynamic_pointer_cast<JoinNode>(node)) {
        auto left_tables = join->getLeft()->getOutputTables();
        auto right_tables = join->getRight()->getOutputTables();

        std::vector<std::string> left_conditions, right_conditions;
        auto it = remaining_conditions.begin();
        while (it != remaining_conditions.end()) {
            auto condition_tables = getTablesInCondition(*it);
            
            bool left_only = true;
            bool right_only = true;
            
            for (const auto& cond_table : condition_tables) {
                bool in_left = false, in_right = false;
                
                for (const auto& left_tab : left_tables) {
                    if (cond_table == left_tab || 
                        (alias_to_table_map_.find(cond_table) != alias_to_table_map_.end() && alias_to_table_map_.at(cond_table) == left_tab) ||
                        (cond_table.length() == 1 && left_tab.substr(0, 1) == cond_table)) {
                        in_left = true;
                        break;
                    }
                }
                
                for (const auto& right_tab : right_tables) {
                    if (cond_table == right_tab || 
                        (alias_to_table_map_.find(cond_table) != alias_to_table_map_.end() && alias_to_table_map_.at(cond_table) == right_tab) ||
                        (cond_table.length() == 1 && right_tab.substr(0, 1) == cond_table)) {
                        in_right = true;
                        break;
                    }
                }
                
                if (!in_left) left_only = false;
                if (!in_right) right_only = false;
            }
            
            if (left_only && !condition_tables.empty()) {
                left_conditions.push_back(*it);
                it = remaining_conditions.erase(it);
            } else if (right_only && !condition_tables.empty()) {
                right_conditions.push_back(*it);
                it = remaining_conditions.erase(it);
            } else {
                ++it;
            }
        }
        
        auto left = pushPredicatesDown(join->getLeft(), left_conditions);
        auto right = pushPredicatesDown(join->getRight(), right_conditions);
        
        remaining_conditions.insert(remaining_conditions.end(), left_conditions.begin(), left_conditions.end());
        remaining_conditions.insert(remaining_conditions.end(), right_conditions.begin(), right_conditions.end());
        
        auto new_join = std::make_shared<JoinNode>(left, right, join->getConditions());
        
        return new_join;
    } else if (auto scan = std::dynamic_pointer_cast<ScanNode>(node)) {
        std::vector<std::string> applicable_conditions;
        auto node_tables = scan->getOutputTables();
        
        auto it = remaining_conditions.begin();
        while (it != remaining_conditions.end()) {
            if (conditionAppliesTo(*it, node_tables)) {
                applicable_conditions.push_back(*it);
                it = remaining_conditions.erase(it);
            } else {
                ++it;
            }
        }
        
        if (!applicable_conditions.empty()) {
            return std::make_shared<FilterNode>(scan, applicable_conditions);
        }
        return scan;
    }
    
    return node;
}

void QueryOptimizer::collectColumnsFromNode(std::shared_ptr<PlanTreeNode> node, std::set<std::string>& required_cols) {
    if (auto project = std::dynamic_pointer_cast<ProjectNode>(node)) {

        collectColumnsFromNode(project->getChild(), required_cols);
    } else if (auto filter = std::dynamic_pointer_cast<FilterNode>(node)) {

        for (const auto& condition : filter->getConditions()) {
            std::regex col_regex(R"((\w+)\.(\w+))");
            std::sregex_iterator iter(condition.begin(), condition.end(), col_regex);
            std::sregex_iterator end;
            
            while (iter != end) {
                std::string prefix = iter->str(1);     // 表名或别名
                std::string col_name = iter->str(2);   // 列名
                required_cols.insert(prefix + "." + col_name);
                ++iter;
            }
        }
        

        collectColumnsFromNode(filter->getChild(), required_cols);
    } else if (auto join = std::dynamic_pointer_cast<JoinNode>(node)) {

        for (const auto& condition : join->getConditions()) {
            std::regex col_regex(R"((\w+)\.(\w+))");
            std::sregex_iterator iter(condition.begin(), condition.end(), col_regex);
            std::sregex_iterator end;
            
            while (iter != end) {
                std::string prefix = iter->str(1);     // 表名或别名
                std::string col_name = iter->str(2);   // 列名
                required_cols.insert(prefix + "." + col_name);
                ++iter;
            }
        }
        

        collectColumnsFromNode(join->getLeft(), required_cols);
        collectColumnsFromNode(join->getRight(), required_cols);
    } else if (auto scan = std::dynamic_pointer_cast<ScanNode>(node)) {

        return;
    }
}

std::shared_ptr<PlanTreeNode> QueryOptimizer::applyProjectionPushdown(
    std::shared_ptr<PlanTreeNode> root, const std::vector<TabCol>& required_cols) {
    

    if (required_cols.empty()) {
        return root;
    }

    std::set<std::string> all_required_cols;
    
    for (const auto& col : required_cols) {
        std::string full_col_name;
        
        std::string prefix = !col.alias.empty() ? col.alias : col.tab_name;
        
        if (!prefix.empty()) {
            full_col_name = prefix + "." + col.col_name;
        } else {
            full_col_name = col.col_name;
        }
        all_required_cols.insert(full_col_name);
    }
    return pushProjectionsDown(root, all_required_cols, true);  // 添加is_root参数
}

std::shared_ptr<PlanTreeNode> QueryOptimizer::pushProjectionsDown(
    std::shared_ptr<PlanTreeNode> node, const std::set<std::string>& required_cols, bool is_root) {
    
    if (auto join = std::dynamic_pointer_cast<JoinNode>(node)) {
        auto left_tables = join->getLeft()->getOutputTables();
        auto right_tables = join->getRight()->getOutputTables();
        
        std::set<std::string> left_required, right_required;
        
        for (const auto& col : required_cols) {
            if (belongsToTables(col, left_tables)) {
                left_required.insert(col);
            } else if (belongsToTables(col, right_tables)) {
                right_required.insert(col);
            }
        }
        
        for (const auto& condition : join->getConditions()) {
            std::regex col_regex(R"((\w+)\.(\w+))");
            std::sregex_iterator iter(condition.begin(), condition.end(), col_regex);
            std::sregex_iterator end;
            
            while (iter != end) {
                std::string full_col = iter->str();
                if (belongsToTables(full_col, left_tables)) {
                    left_required.insert(full_col);
                } else if (belongsToTables(full_col, right_tables)) {
                    right_required.insert(full_col);
                }
                ++iter;
            }
        }
        
        auto left = pushProjectionsDown(join->getLeft(), left_required, false);
        auto right = pushProjectionsDown(join->getRight(), right_required, false);

        if (auto left_scan = std::dynamic_pointer_cast<ScanNode>(left)) {
            std::vector<std::string> left_cols;
            std::string left_table = left_scan->getTableName();
            
            for (const auto& col : left_required) {
                if (belongsToSingleTable(col, left_table)) {
                    left_cols.push_back(col);
                }
            }
            
            size_t total_left_cols = getAllColumnsCount(left_table);
            if (!left_cols.empty() && left_cols.size() < total_left_cols) {
                left = std::make_shared<ProjectNode>(left, left_cols);
            }
        }
        
        if (auto right_scan = std::dynamic_pointer_cast<ScanNode>(right)) {
            std::vector<std::string> right_cols;
            std::string right_table = right_scan->getTableName();
            
            for (const auto& col : right_required) {
                if (belongsToSingleTable(col, right_table)) {
                    right_cols.push_back(col);
                }
            }
            
            size_t total_right_cols = getAllColumnsCount(right_table);
            if (!right_cols.empty() && right_cols.size() < total_right_cols) {
                right = std::make_shared<ProjectNode>(right, right_cols);
            }
        }
        
        return std::make_shared<JoinNode>(left, right, join->getConditions());
    } else if (auto scan = std::dynamic_pointer_cast<ScanNode>(node)) {
        if (!is_root) {  // 只有在非根节点时才考虑添加投影优化
            std::vector<std::string> scan_cols;
            std::string table_name = scan->getTableName();
            
            for (const auto& col : required_cols) {
                if (belongsToSingleTable(col, table_name)) {
                    scan_cols.push_back(col);
                }
            }
            
            size_t total_cols = getAllColumnsCount(table_name);
            // 修改投影下推条件：只要需要的列少于总列数且大于0就进行优化
            if (!scan_cols.empty() && scan_cols.size() < total_cols) {
                return std::make_shared<ProjectNode>(scan, scan_cols);
            }
        }
        
        return scan;
    } else if (auto filter = std::dynamic_pointer_cast<FilterNode>(node)) {
        auto filter_child = filter->getChild();
        
        if (auto scan_child = std::dynamic_pointer_cast<ScanNode>(filter_child)) {
            auto new_filter = std::make_shared<FilterNode>(scan_child, filter->getConditions());
            
            if (!is_root) {
                std::vector<std::string> output_cols;
                auto output_tables = new_filter->getOutputTables();
                
                for (const auto& col : required_cols) {
                    if (belongsToTables(col, output_tables)) {
                        output_cols.push_back(col);
                    }
                }
                
                std::string table_name = scan_child->getTableName();
                size_t total_cols = getAllColumnsCount(table_name);

                if (!output_cols.empty() && output_cols.size() < total_cols) {
                    return std::make_shared<ProjectNode>(new_filter, output_cols);
                }
            }
            
            return new_filter;
        } else {
            std::set<std::string> child_needed_cols = required_cols;
            
            for (const auto& condition : filter->getConditions()) {
                std::regex col_regex(R"((\w+)\.(\w+))");
                std::sregex_iterator iter(condition.begin(), condition.end(), col_regex);
                std::sregex_iterator end;
                
                while (iter != end) {
                    std::string full_col = iter->str();
                    child_needed_cols.insert(full_col);
                    ++iter;
                }
            }
            
            auto child = pushProjectionsDown(filter->getChild(), child_needed_cols, false);
            auto new_filter = std::make_shared<FilterNode>(child, filter->getConditions());
            
            std::vector<std::string> output_cols;
            auto output_tables = new_filter->getOutputTables();
            
            for (const auto& col : required_cols) {
                if (belongsToTables(col, output_tables)) {
                    output_cols.push_back(col);
                }
            }
            
            if (!output_cols.empty() && output_cols.size() < child_needed_cols.size() && 
                output_cols.size() <= child_needed_cols.size() / 2 && !is_root) {  // 添加!is_root检查
                return std::make_shared<ProjectNode>(new_filter, output_cols);
            }
            
            return new_filter;
        }
    } else if (auto project = std::dynamic_pointer_cast<ProjectNode>(node)) {
        return project;
    }
    
    return node;
}


size_t QueryOptimizer::getTableCardinality(const std::string& table_name) {
    try {
        auto& table_meta = sm_manager_->db_.get_table(table_name);
        auto fh = sm_manager_->fhs_.at(table_name).get();
        
        size_t record_count = 0;
        for (RmScan scan(fh); !scan.is_end(); scan.next()) {
            record_count++;
        }
        
        return std::max(record_count, static_cast<size_t>(1));
    } catch (...) {
        return 1000; // 默认中等大小
    }
}

std::vector<std::string> QueryOptimizer::extractJoinConditions(
    const std::vector<Condition>& conditions,
    const std::vector<std::string>& left_tables,
    const std::vector<std::string>& right_tables) {
    
    std::vector<std::string> join_conditions;
    
    for (const auto& cond : conditions) {
        if (!cond.is_rhs_val) { // 只有列-列比较才可能是连接条件
            bool left_has_table = false, right_has_table = false;
            
            // 检查左操作数
            for (const auto& table : left_tables) {
                if (cond.lhs_col.tab_name == table) {
                    left_has_table = true;
                    break;
                }
            }
            
            // 检查右操作数
            for (const auto& table : right_tables) {
                if (cond.rhs_col.tab_name == table) {
                    right_has_table = true;
                    break;
                }
            }
            if (!left_has_table && !right_has_table) {
                for (const auto& table : left_tables) {
                    if (cond.rhs_col.tab_name == table) {
                        left_has_table = true;
                        break;
                    }
                }
                for (const auto& table : right_tables) {
                    if (cond.lhs_col.tab_name == table) {
                        right_has_table = true;
                        break;
                    }
                }
            }
            if (left_has_table && right_has_table) {
                join_conditions.push_back(conditionToString(cond));
            }
        }
    }
    
    return join_conditions;
}

size_t QueryOptimizer::getAllColumnsCount(const std::string& table_name) {
    try {
        auto& table_meta = sm_manager_->db_.get_table(table_name);
        return table_meta.cols.size();
    } catch (...) {
        return 0;
    }
}

std::string QueryOptimizer::conditionToString(const Condition& cond) {
    std::string result;
    
    std::string left_prefix = !cond.lhs_col.alias.empty() ? cond.lhs_col.alias : cond.lhs_col.tab_name;
    if (!left_prefix.empty()) {
        result += left_prefix + ".";
    }
    result += cond.lhs_col.col_name;

    switch (cond.op) {
        case OP_GE: result += ">="; break;
        case OP_LE: result += "<="; break;
        case OP_GT: result += ">"; break;
        case OP_LT: result += "<"; break;
        case OP_NE: result += "<>"; break;  // 题目要求使用<>而不是!=
        case OP_EQ: result += "="; break;
    }

    if (cond.is_rhs_val) {

        switch (cond.rhs_val.type) {
            case TYPE_STRING:
                result += "'" + cond.rhs_val.str_val + "'";  // 保持与原始SQL一致，添加引号
                break;
            case TYPE_FLOAT: {
                float val = cond.rhs_val.float_val;
                char buffer[32];
                
                if (val == static_cast<int>(val)) {
                    snprintf(buffer, sizeof(buffer), "%.1f", val);
                } else {
                    snprintf(buffer, sizeof(buffer), "%.6g", val);
                }
                result += buffer;
                break;
            }
            case TYPE_INT:
                result += std::to_string(cond.rhs_val.int_val);
                break;
        }
    } else {
        std::string right_prefix = !cond.rhs_col.alias.empty() ? cond.rhs_col.alias : cond.rhs_col.tab_name;
        if (!right_prefix.empty()) {
            result += right_prefix + ".";
        }
        result += cond.rhs_col.col_name;
    }
    
    return result;
}

bool QueryOptimizer::conditionAppliesTo(const std::string& condition, 
                                        const std::vector<std::string>& tables) {
    std::set<std::string> condition_tables = getTablesInCondition(condition);
    
    if (condition_tables.count("__ANY_TABLE__")) {
        std::regex col_regex(R"(^(\w+)[<>=!]+)");
        std::smatch match;
        if (std::regex_search(condition, match, col_regex)) {
            std::string col_name = match.str(1);
            for (const auto& table : tables) {
                try {
                    auto& table_meta = sm_manager_->db_.get_table(table);
                    if (table_meta.is_col(col_name)) {
                        return true;
                    }
                } catch (...) {
                    continue;
                }
            }
        }
        return false;
    }

    for (const auto& cond_table : condition_tables) {
        bool found = false;
        for (const auto& table : tables) {
            if (cond_table == table) {
                found = true;
                break;
            }
            if (alias_to_table_map_.find(cond_table) != alias_to_table_map_.end() &&
                alias_to_table_map_.at(cond_table) == table) {
                found = true;
                break;
            }
            if (cond_table.length() == 1 && table.substr(0, 1) == cond_table) {
                found = true;
                break;
            }
        }
        if (!found) {
            return false; 
        }
    }

    return !condition_tables.empty();
}

std::set<std::string> QueryOptimizer::getTablesInCondition(const std::string& condition) {
    std::set<std::string> tables;

    size_t pos = 0;
    while (pos < condition.length()) {

        size_t dot_pos = condition.find('.', pos);
        if (dot_pos == std::string::npos) break;
        
        size_t start = dot_pos;
        while (start > 0 && (std::isalnum(condition[start-1]) || condition[start-1] == '_')) {
            start--;
        }
        
        size_t end = dot_pos + 1;
        while (end < condition.length() && (std::isalnum(condition[end]) || condition[end] == '_')) {
            end++;
        }
        
        if (start < dot_pos && end > dot_pos + 1) {
            std::string prefix = condition.substr(start, dot_pos - start);
            std::string suffix = condition.substr(dot_pos + 1, end - dot_pos - 1);
            
            bool prefix_is_table = false;
            if (!prefix.empty() && !std::isdigit(prefix[0])) {
                bool all_digits = true;
                for (char c : prefix) {
                    if (!std::isdigit(c)) {
                        all_digits = false;
                        break;
                    }
                }
                if (!all_digits) {
                    prefix_is_table = true;
                }
            }
            
            bool suffix_is_column = false;
            if (!suffix.empty() && !std::isdigit(suffix[0])) {
                bool all_digits = true;
                for (char c : suffix) {
                    if (!std::isdigit(c)) {
                        all_digits = false;
                        break;
                    }
                }
                if (!all_digits) {
                    suffix_is_column = true;
                }
            }
            
            if (prefix_is_table && suffix_is_column) {
                tables.insert(prefix);
            }
        }
        
        pos = dot_pos + 1;
    }
    
    if (tables.empty()) {
        std::regex col_regex(R"(^(\w+)[<>=!]+)");
        std::smatch match;
        if (std::regex_search(condition, match, col_regex)) {
            tables.insert("__ANY_TABLE__");
        }
    }
    
    return tables;
}

std::shared_ptr<Plan> QueryOptimizer::convertToExecutionPlan(std::shared_ptr<PlanTreeNode> plan_tree, 
                                                             std::shared_ptr<Query> query) {
    // 合并所有条件用于查找
    std::vector<Condition> all_conditions;
    all_conditions.insert(all_conditions.end(), query->conds.begin(), query->conds.end());
    all_conditions.insert(all_conditions.end(), query->join_conds.begin(), query->join_conds.end());
    
    return convertPlanTreeNodeToPlan(plan_tree, all_conditions);
}

std::shared_ptr<Plan> QueryOptimizer::convertPlanTreeNodeToPlan(std::shared_ptr<PlanTreeNode> node,
                                                                const std::vector<Condition>& all_conditions) {
    if (auto project_node = std::dynamic_pointer_cast<ProjectNode>(node)) {
        // 转换Project节点
        auto child_plan = convertPlanTreeNodeToPlan(project_node->getChild(), all_conditions);
        
        // 转换列名格式
        std::vector<TabCol> sel_cols;
        if (project_node->isSelectAll()) {
            // SELECT *的情况，获取所有表的所有列
            auto tables = project_node->getOutputTables();
            for (const auto& table : tables) {
                try {
                    auto& table_meta = sm_manager_->db_.get_table(table);
                    for (const auto& col : table_meta.cols) {
                        sel_cols.push_back({table, col.name, ""});
                    }
                } catch (...) {
                    continue;
                }
            }
        } else {
            // 解析具体的列名
            for (const auto& col_str : project_node->getColumns()) {
                size_t dot_pos = col_str.find('.');
                if (dot_pos != std::string::npos) {
                    std::string table_name = col_str.substr(0, dot_pos);
                    std::string col_name = col_str.substr(dot_pos + 1);
                    sel_cols.push_back({table_name, col_name, ""});
                } else {
                    sel_cols.push_back({"", col_str, ""});
                }
            }
        }
        
        return std::make_shared<ProjectionPlan>(T_Projection, child_plan, sel_cols);
    } else if (auto join_node = std::dynamic_pointer_cast<JoinNode>(node)) {
        // 转换Join节点
        auto left_plan = convertPlanTreeNodeToPlan(join_node->getLeft(), all_conditions);
        auto right_plan = convertPlanTreeNodeToPlan(join_node->getRight(), all_conditions);
        auto join_conditions = parseConditionStrings(join_node->getConditions(), all_conditions);
        
        return std::make_shared<JoinPlan>(T_NestLoop, left_plan, right_plan, join_conditions);
    } else if (auto filter_node = std::dynamic_pointer_cast<FilterNode>(node)) {
        // 转换Filter节点
        auto child_plan = convertPlanTreeNodeToPlan(filter_node->getChild(), all_conditions);
        auto filter_conditions = parseConditionStrings(filter_node->getConditions(), all_conditions);
        
        return std::make_shared<FilterPlan>(T_Filter, child_plan, filter_conditions);
    } else if (auto scan_node = std::dynamic_pointer_cast<ScanNode>(node)) {
        // 转换Scan节点
        std::string table_name = scan_node->getTableName();
        std::vector<Condition> table_conditions = findConditionsForTables(all_conditions, {table_name});
        
        // 检查是否有索引可用
        std::vector<std::string> index_col_names;
        bool index_exist = false;
        
        if (planner_) {
            auto temp_conditions = table_conditions;  
            index_exist = planner_->get_index_cols(const_cast<std::string&>(table_name), temp_conditions, index_col_names);
        } else {
            for (const auto& cond : table_conditions) {
                if (cond.is_rhs_val && cond.op == OP_EQ) {
                    try {
                        auto& table_meta = sm_manager_->db_.get_table(table_name);
                        for (const auto& [index_name, index] : table_meta.indexes) {
                            if (!index.cols.empty() && index.cols[0].name == cond.lhs_col.col_name) {
                                index_col_names.push_back(cond.lhs_col.col_name);
                                index_exist = true;
                                break;
                            }
                        }
                        if (index_exist) break;
                    } catch (...) {
                        continue;
                    }
                }
            }
        }
        
        if (index_exist) {
            return std::make_shared<ScanPlan>(T_IndexScan, sm_manager_, table_name, 
                                            table_conditions, index_col_names);
        } else {
            return std::make_shared<ScanPlan>(T_SeqScan, sm_manager_, table_name, 
                                            table_conditions, std::vector<std::string>());
        }
    }
    
    return nullptr;
}

std::vector<Condition> QueryOptimizer::findConditionsForTables(const std::vector<Condition>& all_conditions,
                                                               const std::vector<std::string>& tables) {
    std::vector<Condition> result;
    
    for (const auto& cond : all_conditions) {
        bool applies_to_tables = false;
        
        for (const auto& table : tables) {
            if (cond.lhs_col.tab_name == table && cond.is_rhs_val) {
                applies_to_tables = true;
                break;
            } else if (!cond.is_rhs_val && 
                       cond.lhs_col.tab_name == table && cond.rhs_col.tab_name == table) {
                applies_to_tables = true;
                break;
            }
        }
        
        if (applies_to_tables) {
            result.push_back(cond);
        }
    }
    
    return result;
}

std::vector<Condition> QueryOptimizer::parseConditionStrings(const std::vector<std::string>& condition_strings,
                                                            const std::vector<Condition>& all_conditions) {
    std::vector<Condition> result;
    
    for (const auto& cond_str : condition_strings) {
        bool found = false;
        
        for (const auto& cond : all_conditions) {
            if (conditionToString(cond) == cond_str) {
                result.push_back(cond);
                found = true;
                break;
            }
        }
        
        if (!found) {
            std::regex cond_regex(R"((?:(\w+)\.)?(\w+)([<>=!]+)(.+))");
            std::smatch match;
            if (std::regex_match(cond_str, match, cond_regex)) {
                std::string table_prefix = match.str(1);  // 可能为空
                std::string col_name = match.str(2);
                std::string op_str = match.str(3);
                std::string value_str = match.str(4);
                
                // 转换操作符字符串为CompOp
                CompOp target_op;
                if (op_str == ">=") target_op = OP_GE;
                else if (op_str == "<=") target_op = OP_LE;
                else if (op_str == ">") target_op = OP_GT;
                else if (op_str == "<") target_op = OP_LT;
                else if (op_str == "<>") target_op = OP_NE;
                else if (op_str == "=") target_op = OP_EQ;
                else continue;  // 不支持的操作符
                
                for (const auto& cond : all_conditions) {
                    // 匹配列名、操作符和值
                    if (cond.lhs_col.col_name == col_name && cond.op == target_op && cond.is_rhs_val) {
                        // 如果条件字符串有表前缀，检查表名是否匹配
                        bool table_matches = true;
                        if (!table_prefix.empty()) {
                            table_matches = (cond.lhs_col.tab_name == table_prefix || 
                                           (!cond.lhs_col.alias.empty() && cond.lhs_col.alias == table_prefix) ||
                                           (table_prefix.length() == 1 && cond.lhs_col.tab_name.substr(0, 1) == table_prefix));
                        }
                        
                        if (table_matches) {
                            // 检查值是否匹配
                            bool value_matches = false;
                            switch (cond.rhs_val.type) {
                                case TYPE_STRING: {
                                    value_matches = (cond.rhs_val.str_val == value_str);
                                    break;
                                }
                                case TYPE_FLOAT: {
                                    try {
                                        float parsed_val = std::stof(value_str);
                                        // 使用小的容差比较浮点数
                                        value_matches = (std::abs(cond.rhs_val.float_val - parsed_val) < 1e-6);
                                    } catch (...) {
                                        value_matches = false;
                                    }
                                    break;
                                }
                                case TYPE_INT: {
                                    try {
                                        int parsed_val = std::stoi(value_str);
                                        value_matches = (cond.rhs_val.int_val == parsed_val);
                                    } catch (...) {
                                        value_matches = false;
                                    }
                                    break;
                                }
                            }
                            
                            if (value_matches) {
                                result.push_back(cond);
                                found = true;
                                break;
                            }
                        }
                    }
                }
                
                if (found) break;
            }
        }
    }
    
    return result;
}

size_t QueryOptimizer::getTableRowCount(const std::string& table_name) {
    return getTableCardinality(table_name);  
}

double QueryOptimizer::getJoinSelectivity(const Condition& condition) {

    if (condition.op == OP_EQ) {
        // 等值连接的选择性
        return 0.1;  // 10%
    } else if (condition.op == OP_NE) {
        // 不等条件的选择性
        return 0.9;   // 90%
    } else if (condition.op == OP_LT || condition.op == OP_GT || 
               condition.op == OP_LE || condition.op == OP_GE) {
        // 范围条件的选择性
        return 0.33;  // 33%
    } else {
        return 0.1;   // 默认
    }
}

bool QueryOptimizer::belongsToTables(const std::string& column, const std::vector<std::string>& tables) {
    
    if (column.find('.') != std::string::npos) {
        size_t dot_pos = column.find('.');
        std::string prefix = column.substr(0, dot_pos);
        std::string col_name = column.substr(dot_pos + 1);
        
        for (const auto& table : tables) {
            if (prefix == table) {
                try {
                    auto& table_meta = sm_manager_->db_.get_table(table);
                    return table_meta.is_col(col_name);
                } catch (...) {
                    continue;
                }
            }
        
            if (alias_to_table_map_.find(prefix) != alias_to_table_map_.end() &&
                alias_to_table_map_.at(prefix) == table) {
                try {
                    auto& table_meta = sm_manager_->db_.get_table(table);
                    return table_meta.is_col(col_name);
                } catch (...) {
                    continue;
                }
            }
        }
    } else {
        for (const auto& table : tables) {
            try {
                auto& table_meta = sm_manager_->db_.get_table(table);
                if (table_meta.is_col(column)) {
                    return true;
                }
            } catch (...) {
                continue;
            }
        }
    }
    
    return false;
}

bool QueryOptimizer::belongsToSingleTable(const std::string& column, const std::string& table_name) {
    
    if (column.find('.') != std::string::npos) {
        size_t dot_pos = column.find('.');
        std::string prefix = column.substr(0, dot_pos);
        std::string col_name = column.substr(dot_pos + 1);
        
        if (prefix == table_name) {
            try {
                auto& table_meta = sm_manager_->db_.get_table(table_name);
                return table_meta.is_col(col_name);
            } catch (...) {
                return false;
            }
        }
        
        if (alias_to_table_map_.find(prefix) != alias_to_table_map_.end() &&
            alias_to_table_map_.at(prefix) == table_name) {
            try {
                auto& table_meta = sm_manager_->db_.get_table(table_name);
                return table_meta.is_col(col_name);
            } catch (...) {
                return false;
            }
        }
    } else {
        try {
            auto& table_meta = sm_manager_->db_.get_table(table_name);
            return table_meta.is_col(column);
        } catch (...) {
            return false;
        }
    }
    
    return false;
}
