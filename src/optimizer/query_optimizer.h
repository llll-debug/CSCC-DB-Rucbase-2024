#pragma once

#include "plan_tree.h"
#include "plan.h"
#include "system/sm.h"
#include "analyze/analyze.h"
#include "common/common.h"
#include <memory>
#include <vector>
#include <map>
#include <set>
#include <algorithm>

// 前向声明
class Planner;

class QueryOptimizer {
private:
    SmManager* sm_manager_;
    Planner* planner_;
    std::map<std::string, std::string> alias_to_table_map_;  // 别名到表名的映射
    
public:
    QueryOptimizer(SmManager* sm_manager, Planner* planner = nullptr) 
        : sm_manager_(sm_manager), planner_(planner) {}
    
    // 主优化入口
    std::shared_ptr<PlanTreeNode> optimize(std::shared_ptr<Query> query);
    
    // 新增：PlanTreeNode到执行计划的转换
    std::shared_ptr<Plan> convertToExecutionPlan(std::shared_ptr<PlanTreeNode> plan_tree, 
                                                  std::shared_ptr<Query> query);
    
private:
    // 构建初始查询计划树
    std::shared_ptr<PlanTreeNode> buildInitialPlan(std::shared_ptr<Query> query);
    
    // 三大优化策略
    std::shared_ptr<PlanTreeNode> applyPredicatePushdown(std::shared_ptr<PlanTreeNode> root, 
                                                         const std::vector<Condition>& conditions);
    std::shared_ptr<PlanTreeNode> applyProjectionPushdown(std::shared_ptr<PlanTreeNode> root, 
                                                          const std::vector<TabCol>& required_cols);
    std::shared_ptr<PlanTreeNode> optimizeJoinOrder(std::shared_ptr<Query> query);
    
    // 辅助方法 - 统计信息
    size_t getTableCardinality(const std::string& table_name);
    std::vector<std::string> getTablesFromNode(std::shared_ptr<PlanTreeNode> node);
    size_t getTableRowCount(const std::string& table_name);  // 新增
    double getJoinSelectivity(const Condition& condition);   // 新增
    
    // 辅助方法 - 条件处理
    std::vector<std::string> extractJoinConditions(const std::vector<Condition>& conditions,
                                                   const std::vector<std::string>& left_tables,
                                                   const std::vector<std::string>& right_tables);
    std::vector<std::string> extractFilterConditions(const std::vector<Condition>& conditions,
                                                     const std::vector<std::string>& available_tables);
    std::string conditionToString(const Condition& cond);
    
    // 转换相关的辅助方法
    std::shared_ptr<Plan> convertPlanTreeNodeToPlan(std::shared_ptr<PlanTreeNode> node,
                                                    const std::vector<Condition>& all_conditions);
    std::vector<Condition> findConditionsForTables(const std::vector<Condition>& all_conditions,
                                                   const std::vector<std::string>& tables);
    std::vector<Condition> parseConditionStrings(const std::vector<std::string>& condition_strings,
                                                 const std::vector<Condition>& all_conditions);
    
    // 谓词下推相关
    std::shared_ptr<PlanTreeNode> pushPredicatesDown(std::shared_ptr<PlanTreeNode> node, 
                                                     std::vector<std::string>& remaining_conditions);
    bool conditionAppliesTo(const std::string& condition, const std::vector<std::string>& tables);
    std::set<std::string> getTablesInCondition(const std::string& condition);
    
    // 投影下推相关
    std::shared_ptr<PlanTreeNode> pushProjectionsDown(std::shared_ptr<PlanTreeNode> node, 
                                                       const std::set<std::string>& required_cols,
                                                       bool is_root = false);
    std::set<std::string> getRequiredColumnsForNode(std::shared_ptr<PlanTreeNode> node,
                                                    const std::set<std::string>& parent_required);
    size_t getAllColumnsCount(const std::string& table_name);
    void collectColumnsFromNode(std::shared_ptr<PlanTreeNode> node, std::set<std::string>& required_cols);
    bool belongsToTables(const std::string& column, const std::vector<std::string>& tables);
    bool belongsToSingleTable(const std::string& column, const std::string& table_name);
    
    // 连接顺序优化相关
    struct TableInfo {
        std::string name;
        size_t cardinality;
        std::vector<std::string> join_conditions;
    };
    
    std::shared_ptr<PlanTreeNode> buildOptimalJoinOrder(const std::vector<std::string>& tables,
                                                        const std::vector<Condition>& conditions);
    size_t estimateJoinResultSize(std::shared_ptr<PlanTreeNode> left, const std::string& right_table);
    
    // 工具方法
    std::string getTablePrefix(const std::string& table_name, const std::vector<std::string>& all_tables);
    std::vector<std::string> conditionsToStringList(const std::vector<Condition>& conditions);
}; 