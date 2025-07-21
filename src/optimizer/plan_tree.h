#pragma once

#include <memory>
#include <string>
#include <vector>
#include <algorithm>
#include <iostream>

// 查询计划树节点基类
class PlanTreeNode {
public:
    enum NodeType { FILTER = 0, JOIN = 1, PROJECT = 2, SCAN = 3 };
    
    virtual ~PlanTreeNode() = default;
    virtual NodeType getType() const = 0;
    virtual std::string toString(int indent = 0) const = 0;
    virtual std::vector<std::string> getOutputTables() const = 0;
};

// Scan节点：Scan(table=表名)
class ScanNode : public PlanTreeNode {
private:
    std::string table_name_;
    
public:
    ScanNode(const std::string& table_name) : table_name_(table_name) {}
    
    NodeType getType() const override { return SCAN; }
    
    std::string toString(int indent = 0) const override {
        std::string spaces(indent, '\t');
        return spaces + "Scan(table=" + table_name_ + ")";
    }
    
    std::vector<std::string> getOutputTables() const override {
        return {table_name_};
    }
    
    const std::string& getTableName() const { return table_name_; }
};

// Filter节点：Filter(condition=[条件1,条件2,...])
class FilterNode : public PlanTreeNode {
private:
    std::shared_ptr<PlanTreeNode> child_;
    std::vector<std::string> conditions_;
    
public:
    FilterNode(std::shared_ptr<PlanTreeNode> child, std::vector<std::string> conditions)
        : child_(std::move(child)), conditions_(std::move(conditions)) {}
    
    NodeType getType() const override { return FILTER; }
    
    std::string toString(int indent = 0) const override {
        std::string spaces(indent, '\t');
        std::string result = spaces + "Filter(condition=[";
        
        // 按字典序排序条件
        auto sorted_conditions = conditions_;
        std::sort(sorted_conditions.begin(), sorted_conditions.end());
        
        for (size_t i = 0; i < sorted_conditions.size(); ++i) {
            if (i > 0) result += ",";
            result += sorted_conditions[i];
        }
        result += "])\n";
        result += child_->toString(indent + 1);
        return result;
    }
    
    std::vector<std::string> getOutputTables() const override {
        return child_->getOutputTables();
    }
    
    const std::shared_ptr<PlanTreeNode>& getChild() const { return child_; }
    void setChild(std::shared_ptr<PlanTreeNode> child) { child_ = std::move(child); }
    const std::vector<std::string>& getConditions() const { return conditions_; }
};

// Project节点：Project(columns=[表名1.列名1,表名2.列名2,...])
class ProjectNode : public PlanTreeNode {
private:
    std::shared_ptr<PlanTreeNode> child_;
    std::vector<std::string> columns_;
    bool select_all_;
    
public:
    ProjectNode(std::shared_ptr<PlanTreeNode> child, std::vector<std::string> columns, bool select_all = false)
        : child_(std::move(child)), columns_(std::move(columns)), select_all_(select_all) {}
    
    NodeType getType() const override { return PROJECT; }
    
    std::string toString(int indent = 0) const override {
        std::string spaces(indent, '\t');
        std::string result = spaces + "Project(columns=[";
        
        if (select_all_) {
            result += "*";
        } else {
            // 按字母顺序排序列名（题目要求所有Project节点都按字母排序）
            auto sorted_columns = columns_;
            std::sort(sorted_columns.begin(), sorted_columns.end());
            
            // 所有Project节点都显示完整的"表名.列名"格式
            for (size_t i = 0; i < sorted_columns.size(); ++i) {
                if (i > 0) result += ",";
                result += sorted_columns[i];  // 保持完整格式，不去掉表名前缀
            }
        }
        result += "])\n";
        result += child_->toString(indent + 1);
        return result;
    }
    
    std::vector<std::string> getOutputTables() const override {
        return child_->getOutputTables();
    }
    
    const std::shared_ptr<PlanTreeNode>& getChild() const { return child_; }
    void setChild(std::shared_ptr<PlanTreeNode> child) { child_ = std::move(child); }
    bool isSelectAll() const { return select_all_; }
    const std::vector<std::string>& getColumns() const { return columns_; }
};

// Join节点：Join(tables=[表名1,表名2,...], condition=[条件1,条件2,...])
class JoinNode : public PlanTreeNode {
private:
    std::shared_ptr<PlanTreeNode> left_;
    std::shared_ptr<PlanTreeNode> right_;
    std::vector<std::string> conditions_;
    
public:
    JoinNode(std::shared_ptr<PlanTreeNode> left, std::shared_ptr<PlanTreeNode> right, 
             std::vector<std::string> conditions)
        : left_(std::move(left)), right_(std::move(right)), conditions_(std::move(conditions)) {}
    
    NodeType getType() const override { return JOIN; }
    
    std::string toString(int indent = 0) const override {
        std::string spaces(indent, '\t');
        
        // 获取所有涉及的表名并排序
        auto tables = getOutputTables();
        std::sort(tables.begin(), tables.end());
        
        std::string result = spaces + "Join(tables=[";
        for (size_t i = 0; i < tables.size(); ++i) {
            if (i > 0) result += ",";
            result += tables[i];
        }
        result += "],condition=[";
        
        // 排序连接条件
        auto sorted_conditions = conditions_;
        std::sort(sorted_conditions.begin(), sorted_conditions.end());
        
        for (size_t i = 0; i < sorted_conditions.size(); ++i) {
            if (i > 0) result += ",";
            result += sorted_conditions[i];
        }
        result += "])\n";
        
        // 子节点按字典序输出
        if (shouldLeftFirst()) {
            result += left_->toString(indent + 1) + "\n";
            result += right_->toString(indent + 1);
        } else {
            result += right_->toString(indent + 1) + "\n";
            result += left_->toString(indent + 1);
        }
        
        return result;
    }
    
    std::vector<std::string> getOutputTables() const override {
        auto left_tables = left_->getOutputTables();
        auto right_tables = right_->getOutputTables();
        
        std::vector<std::string> all_tables;
        all_tables.insert(all_tables.end(), left_tables.begin(), left_tables.end());
        all_tables.insert(all_tables.end(), right_tables.begin(), right_tables.end());
        
        return all_tables;
    }
    
    const std::shared_ptr<PlanTreeNode>& getLeft() const { return left_; }
    const std::shared_ptr<PlanTreeNode>& getRight() const { return right_; }
    void setLeft(std::shared_ptr<PlanTreeNode> left) { left_ = std::move(left); }
    void setRight(std::shared_ptr<PlanTreeNode> right) { right_ = std::move(right); }
    const std::vector<std::string>& getConditions() const { return conditions_; }
    
private:
    bool shouldLeftFirst() const {
        // 按照题目要求的排序规则：Filter Join Project Scan
        if (left_->getType() != right_->getType()) {
            return left_->getType() < right_->getType();
        }
        
        // 同类型节点的特殊排序规则
        NodeType type = left_->getType();
        
        if (type == FILTER) {
            // Filter按照条件的字典序升序输出
            auto left_filter = std::dynamic_pointer_cast<FilterNode>(left_);
            auto right_filter = std::dynamic_pointer_cast<FilterNode>(right_);
            if (left_filter && right_filter) {
                auto left_conditions = left_filter->getConditions();
                auto right_conditions = right_filter->getConditions();
                std::sort(left_conditions.begin(), left_conditions.end());
                std::sort(right_conditions.begin(), right_conditions.end());
                
                if (!left_conditions.empty() && !right_conditions.empty()) {
                    return left_conditions[0] < right_conditions[0];
                }
            }
        } else if (type == PROJECT) {
            // Project节点按照"表名.列名"升序输出
            auto left_project = std::dynamic_pointer_cast<ProjectNode>(left_);
            auto right_project = std::dynamic_pointer_cast<ProjectNode>(right_);
            if (left_project && right_project) {
                auto left_columns = left_project->getColumns();
                auto right_columns = right_project->getColumns();
                std::sort(left_columns.begin(), left_columns.end());
                std::sort(right_columns.begin(), right_columns.end());
                
                if (!left_columns.empty() && !right_columns.empty()) {
                    return left_columns[0] < right_columns[0];
                }
            }
        } else if (type == SCAN) {
            // Scan节点按照table升序输出
            auto left_scan = std::dynamic_pointer_cast<ScanNode>(left_);
            auto right_scan = std::dynamic_pointer_cast<ScanNode>(right_);
            if (left_scan && right_scan) {
                return left_scan->getTableName() < right_scan->getTableName();
            }
        } else if (type == JOIN) {
            // Join节点按照table升序输出（使用第一个表名）
            auto left_tables = left_->getOutputTables();
            auto right_tables = right_->getOutputTables();
            std::sort(left_tables.begin(), left_tables.end());
            std::sort(right_tables.begin(), right_tables.end());
            
            if (!left_tables.empty() && !right_tables.empty()) {
                return left_tables[0] < right_tables[0];
            }
        }
        
        // 默认情况：保持构建时的左深树顺序
        return true;
    }
}; 