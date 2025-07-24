/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sm_manager.h"

#include <sys/stat.h>
#include <unistd.h>

#include <fstream>

#include "index/ix.h"
#include "record/rm.h"
#include "record_printer.h"
#include "record/rm_scan.h"

/**
 * @description: 判断是否为一个文件夹
 * @return {bool} 返回是否为一个文件夹
 * @param {string&} db_name 数据库文件名称，与文件夹同名
 */
bool SmManager::is_dir(const std::string& db_name) {
    struct stat st;
    return stat(db_name.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

/**
 * @description: 创建数据库，所有的数据库相关文件都放在数据库同名文件夹下
 * @param {string&} db_name 数据库名称
 */
void SmManager::create_db(const std::string& db_name) {
    if (is_dir(db_name)) {
        throw DatabaseExistsError(db_name);
    }
    //为数据库创建一个子目录
    std::string cmd = "mkdir " + db_name;
    if (system(cmd.c_str()) < 0) {  // 创建一个名为db_name的目录
        throw UnixError();
    }
    if (chdir(db_name.c_str()) < 0) {  // 进入名为db_name的目录
        throw UnixError();
    }
    //创建系统目录
    DbMeta *new_db = new DbMeta();
    new_db->name_ = db_name;

    // 注意，此处ofstream会在当前目录创建(如果没有此文件先创建)和打开一个名为DB_META_NAME的文件
    std::ofstream ofs(DB_META_NAME);

    // 将new_db中的信息，按照定义好的operator<<操作符，写入到ofs打开的DB_META_NAME文件中
    ofs << *new_db;  // 注意：此处重载了操作符<<

    delete new_db;

    // 创建日志文件
    disk_manager_->create_file(LOG_FILE_NAME);

    // 回到根目录
    if (chdir("..") < 0) {
        throw UnixError();
    }
}

/**
 * @description: 删除数据库，同时需要清空相关文件以及数据库同名文件夹
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::drop_db(const std::string& db_name) {
    if (!is_dir(db_name)) {
        throw DatabaseNotFoundError(db_name);
    }
    std::string cmd = "rm -r " + db_name;
    if (system(cmd.c_str()) < 0) {
        throw UnixError();
    }
}

/**
 * @description: 打开数据库，找到数据库对应的文件夹，并加载数据库元数据和相关文件
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::open_db(const std::string& db_name) {
    if (!is_dir(db_name)) {
        throw DatabaseNotFoundError(db_name);
    }

    if (!db_.name_.empty()) {
        throw DatabaseExistsError(db_name);
    }

    // 设置工作目录，保证output.txt和表文件都存储在数据库目录下
    if (chdir(db_name.c_str()) < 0) {
        throw InternalError("Failed to change directory to " + db_name);
    }

    // 加载数据库元数据
    std::ifstream ifs(DB_META_NAME);
    if (ifs.fail()) {
        throw InternalError("Failed to open database metadata file: " + DB_META_NAME);
    }
    ifs >> db_;  // 注意：此处重载了操作符>>，将DB_META_NAME文件中的内容读入到db_中

    // 加载表元数据
    for (auto &t : db_.tabs_) {
        auto &tab = t.second;
        // 加载表文件到 fhs_
        fhs_.emplace(tab.name, rm_manager_->open_file(tab.name));

        // 打开表文件
        // fhs_[tab.name] = rm_manager_->open_file(tab.name);

        // 加载索引文件到 ihs_
        for (auto &index : tab.indexes) {
            const std::string &index_name = index.first;
            // ihs_[index_name] = ix_manager_->open_index(index_name);
            ihs_.emplace(index_name, ix_manager_->open_index(index_name));
        }
    }
}

/**
 * @description: 把数据库相关的元数据刷入磁盘中
 */
void SmManager::flush_meta() {
    // 默认清空文件
    std::ofstream ofs(DB_META_NAME);
    ofs << db_;
}

/**
 * @description: 关闭数据库并把数据落盘
 */
void SmManager::close_db() {
    // flush所有脏page
    for (auto &rm_file : fhs_) {
        buffer_pool_manager_->flush_all_pages(rm_file.second->GetFd());
    }
    // 刷新元数据
    flush_meta();
    db_.name_.clear();
    db_.tabs_.clear();

    // 关闭所有表文件
    for (auto &tab_file : fhs_) {
        rm_manager_->close_file(tab_file.second.get());
    }
    fhs_.clear();

    // 关闭所有索引文件
    for (auto &index_file : ihs_) {
        ix_manager_->close_index(index_file.second.get());
    }
    ihs_.clear();

    // 回到根目录
    if (chdir("..") < 0) {
        throw UnixError();
    }

}

/**
 * @description: 显示所有的表,通过测试需要将其结果写入到output.txt,详情看题目文档
 * @param {Context*} context 
 */
void SmManager::show_tables(Context* context) {
    std::fstream outfile;
    outfile.open("output.txt", std::ios::out | std::ios::app);
    outfile << "| Tables |\n";
    RecordPrinter printer(1);
    printer.print_separator(context);
    printer.print_record({"Tables"}, context);
    printer.print_separator(context);
    for (auto &entry : db_.tabs_) {
        auto &tab = entry.second;
        printer.print_record({tab.name}, context);
        outfile << "| " << tab.name << " |\n";
    }
    printer.print_separator(context);
    outfile.close();
}

/**
 * @description: 显示表的元数据
 * @param {string&} tab_name 表名称
 * @param {Context*} context 
 */
void SmManager::desc_table(const std::string& tab_name, Context* context) {
    TabMeta &tab = db_.get_table(tab_name);

    std::vector<std::string> captions = {"Field", "Type", "Index"};
    RecordPrinter printer(captions.size());
    // Print header
    printer.print_separator(context);
    printer.print_record(captions, context);
    printer.print_separator(context);
    // Print fields
    for (auto &col : tab.cols) {
        std::vector<std::string> field_info = {col.name, coltype2str(col.type), col.index ? "YES" : "NO"};
        printer.print_record(field_info, context);
    }
    // Print footer
    printer.print_separator(context);
}

/**
 * @description: 创建表
 * @param {string&} tab_name 表的名称
 * @param {vector<ColDef>&} col_defs 表的字段
 * @param {Context*} context 
 */
void SmManager::create_table(const std::string& tab_name, const std::vector<ColDef>& col_defs, Context* context) {
    if (db_.is_table(tab_name)) {
        throw TableExistsError(tab_name);
    }
    // Create table meta
    int curr_offset = 0;
    TabMeta tab;
    tab.name = tab_name;
    for (auto &col_def : col_defs) {
        ColMeta col = {.tab_name = tab_name,
                       .name = col_def.name,
                       .type = col_def.type,
                       .len = col_def.len,
                       .offset = curr_offset,
                       .index = false};
        curr_offset += col_def.len;
        tab.cols.push_back(col);
    }
    // Create & open record file
    int record_size = curr_offset;  // record_size就是col meta所占的大小（表的元数据也是以记录的形式进行存储的）
    rm_manager_->create_file(tab_name, record_size);
    db_.tabs_[tab_name] = tab;
    // fhs_[tab_name] = rm_manager_->open_file(tab_name);
    fhs_.emplace(tab_name, rm_manager_->open_file(tab_name));

    flush_meta();
}

/**
 * @description: 删除表
 * @param {string&} tab_name 表的名称
 * @param {Context*} context
 */
void SmManager::drop_table(const std::string& tab_name, Context* context) {
    if (!db_.is_table(tab_name)) {
        throw TableNotFoundError(tab_name);
    }

    // 删除表文件
    rm_manager_->close_file(fhs_[tab_name].get());
    rm_manager_->destroy_file(tab_name);
    fhs_.erase(tab_name);  // 删除对应的文件句柄
    db_.tabs_.erase(tab_name);

    // 更新元数据
    flush_meta();
}

/**
 * @description: 创建索引
 * @param {string&} tab_name 表的名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::create_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {
    std::string index_name = ix_manager_->get_index_name(tab_name, col_names);
    // 如果索引文件存在就抛出异常
    if (disk_manager_->is_file(index_name)) {
        throw IndexExistsError(tab_name, col_names);
    }

    TabMeta &tab = db_.get_table(tab_name);
    // 如果字段不存在就抛出异常
    for (const auto &col_name : col_names) {
        if (!tab.is_col(col_name)) {
            throw ColumnNotFoundError(col_name);
        }
    }

    // 字段存在，正常创建索引，先获取字段的元数据
    std::vector<ColMeta> cols;
    cols.reserve(col_names.size());
    int len = 0;
    for (const auto &col_name : col_names) {
        auto col_meta = tab.get_col(col_name);
        cols.push_back(*col_meta);
        len += col_meta->len;
    }

    // 创建索引元数据
    ix_manager_->create_index(index_name, cols);
    std::unique_ptr<IxIndexHandle> ix_handle = ix_manager_->open_index(index_name);
    RmFileHandle* file_handle = fhs_[tab_name].get();

    // 索引是record的有序存放结构，这里使用B+树来实现索引
    // 唯一索引可以防止重复插入
    // 分配索引键值空间
    char* key = new char[len];
    // 使用先前实现的RmScan扫描表中的记录
    int pos = 0;
    auto scan = std::make_unique<RmScan>(file_handle);
    while (!scan->is_end()) {
        auto rid = scan->rid();
        // std::cout << "[DEBUG] Insert index: rid.page_no=" << rid.page_no << ", rid.slot_no=" << rid.slot_no << std::endl;
        auto record = file_handle->get_record(rid, context);
        pos = 0;
        for (const auto &col_meta: cols) {
            memcpy(key + pos, record->data + col_meta.offset, col_meta.len);
            pos += col_meta.len;
        }
        // 打印key的值用于调试
        // std::cout << "Inserting key (hex): ";
        // for (int i = 0; i < len; ++i) {
        //     printf("%02x ", (unsigned char)key[i]);
        // }
        // std::cout << std::endl;

        // 插入B+树索引
        page_id_t result = ix_handle->insert_entry(key, rid, context->txn_);
        if (result == IX_NO_PAGE) {
            delete[] key;  // 释放key内存
            ix_manager_->close_index(ix_handle.get());
            ix_manager_->destroy_index(index_name);
            throw InternalError("Duplicate key found when creating unique index: " + index_name);
        }
        scan->next();
    }

    delete [] key;  // 释放key内存
    // 更新表的索引信息
    tab.indexes[index_name] = IndexMeta {
        tab_name,
        len,
        static_cast<int>(col_names.size()),
        cols
    };

    ihs_.emplace(index_name, std::move(ix_handle));  
    // 将索引句柄存入ihs_，通过ihs_统一管理索引文件句柄
    flush_meta();
}

/**
 * @description: 显示该表所有的索引,通过测试需要将其结果写入到output.txt,详情看题目文档
 * @param {String} table_name {Context*} context
 */
// 辅助函数：格式化索引字段名
std::string format_index_cols(const std::vector<ColMeta>& cols) {
    std::string result = "(";
    for (size_t i = 0; i < cols.size(); ++i) {
        if (i > 0) result += ",";
        result += cols[i].name;
    }
    result += ")";
    return result;
}

void SmManager::show_indexes(const std::string &table_name, Context *context) {
    std::ofstream outfile("output.txt", std::ios::out | std::ios::app);
    RecordPrinter printer(3);
    std::vector<std::string> rec_str{table_name, "unique", ""};

    for (const auto &[_, index]: db_.tabs_[table_name].indexes) {
        std::string cols_str = format_index_cols(index.cols);
        // 文件输出
        outfile << "| " << table_name << " | unique | " << cols_str << " |\n";
        // 缓冲区输出
        rec_str[2] = cols_str;
        printer.print_indexes(rec_str, context);
    }
}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {
    TabMeta &tab = db_.get_table(tab_name);
    // 获取表的元数据
    std::string index_name = ix_manager_->get_index_name(tab_name, col_names);
    // 无法删除不存在的索引文件
    if (!disk_manager_->is_file(index_name)) {
        throw IndexNotFoundError(tab_name, col_names);
    }

    ix_manager_->close_index(ihs_[index_name].get());
    ix_manager_->destroy_index(index_name);
    ihs_.erase(index_name);  
    // 删除ihs_中的索引文件句柄
    tab.indexes.erase(index_name);  
    // 删除表元数据中的索引信息
    flush_meta();
}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<ColMeta>&} 索引包含的字段元数据
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string& tab_name, const std::vector<ColMeta>& cols, Context* context) {
    TabMeta &tab = db_.get_table(tab_name);
    // 获取表的元数据
    std::string index_name = ix_manager_->get_index_name(tab_name, cols);
    // 无法删除不存在的索引文件
    if (!disk_manager_->is_file(index_name)) {
        std::vector<std::string> col_names;
        col_names.reserve(cols.size());
        for (const auto &col : cols) {
            col_names.push_back(col.name);
        }
        throw IndexNotFoundError(tab_name, col_names);
    }

    ix_manager_->close_index(ihs_[index_name].get());
    ix_manager_->destroy_index(index_name);
    ihs_.erase(index_name);
    // 删除ihs_中的索引文件句柄
    tab.indexes.erase(index_name);  
    // 删除表元数据中的索引信息
    flush_meta();
}

// 新增：统计信息相关方法实现
size_t SmManager::getTableRowCount(const std::string& tab_name) {
    if (!db_.is_table(tab_name)) {
        throw TableNotFoundError(tab_name);
    }
    
    auto it = fhs_.find(tab_name);
    if (it == fhs_.end()) {
        return 0;
    }
    
    RmFileHandle* fh = it->second.get();
    size_t row_count = 0;
    
    // 扫描整个表来计算行数
    try {
        auto scan = std::make_unique<RmScan>(fh);
        while (!scan->is_end()) {
            row_count++;
            scan->next();
        }
    } catch (...) {
        // 如果扫描出错，返回0
        return 0;
    }
    
    return row_count;
}

std::vector<size_t> SmManager::getColumnCardinalities(const std::string& tab_name) {
    std::vector<size_t> cardinalities;
    
    if (!db_.is_table(tab_name)) {
        throw TableNotFoundError(tab_name);
    }
    
    TabMeta& tab = db_.get_table(tab_name);
    auto it = fhs_.find(tab_name);
    if (it == fhs_.end()) {
        // 如果表文件不存在，返回空的基数信息
        cardinalities.resize(tab.cols.size(), 0);
        return cardinalities;
    }
    
    // 为每一列初始化基数为0
    cardinalities.resize(tab.cols.size(), 0);
    
    // 实际实现中应该维护更精确的统计信息
    // 这里简化为返回表行数的估算值
    size_t row_count = getTableRowCount(tab_name);
    for (size_t i = 0; i < cardinalities.size(); i++) {
        // 假设每列的基数约为行数的70%（估算值）
        cardinalities[i] = static_cast<size_t>(row_count * 0.7);
    }
    
    return cardinalities;
}

double SmManager::getSelectivity(const std::string& tab_name, const std::string& col_name, CompOp op) {
    if (!db_.is_table(tab_name)) {
        throw TableNotFoundError(tab_name);
    }
    
    TabMeta& tab = db_.get_table(tab_name);
    if (!tab.is_col(col_name)) {
        throw ColumnNotFoundError(col_name);
    }
    
    // 简化的选择性估算
    switch (op) {
        case OP_EQ:
            return 0.1;   // 等值条件选择性为10%
        case OP_NE:
            return 0.9;   // 不等条件选择性为90%
        case OP_LT:
        case OP_LE:
        case OP_GT:
        case OP_GE:
            return 0.33;  // 范围条件选择性为33%
        default:
            return 0.5;   // 默认选择性为50%
    }
}