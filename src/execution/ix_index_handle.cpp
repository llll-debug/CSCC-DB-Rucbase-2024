/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "ix_index_handle.h"

#include "ix_scan.h"

bool IxNodeHandle::node_is_safe(Operation operation) {
    int min_size = 2;
    if (!is_root_page()) {
        min_size = get_min_size();
    }
    if (operation == Operation::INSERT) {
        return get_size() + 1 < get_max_size();
    }
    if (operation == Operation::DELETE) {
        return get_size() > min_size;
    }
    // 其他操作
    return true;
}

/**
 * @brief 在当前node中查找第一个>=target的key_idx
 *
 * @return key_idx，范围为[0,num_key)，如果返回的key_idx=num_key，则表示target大于最后一个key
 * @note 返回key index（同时也是rid index），作为slot no
 */
int IxNodeHandle::lower_bound(const char *target) const {
    int l = 0, r = page_hdr->num_key;
    while (l < r) {
        int mid = l + r >> 1;
        int cmp = Compare(target, get_key(mid));
        if (cmp <= 0) {
            r = mid;
        } else {
            l = mid + 1;
        }
        }
    return l;
}

/**
 * @brief 在当前node中查找第一个>target的key_idx
 *
 * @return key_idx，范围为[1,num_key)，如果返回的key_idx=num_key，则表示target大于等于最后一个key
 * @note 注意此处的范围从1开始
 */
int IxNodeHandle::upper_bound(const char *target) const {
    int l = 1, r = page_hdr->num_key;
    while (l < r) {
        int mid = l + r >> 1;
        int cmp = Compare(target, get_key(mid));
        if (cmp < 0) {
            r = mid;
        } else {
            l = mid + 1;
        }
        }
    return l;
}

/**
 * @brief 用于叶子结点根据key来查找该结点中的键值对
 * 值value作为传出参数，函数返回是否查找成功
 *
 * @param key 目标key
 * @param[out] value 传出参数，目标key对应的Rid
 * @return 目标key是否存在
 */
bool IxNodeHandle::leaf_lookup(const char *key, Rid **value) {
    int pos = lower_bound(key);
    if (pos == page_hdr->num_key || Compare(key, get_key(pos))) {
        return false;
    }
    *value = get_rid(pos);
    return true;
}

/**
 * 用于内部结点（非叶子节点）查找目标key所在的孩子结点（子树）
 * @param key 目标key
 * @return page_id_t 目标key所在的孩子节点（子树）的存储页面编号
 */
page_id_t IxNodeHandle::internal_lookup(const char *key) {
    return value_at(upper_bound(key) - 1);
}

/**
 * @brief 在指定位置插入n个连续的键值对
 * 将key的前n位插入到原来keys中的pos位置；将rid的前n位插入到原来rids中的pos位置
 *
 * @param pos 要插入键值对的位置
 * @param (key, rid) 连续键值对的起始地址，也就是第一个键值对，可以通过(key, rid)来获取n个键值对
 * @param n 键值对数量
 */
void IxNodeHandle::insert_pairs(int pos, const char *key, const Rid *rid, int n) {
    // 检查 pos 的合法性
    if (pos < 0 || pos > page_hdr->num_key) {
        throw IndexEntryNotFoundError();
    }

    // 获取当前键和 RID 的指针
    auto &&cur_key = get_key(pos);
    auto &&cur_rid = get_rid(pos);
    auto &&num_keys = page_hdr->num_key;
    auto &&cols_len = file_hdr->col_tot_len_;

    // 腾出 (num_keys - pos) 个空间
    if (pos < num_keys) {
        memmove(cur_key + n * cols_len, cur_key, (num_keys - pos) * cols_len);
        memmove(cur_rid + n, cur_rid, (num_keys - pos) * sizeof(Rid));
    }

    // 拷贝 n 个键值对
    memcpy(cur_key, key, n * cols_len);
    memcpy(cur_rid, rid, n * sizeof(Rid));

    // 更新键数量
    page_hdr->num_key += n;
}

/**
 * @brief 用于在结点中插入单个键值对。
 * 函数返回插入后的键值对数量
 *
 * @param (key, value) 要插入的键值对
 * @return std::pair<int, int> 键值对数量和插入的位置
 */
std::pair<int, int> IxNodeHandle::insert_kv_pair(const char *key, const Rid &value) {
    auto &&pos = lower_bound(key);
    if (pos < page_hdr->num_key && Compare(key, get_key(pos)) == 0) {
        return {page_hdr->num_key, -1};
        }
    insert_pair(pos, key, value);
    return {page_hdr->num_key, pos};
}

/**
 * @brief 用于在结点中的指定位置删除单个键值对
 *
 * @param pos 要删除键值对的位置
 */
void IxNodeHandle::erase_pair(int pos) {
    if (pos < 0 || pos >= page_hdr->num_key) {
        throw IndexEntryNotFoundError();
    }

    // 获取当前键和 RID 的指针
    auto &&cur_key = get_key(pos);
    auto &&cur_rid = get_rid(pos);
    auto &&num_keys = page_hdr->num_key;
    auto &&cols_len = file_hdr->col_tot_len_;

    // 腾出 1 个空间
    memmove(cur_key, cur_key + cols_len, (num_keys - pos - 1) * cols_len);
    memmove(cur_rid, cur_rid + 1, (num_keys - pos - 1) * sizeof(Rid));

    // 更新键数量
    --page_hdr->num_key;
}

/**
 * @brief 用于在结点中删除指定key的键值对。函数返回删除后的键值对数量
 *
 * @param key 要删除的键值对key值
 * @return std::pair<int, int> 完成删除操作后的键值对数量和移除位置
 */
std::pair<int, int> IxNodeHandle::remove_key(const char *key) {
    auto &&pos = lower_bound(key);
    if (pos < page_hdr->num_key && Compare(key, get_key(pos)) == 0) {
        erase_pair(pos);
    }
    return {page_hdr->num_key, pos};
}

IxIndexHandle::IxIndexHandle(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, int fd)
    : disk_manager_(disk_manager), buffer_pool_manager_(buffer_pool_manager), fd_(fd) {
    // init file_hdr_
    disk_manager_->read_page(fd, IX_FILE_HDR_PAGE, (char *) &file_hdr_, sizeof(file_hdr_));
    char *buf = new char[PAGE_SIZE];
    memset(buf, 0, PAGE_SIZE);
    disk_manager_->read_page(fd, IX_FILE_HDR_PAGE, buf, PAGE_SIZE);
    file_hdr_ = new IxFileHdr();
    file_hdr_->deserialize(buf);

    // disk_manager管理的fd对应的文件中，设置从file_hdr_->num_pages开始分配page_no
    int now_page_no = disk_manager_->get_fd2pageno(fd);
    disk_manager_->set_fd2pageno(fd, now_page_no + 1);
}

/**
 * @brief 用于查找指定键所在的叶子结点
 * @param key 要查找的目标key值
 * @param operation 查找到目标键值对后要进行的操作类型
 * @param transaction 事务参数，如果不需要则默认传入nullptr
 * @return [leaf node] and [root_is_latched] 返回目标叶子结点以及根结点是否加锁
 * @note need to Unlatch and unpin the leaf node outside!
 * 注意：用了FindLeafPage之后一定要unlatch叶结点，否则下次latch该结点会堵塞！
 */
std::pair<IxNodeHandle *, bool> IxIndexHandle::find_leaf_page(const char *key, Operation operation,
                                                              Transaction *transaction, bool find_first) {
    // 因为根有可能被删除 获取根节点前先加根锁
    bool is_root_locked = false;
    auto &&node = fetch_node(file_hdr_->root_page_);
    if (operation == Operation::FIND) {
        // 读操作
    } else {
        // 写操作
    }

    while (!node->is_leaf_page()) {
        auto &&child_node = fetch_node(find_first ? node->value_at(0) : node->internal_lookup(key));
        if (operation == Operation::FIND) {
        buffer_pool_manager_->unpin_page(node->get_page_id(), false);
        } else {
            // 写操作的处理
        }

        node = child_node;
    }

    return {node, is_root_locked};
}

/**
 * @brief 用于查找指定键在叶子结点中的对应的值result
 *
 * @param key 查找的目标key值
 * @param result 用于存放结果的容器
 * @param transaction 事务指针
 * @return bool 返回目标键值对是否存在
 */
bool IxIndexHandle::get_value(const char *key, std::vector<Rid> *result, Transaction *transaction) {
    auto &&leaf_node = find_leaf_page(key, Operation::FIND, transaction, false).first;
    Rid *rid;
    if (leaf_node->leaf_lookup(key, &rid)) {
        result->emplace_back(*rid);
        buffer_pool_manager_->unpin_page(leaf_node->get_page_id(), false);
        return true;
    }
    buffer_pool_manager_->unpin_page(leaf_node->get_page_id(), false);
    return false;
}

/**
 * @brief  将传入的一个node拆分(Split)成两个结点，在node的右边生成一个新结点new node
 * @param node 需要拆分的结点
 * @return 拆分得到的new_node
 * @note need to unpin the new node outside
 * 注意：本函数执行完毕后，原node和new node都需要在函数外面进行unpin
 */
IxNodeHandle *IxIndexHandle::split_node(IxNodeHandle *node) {
    auto &&new_sibling_node = create_new_node();
    auto &&split_point = node->get_min_size();
    if (node->is_leaf_page()) {
        // 更新左右叶子节点关系
        new_sibling_node->set_prev_leaf(node->get_page_no());
        new_sibling_node->set_next_leaf(node->get_next_leaf());
        node->set_next_leaf(new_sibling_node->get_page_no());
        // 更新下一个叶子节点的前一个节点为新节点
        if (new_sibling_node->get_next_leaf() != IX_NO_PAGE) {
            auto &&next_leaf = fetch_node(new_sibling_node->get_next_leaf());
            next_leaf->set_prev_leaf(new_sibling_node->get_page_no());
                    buffer_pool_manager_->unpin_page(next_leaf->get_page_id(), true);
        }
    }
    // 记得维护节点元信息
    new_sibling_node->page_hdr->num_key = 0;
    new_sibling_node->page_hdr->is_leaf = node->is_leaf_page();
    new_sibling_node->page_hdr->parent = node->get_parent_page_no();
    // 插入到右兄弟节点
    new_sibling_node->insert_pairs(0, node->get_key(split_point), node->get_rid(split_point),
                                   node->page_hdr->num_key - split_point);
    // 这里直接设置 size，软移除
    node->set_size(split_point);

    // 如果是内部节点，还需要维护孩子节点关系
    if (new_sibling_node->is_internal_page()) {
        for (int i = 0; i < new_sibling_node->page_hdr->num_key; ++i) {
            maintain_child(new_sibling_node, i);
        }
    }
    return new_sibling_node;
}

void IxIndexHandle::insert_into_parent(IxNodeHandle *old_node, const char *key, IxNodeHandle *new_node,
                                       Transaction *transaction) {
    // 是否为根结点
    if (old_node->is_root_page()) {
        auto &&new_root = create_new_node();
        new_root->page_hdr->parent = IX_NO_PAGE;
        new_root->page_hdr->num_key = 0;
        new_root->page_hdr->is_leaf = false;
        new_root->page_hdr->prev_leaf = new_root->page_hdr->next_leaf = IX_NO_PAGE;
        new_root->insert_pair(0, old_node->get_key(0), {old_node->get_page_no(), -1});
        new_root->insert_pair(1, key, {new_node->get_page_no(), -1});

        // 维护父子关系
        old_node->set_parent_page_no(new_root->get_page_no());
        new_node->set_parent_page_no(new_root->get_page_no());

        // 成为新的根节点
        file_hdr_->root_page_ = new_root->get_page_no();
        buffer_pool_manager_->unpin_page(new_root->get_page_id(), true);
    } else {
        // 获取原结点（old_node）的父亲结点
        auto &&parent_node = fetch_node(old_node->get_parent_page_no());
        // 将新右兄弟节点头记录信息插入
        parent_node->insert_pair(parent_node->find_child(old_node) + 1, key, {new_node->get_page_no(), -1});
        new_node->set_parent_page_no(parent_node->get_page_no());
        // 插入后满了
        if (parent_node->node_is_full()) {
            auto &&new_sibling_node = split_node(parent_node);
            insert_into_parent(parent_node, new_sibling_node->get_key(0), new_sibling_node, transaction);
            buffer_pool_manager_->unpin_page(new_sibling_node->get_page_id(), true);
        }
        buffer_pool_manager_->unpin_page(parent_node->get_page_id(), true);
    }
}

// 检查是否是 unique key，只有 insert 操作会调用
bool IxIndexHandle::check_unique(const char *key, Rid &value, Transaction *transaction) {
    // 操作应该为insert
    auto &&[leaf_node, is_root_locked] = find_leaf_page(key, Operation::INSERT, transaction, false);
    int pos = leaf_node->lower_bound(key);
    // 释放写锁
    if (pos == leaf_node->page_hdr->num_key || Compare(key, leaf_node->get_key(pos))) {
        buffer_pool_manager_->unpin_page(leaf_node->get_page_id(), false);
        return true;
    }
    value = *leaf_node->get_rid(pos);
    buffer_pool_manager_->unpin_page(leaf_node->get_page_id(), false);
    return false;
}

/**
 * @brief 将指定键值对插入到B+树中
 * @param (key, value) 要插入的键值对
 * @param transaction 事务指针
 * @return page_id_t 插入到的叶结点的page_no
 */
page_id_t IxIndexHandle::insert_entry(const char *key, const Rid &value, Transaction *transaction) {
    auto &&[leaf_node, is_root_locked] = find_leaf_page(key, Operation::INSERT, transaction, false);
    int old_size = leaf_node->get_size();
    // key 重复
    const auto &[new_size, pos] = leaf_node->insert_kv_pair(key, value);
    if (new_size == old_size) {
        if (is_root_locked) {
            root_latch_.unlock();
        }
        buffer_pool_manager_->unpin_page(leaf_node->page->get_page_id(), false);
        return IX_NO_PAGE;
    }

    // 优化，只有插入在第一个key位置时，才需要在父节点中向上更新node的第一个key
    if (pos == 0) {
        maintain_parent(leaf_node);
    }

    page_id_t return_page_id = INVALID_PAGE_ID;
    // 如果结点已满，分裂结点，并把新结点的相关信息插入父节点
    if (leaf_node->node_is_full()) {
        auto &&new_sibling_node = split_node(leaf_node);
        // 分裂完成后兄弟叶子节点关系已经维护好了
        // 维护最右的叶子节点
        if (leaf_node->get_page_no() == file_hdr_->last_leaf_) {
            file_hdr_->last_leaf_ = new_sibling_node->get_page_no();
        }
        insert_into_parent(leaf_node, new_sibling_node->get_key(0), new_sibling_node, transaction);
        // 如果分裂后插入的key在兄弟叶子节点
        if (Compare(new_sibling_node->get_key(new_sibling_node->lower_bound(key)), key) == 0) {
            return_page_id = new_sibling_node->get_page_no();
        } else {
            return_page_id = leaf_node->get_page_no();
        }
        buffer_pool_manager_->unpin_page(leaf_node->get_page_id(), true);
        buffer_pool_manager_->unpin_page(new_sibling_node->get_page_id(), true);
        return return_page_id;
    }

    return_page_id = leaf_node->get_page_no();
    buffer_pool_manager_->unpin_page(leaf_node->get_page_id(), true);
    return return_page_id;
}

/**
 * @brief 用于删除B+树中含有指定key的键值对
 * @param key 要删除的key值
 * @param transaction 事务指针
 */
bool IxIndexHandle::delete_entry(const char *key, Transaction *transaction)
{
    // 1. 获取该键值对所在的叶子结点
    IxNodeHandle *leaf = find_leaf_page(key, Operation::DELETE, transaction, false).first;
    if (leaf == nullptr)
    {
        return false;
    }

    // 记录删除前的键值对数量和位置
    int size_before_deletion = leaf->get_size();
    int pos = leaf->lower_bound(key);

    // 2. 在该叶子结点中删除键值对
    const auto &[size_after_deletion, removed_pos] = leaf->remove_key(key);

    // 删除失败(key不存在)，释放资源并返回false
    if (size_before_deletion == size_after_deletion)
    {
        buffer_pool_manager_->unpin_page(leaf->get_page_id(), false);
        delete leaf;
        return false;
    }

    // 如果删除的是节点的第一个key，需要更新父节点中的key
    if (pos == 0)
    {
        maintain_parent(leaf);
    }

    // 3. 删除成功后进行合并或重分配操作
    bool root_is_latched = false;
    bool should_delete = coalesce_or_redistribute(leaf, transaction, &root_is_latched);

    // 4. 如果需要删除节点
    if (should_delete)
    {
        if (leaf->is_leaf_page())
        {
            // 如果是叶子节点，需要处理叶子节点的链表
            erase_leaf(leaf);
        }
        release_node_handle(*leaf);
        buffer_pool_manager_->delete_page(leaf->get_page_id());
    }

    // 释放资源
    buffer_pool_manager_->unpin_page(leaf->get_page_id(), true);
    delete leaf;

    return true;
}

/**
 * @brief 用于处理合并和重分配的逻辑，用于删除键值对后调用
 *
 * @param node 执行完删除操作的结点
 * @param transaction 事务指针
 * @param root_is_latched 传出参数：根节点是否上锁，用于并发操作
 * @return 是否需要删除结点
 * @note User needs to first find the sibling of input page.
 * If sibling's size + input page's size >= 2 * page's minsize, then redistribute.
 * Otherwise, merge(Coalesce).
 */
bool IxIndexHandle::coalesce_or_redistribute(IxNodeHandle *node, Transaction *transaction, bool *root_is_latched)
{
    // 1. 判断node结点是否为根节点
    if (node->get_page_no() == file_hdr_->root_page_)
    {
        return adjust_root(node);
    }

    // 1.2 如果不是根节点，检查是否需要合并或重分配
    if (node->get_size() >= node->get_min_size())
    {
        return false;
    }

    // 2. 获取node结点的父亲结点
    IxNodeHandle *parent = fetch_node(node->get_parent_page_no());

    // 3. 寻找node结点的兄弟结点（优先选取前驱结点）
    int node_idx = parent->find_child(node);
    IxNodeHandle *neighbor_node;
    int neighbor_idx;

    // 优先选择前驱节点作为兄弟节点
    if (node_idx > 0)
    {
        // 存在前驱节点
        neighbor_idx = node_idx - 1;
        neighbor_node = fetch_node(parent->value_at(neighbor_idx));
    }
    else
    {
        // 不存在前驱节点，选择后继节点
        neighbor_idx = 1;
        neighbor_node = fetch_node(parent->value_at(neighbor_idx));
    }

    // 4. 判断是否需要重分配
    bool should_delete = false;
    if (node->get_size() + neighbor_node->get_size() >= node->get_max_size())
    {
        // 重新分配键值对
        redistribute_keys(neighbor_node, node, parent, node_idx);
    }
    else
    {
        // 5. 合并节点
        should_delete = coalesce_nodes(&neighbor_node, &node, &parent, node_idx, transaction, root_is_latched);
    }

    // 释放资源
    buffer_pool_manager_->unpin_page(parent->get_page_id(), true);
    buffer_pool_manager_->unpin_page(neighbor_node->get_page_id(), true);
    delete parent;
    delete neighbor_node;

    return should_delete;
}

/**
 * @brief 用于当根结点被删除了一个键值对之后的处理
 * @param old_root_node 原根节点
 * @return bool 根结点是否需要被删除
 * @note size of root page can be less than min size and this method is only called within coalesce_or_redistribute()
 */
bool IxIndexHandle::adjust_root(IxNodeHandle *old_root_node)
{
    // 1. 如果old_root_node是内部结点，并且大小为1，则直接把它的孩子更新成新的根结点
    if (!old_root_node->is_leaf_page() && old_root_node->get_size() == 1)
    {
        // 获取根节点唯一的孩子
        int child_page_no = old_root_node->value_at(0);
        IxNodeHandle *child = fetch_node(child_page_no);

        // 更新孩子节点为新的根节点
        child->set_parent_page_no(IX_NO_PAGE);
        file_hdr_->root_page_ = child_page_no;

        buffer_pool_manager_->unpin_page(child->get_page_id(), true);
        delete child;

        buffer_pool_manager_->delete_page(old_root_node->get_page_id());
        release_node_handle(*old_root_node);

        return true;
    }

    // 2. 如果old_root_node是叶结点，且大小为0，则直接更新root page
    if (old_root_node->is_leaf_page() && old_root_node->get_size() == 0)
    {
        file_hdr_->root_page_ = IX_INIT_ROOT_PAGE; // 设置根节点为初始值
        return true;
    }

    // 3. 其他情况不需要调整
    return false;
}

/**
 * @brief 重新分配node和兄弟结点neighbor_node的键值对
 * Redistribute key & value pairs from one page to its sibling page. If index == 0, move sibling page's first key
 * & value pair into end of input "node", otherwise move sibling page's last key & value pair into head of input "node".
 *
 * @param neighbor_node sibling page of input "node"
 * @param node input from method coalesceOrRedistribute()
 * @param parent the parent of "node" and "neighbor_node"
 * @param index node在parent中的rid_idx
 * @note node是之前刚被删除过一个key的结点
 * index=0，则neighbor是node后继结点，表示：node(left)      neighbor(right)
 * index>0，则neighbor是node前驱结点，表示：neighbor(left)  node(right)
 * 注意更新parent结点的相关kv对
 */
void IxIndexHandle::redistribute_keys(IxNodeHandle *neighbor_node, IxNodeHandle *node, IxNodeHandle *parent, int index)
{
    // 1. 根据index判断neighbor_node是node的前驱还是后继节点
    if (index == 0)
    {
        // case 1: node(left) neighbor(right)
        // 将neighbor第一个键值对移动到node的末尾
        const char *key = neighbor_node->get_key(0);
        const Rid *rid = neighbor_node->get_rid(0);

        // 移动键值对到node末尾
        memcpy(node->get_key(node->get_size()), key, file_hdr_->col_tot_len_);
        memcpy(node->get_rid(node->get_size()), rid, sizeof(Rid));
        node->page_hdr->num_key++;

        // 删除neighbor第一个键值对
        neighbor_node->erase_pair(0);

        // 更新父节点中index+1处的key为neighbor新的第一个key
        memcpy(parent->get_key(index + 1), neighbor_node->get_key(0), file_hdr_->col_tot_len_);

        // 如果是内部节点，维护移动过来的键值对对应的子节点的父指针
        if (!node->is_leaf_page())
        {
            maintain_child(node, node->get_size() - 1);
        }
    }
    else
    {
        // case 2: neighbor(left) node(right)
        // 将neighbor最后一个键值对移动到node的开头
        const char *key = neighbor_node->get_key(neighbor_node->get_size() - 1);
        const Rid *rid = neighbor_node->get_rid(neighbor_node->get_size() - 1);

        // 为node开头腾出空间
        for (int i = node->get_size(); i > 0; i--)
        {
            memcpy(node->get_key(i), node->get_key(i - 1), file_hdr_->col_tot_len_);
            memcpy(node->get_rid(i), node->get_rid(i - 1), sizeof(Rid));
        }

        // 插入键值对到node开头
        memcpy(node->get_key(0), key, file_hdr_->col_tot_len_);
        memcpy(node->get_rid(0), rid, sizeof(Rid));
        node->page_hdr->num_key++;

        // 删除neighbor最后一个键值对
        neighbor_node->page_hdr->num_key--;

        // 更新父节点中index处的key为node新的第一个key
        memcpy(parent->get_key(index), node->get_key(0), file_hdr_->col_tot_len_);

        // 如果是内部节点，维护移动过来的键值对对应的子节点的父指针
        if (!node->is_leaf_page())
        {
            maintain_child(node, 0);
        }
    }
}

/**
 * @brief 合并(Coalesce)函数是将node和其直接前驱进行合并，也就是和它左边的neighbor_node进行合并；
 * 假设node一定在右边。如果上层传入的index=0，说明node在左边，那么交换node和neighbor_node，保证node在右边；合并到左结点，实际上就是删除了右结点；
 * Move all the key & value pairs from one page to its sibling page, and notify buffer pool manager to delete this page.
 * Parent page must be adjusted to take info of deletion into account. Remember to deal with coalesce or redistribute
 * recursively if necessary.
 *
 * @param neighbor_node sibling page of input "node" (neighbor_node是node的前结点)
 * @param node input from method coalesceOrRedistribute() (node结点是需要被删除的)
 * @param parent parent page of input "node"
 * @param index node在parent中的rid_idx
 * @return true means parent node should be deleted, false means no deletion happend
 * @note Assume that *neighbor_node is the left sibling of *node (neighbor -> node)
 */
bool IxIndexHandle::coalesce_nodes(IxNodeHandle **neighbor_node, IxNodeHandle **node, IxNodeHandle **parent, int index,
                             Transaction *transaction, bool *root_is_latched)
{
    // 1. 如果node不是neighbor_node的后继节点，则交换两个节点
    if (index == 0)
    {
        std::swap(*neighbor_node, *node);
        index = 1;
    }

    // 2. 把node节点的键值对移动到neighbor_node中
    int start_idx = (*neighbor_node)->get_size();
    (*neighbor_node)->insert_pairs(start_idx, (*node)->get_key(0), (*node)->get_rid(0), (*node)->get_size());

    // 3. 如果是内部节点，需要更新被移动的子节点的父指针
    if (!(*node)->is_leaf_page())
    {
        for (int i = 0; i < (*node)->get_size(); i++)
        {
            IxNodeHandle *child = fetch_node((*node)->value_at(i));
            child->set_parent_page_no((*neighbor_node)->get_page_no());
            buffer_pool_manager_->unpin_page(child->get_page_id(), true);
        }
    }

    // 4. 如果是叶子节点，需要更新链表指针
    if ((*node)->is_leaf_page())
    {
        (*neighbor_node)->set_next_leaf((*node)->get_next_leaf());
        if ((*node)->get_next_leaf() != IX_NO_PAGE)
        {
            IxNodeHandle *next_leaf = fetch_node((*node)->get_next_leaf());
            next_leaf->set_prev_leaf((*neighbor_node)->get_page_no());
            buffer_pool_manager_->unpin_page(next_leaf->get_page_id(), true);
        }
        else
        {
            // 如果node是最右叶子节点，更新file_hdr_
            file_hdr_->last_leaf_ = (*neighbor_node)->get_page_no();
        }
    }

    // 5. 在父节点中删除node的信息
    (*parent)->erase_pair(index);

    // 6. 如果父节点空间不足，递归处理
    bool should_delete_parent = false;
    if ((*parent)->get_size() < (*parent)->get_min_size())
    {
        should_delete_parent = coalesce_or_redistribute(*parent, transaction, root_is_latched);
    }

    return should_delete_parent;
}

/**
 * @brief 这里把iid转换成了rid，即iid的slot_no作为node的rid_idx(key_idx)
 * node其实就是把slot_no作为键值对数组的下标
 * 换而言之，每个iid对应的索引槽存了一对(key,rid)，指向了(要建立索引的属性首地址,插入/删除记录的位置)
 *
 * @param iid
 * @return Rid
 * @note iid和rid存的不是一个东西，rid是上层传过来的记录位置，iid是索引内部生成的索引槽位置
 */
Rid IxIndexHandle::get_rid(const Iid &iid) const
{
    IxNodeHandle *node = fetch_node(iid.page_no);
    if (iid.slot_no >= node->get_size())
    {
        throw IndexEntryNotFoundError();
    }
    Rid r = *node->get_rid(iid.slot_no);
    // std::cout << "[DEBUG] IxIndexHandle::get_rid: iid.page_no=" << iid.page_no
    //           << ", iid.slot_no=" << iid.slot_no
    //           << ", rid.page_no=" << r.page_no
    //           << ", rid.slot_no=" << r.slot_no << std::endl;
    buffer_pool_manager_->unpin_page(node->get_page_id(), false); // unpin it!
    return r;
}

/**
 * @brief FindLeafPage + upper_bound
 *
 * @param key
 * @return Iid
 */
Iid IxIndexHandle::upper_bound(const char *key) {
    auto &&leaf_node = find_leaf_page(key, Operation::FIND, nullptr, false).first;
    auto &&pos = leaf_node->upper_bound(key);
    Iid iid{};
    if (pos == leaf_node->get_size()) {
        if (leaf_node->get_page_no() == file_hdr_->last_leaf_) {
            iid = {leaf_node->get_page_no(), pos};
        } else {
            // 比如leafnode最后一个是38，而右边开始是40
            // 我找<=39，然后如果不是最右叶子节点，那么要找的地方必然是下一个叶子的第0个位置
            iid = {leaf_node->get_next_leaf(), 0};
        }
    } else {
        // 如果第一个比他大的在第 0 个 key
        if (pos == 1 && Compare(key, leaf_node->get_key(0)) < 0) {
            --pos;
        }
        iid = {leaf_node->get_page_no(), pos};
    }
    buffer_pool_manager_->unpin_page(leaf_node->get_page_id(), false);
    delete leaf_node;
    return iid;
}

/**
 * @brief FindLeafPage + lower_bound
 *
 * @param key
 * @return Iid
 */
Iid IxIndexHandle::lower_bound(const char *key) {
    auto &&leaf_node = find_leaf_page(key, Operation::FIND, nullptr, false).first;
    auto &&pos = leaf_node->lower_bound(key);
    Iid iid{};
    if (pos == leaf_node->get_size()) {
        if (leaf_node->get_page_no() == file_hdr_->last_leaf_) {
            iid = {leaf_node->get_page_no(), pos};
        } else {
            // 比如leafnode最后一个是38，而右边叶子开始是40
            // 我找>=39，然后如果不是最右叶子节点，那么要找的地方必然是下一个叶子的第0个位置
            iid = {leaf_node->get_next_leaf(), 0};
        }
    } else {
        iid = {leaf_node->get_page_no(), pos};
    }
    buffer_pool_manager_->unpin_page(leaf_node->get_page_id(), false);
    delete leaf_node;
    return iid;
}

/**
 * @brief 指向最后一个叶子的最后一个结点的后一个
 * 用处在于可以作为IxScan的最后一个
 *
 * @return Iid
 */
Iid IxIndexHandle::leaf_end() const
{
    IxNodeHandle *node = fetch_node(file_hdr_->last_leaf_);
    Iid iid = {.page_no = file_hdr_->last_leaf_, .slot_no = node->get_size()};
    buffer_pool_manager_->unpin_page(node->get_page_id(), false); // unpin it!
    delete node;
    return iid;
}

/**
 * @brief 指向第一个叶子的第一个结点
 * 用处在于可以作为IxScan的第一个
 *
 * @return Iid
 */
Iid IxIndexHandle::leaf_begin() const
{
    Iid iid = {.page_no = file_hdr_->first_leaf_, .slot_no = 0};
    return iid;
}

/**
 * @brief 获取一个指定结点
 *
 * @param page_no
 * @return IxNodeHandle*
 * @note pin the page, remember to unpin it outside!
 */
IxNodeHandle *IxIndexHandle::fetch_node(int page_no) const
{
    Page *page = buffer_pool_manager_->fetch_page(PageId{fd_, page_no});
    IxNodeHandle *node = new IxNodeHandle(file_hdr_, page);

    return node;
}

/**
 * @brief 创建一个新结点
 *
 * @return IxNodeHandle*
 * @note pin the page, remember to unpin it outside!
 * 注意：对于Index的处理是，删除某个页面后，认为该被删除的页面是free_page
 * 而first_free_page实际上就是最新被删除的页面，初始为IX_NO_PAGE
 * 在最开始插入时，一直是create node，那么first_page_no一直没变，一直是IX_NO_PAGE
 * 与Record的处理不同，Record将未插入满的记录页认为是free_page
 */
IxNodeHandle *IxIndexHandle::create_new_node()
{
    IxNodeHandle *node;
    file_hdr_->num_pages_++;

    PageId new_page_id = {.fd = fd_, .page_no = INVALID_PAGE_ID};
    // 从3开始分配page_no，第一次分配之后，new_page_id.page_no=3，file_hdr_.num_pages=4
    Page *page = buffer_pool_manager_->new_page(&new_page_id);
    memset(page->get_data(), 0, PAGE_SIZE);
    node = new IxNodeHandle(file_hdr_, page);
    return node;
}

/**
 * @brief 从node开始更新其父节点的第一个key，一直向上更新直到根节点
 *
 * @param node
 */
void IxIndexHandle::maintain_parent(IxNodeHandle *node)
{
    IxNodeHandle *curr = node;
    while (curr->get_parent_page_no() != IX_NO_PAGE)
    {
        // Load its parent
        IxNodeHandle *parent = fetch_node(curr->get_parent_page_no());
        int rank = parent->find_child(curr);
        char *parent_key = parent->get_key(rank);
        char *child_first_key = curr->get_key(0);
        if (memcmp(parent_key, child_first_key, file_hdr_->col_tot_len_) == 0)
        {
            assert(buffer_pool_manager_->unpin_page(parent->get_page_id(), true));
            break;
        }
        memcpy(parent_key, child_first_key, file_hdr_->col_tot_len_); // 修改了parent node
        assert(buffer_pool_manager_->unpin_page(parent->get_page_id(), true));
        
        if (curr != node) {
            delete curr;  // 释放之前的parent节点
        }
        curr = parent;
    }
    
    // 释放最后的parent节点
    if (curr != node) {
        delete curr;
    }
}

/**
 * @brief 要删除leaf之前调用此函数，更新leaf前驱结点的next指针和后继结点的prev指针
 *
 * @param leaf 要删除的leaf
 */
void IxIndexHandle::erase_leaf(IxNodeHandle *leaf)
{
    assert(leaf->is_leaf_page());

    IxNodeHandle *prev = fetch_node(leaf->get_prev_leaf());
    prev->set_next_leaf(leaf->get_next_leaf());
    buffer_pool_manager_->unpin_page(prev->get_page_id(), true);
    delete prev;

    IxNodeHandle *next = fetch_node(leaf->get_next_leaf());
    next->set_prev_leaf(leaf->get_prev_leaf()); // 注意此处是SetPrevLeaf()
    buffer_pool_manager_->unpin_page(next->get_page_id(), true);
    delete next;
}

/**
 * @brief 删除node时，更新file_hdr_.num_pages
 *
 * @param node
 */
void IxIndexHandle::release_node_handle(IxNodeHandle &node)
{
    file_hdr_->num_pages_--;
}

/**
 * @brief 将node的第child_idx个孩子结点的父节点置为node
 */
void IxIndexHandle::maintain_child(IxNodeHandle *node, int child_idx)
{
    if (!node->is_leaf_page())
    {
        //  Current node is inner node, load its child and set its parent to current node
        int child_page_no = node->value_at(child_idx);
        IxNodeHandle *child = fetch_node(child_page_no);
        child->set_parent_page_no(node->get_page_no());
        buffer_pool_manager_->unpin_page(child->get_page_id(), true);
        delete child;
    }
}