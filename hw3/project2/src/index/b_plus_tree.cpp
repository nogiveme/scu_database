/**
 * b_plus_tree.cpp
 */
#include <iostream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "index/b_plus_tree.h"
#include "page/header_page.h"

namespace scudb {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(const std::string &name,
                                BufferPoolManager *buffer_pool_manager,
                                const KeyComparator &comparator,
                                page_id_t root_page_id)
    : index_name_(name), root_page_id_(root_page_id),
      buffer_pool_manager_(buffer_pool_manager), comparator_(comparator) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key,
                              std::vector<ValueType> &result,
                              Transaction *transaction) {
  // check
  if(IsEmpty()) return false;

  // search
  auto leaf_raw_page = FindLeafPage(key, false, transaction, Operation::SEARCH);
  B_PLUS_TREE_LEAF_PAGE_TYPE* leaf_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(leaf_raw_page->GetData());
  ValueType vt;

  if(leaf_page->Lookup(key, vt, comparator_)){
    result.push_back(vt);
    UnlockPage(leaf_raw_page, transaction, Operation::SEARCH);
    return true;
  } else {
    UnlockPage(leaf_raw_page, transaction, Operation::SEARCH);
    return false;
  }
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value,
                            Transaction *transaction) {
  if(IsEmpty()) {
    std::cout << "start a new tree" << std::endl;
    StartNewTree(key, value, transaction);
    return true;
  }
  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value, Transaction* txn) {
  // detection
  assert(IsEmpty());

  // require new page
  auto page = buffer_pool_manager_->NewPage(root_page_id_);
  if(page == nullptr)
    throw Exception(ExceptionType::EXCEPTION_TYPE_INDEX, "StartNewTree: out of memory");
  
  // update root
  LockPage(page, txn, Operation::INSERT);
  B_PLUS_TREE_LEAF_PAGE_TYPE* root_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(page->GetData());
  UpdateRootPageId(true);
  
  // config root
  root_page->Init(root_page_id_, INVALID_PAGE_ID);
  assert(!IsEmpty());
  root_page->Insert(key, value, comparator_);
  page->WUnlatch();

  // other updates
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  txn->GetPageSet()->pop_front();
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value,
                                    Transaction *transaction) {
  // get leaf page
  auto leaf_raw_page = FindLeafPage(key, false, transaction, Operation::INSERT);
  std::cout << "found leaf page" << std::endl;
  B_PLUS_TREE_LEAF_PAGE_TYPE* leaf_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(leaf_raw_page->GetData());
  
  // look up and insert
  ValueType vt;
  if(leaf_page->Lookup(key, vt, comparator_)){
    // duplicate
    std::cout << "b plus tree: duplicate pair" << std::endl;
    UnlockParentPage(leaf_raw_page, transaction, Operation::INSERT);
    UnlockPage(leaf_raw_page, transaction, Operation::INSERT);
    return false;
  } else {
    // // not find key & value pair
    // if(leaf_page->GetSize() < leaf_page->GetMaxSize()){
    //   // not full
    //   leaf_page->Insert(key, value, comparator_);
    //   UnlockParentPage(page, transaction, Operation::INSERT);
    //   UnlockPage(leaf_page, transaction, Operation::INSERT);
    // } else {
    //   // split
    //   auto n_leaf_page = Split(leaf_page);
    //   n_leaf_page->SetParentPageId(leaf_page->GetParentPageId());
    //   n_leaf_page->SetNextPage(leaf_page->GetNextPage());
    //   leaf_page->SetNextPage(n_leaf_page->GetPageId());
      
    //   // insert
    //   auto mid = n_leaf_page->KeyAt(0);
    //   if(comparator_(key, mid) < 0) 
    //     leaf_page->Insert(key, value, comparator_);
    //   else
    //     n_leaf_page->Insert(key, value, comparator_);
      
    //   InsertIntoParent(leaf_page, mid, n_leaf_page);

    //   // unlock
    //   UnlockParentPage(page, transaction, Operation::INSERT);
    //   UnlockPage(leaf_page, transaction, Operation::INSERT);
    // }

    // insert 
    std::cout << "page " << leaf_page->GetPageId() << ": start insert" << std::endl; 
    int cur_size = leaf_page->Insert(key, value, comparator_);
    std::cout << "page " << leaf_page->GetPageId() << ": finish insert" << std::endl;

    // check full
    if(cur_size >= leaf_page->GetMaxSize()) {
      std::cout << "page " << leaf_page->GetPageId() << ": start split" << std::endl;

      // split
      auto n_leaf_page = Split(leaf_page);
      n_leaf_page->SetParentPageId(leaf_page->GetParentPageId());
      n_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
      leaf_page->SetNextPageId(n_leaf_page->GetPageId());

      // insert into parent
      auto mid = n_leaf_page->KeyAt(0);
      InsertIntoParent(leaf_page, mid, n_leaf_page, transaction);

      // unlock
      UnlockParentPage(leaf_raw_page, transaction, Operation::INSERT);
      std::cout << "page " << leaf_page->GetPageId() << ": finish spliting" << std::endl;
    }
    UnlockPage(leaf_raw_page, transaction, Operation::INSERT);
  }
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N> N *BPLUSTREE_TYPE::Split(N *node) {
  // require new page
  page_id_t page_id;
  auto page = buffer_pool_manager_->NewPage(page_id);
  if(page == nullptr)
    throw Exception(ExceptionType::EXCEPTION_TYPE_INDEX, "Split: out of memory");
  
  // move half pairs
  auto n_page = reinterpret_cast<N*>(page->GetData());
  n_page->Init(page_id, INVALID_PAGE_ID);
  node->MoveHalfTo(n_page, buffer_pool_manager_);
  
  return n_page;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node,
                                      const KeyType &key,
                                      BPlusTreePage *new_node,
                                      Transaction *transaction) {
  // detection
  assert(old_node != nullptr && new_node != nullptr);
  
  //check root
  int parent_page_id;
  if(old_node->IsRootPage()) {
    // require new page
    auto root_raw_page = buffer_pool_manager_->NewPage(root_page_id_);
    if(root_raw_page == nullptr)
      throw Exception(ExceptionType::EXCEPTION_TYPE_INDEX, "InsertIntoParent: out of memory");
    
    // config new root
    auto root_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,
                                        KeyComparator>*>(root_raw_page->GetData());
    root_page->Init(root_page_id_, INVALID_PAGE_ID);
    root_page->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());

    // update children
    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);

    // update root
    UpdateRootPageId(true);

  } else {
    // get parent page
    parent_page_id = old_node->GetParentPageId();
    auto parent_raw_page = buffer_pool_manager_->FetchPage(parent_page_id);
    auto parent_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,
                                          KeyComparator>*>(parent_raw_page->GetData());

    // insert into parent
    int cur_size = parent_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    
    // check parent
    if(cur_size >= parent_page->GetMaxSize()) {
      // split
      auto n_parent_page = Split(parent_page);
      n_parent_page->SetParentPageId(parent_page->GetParentPageId());

      // insert into parent
      auto mid = n_parent_page->KeyAt(0);
      InsertIntoParent(parent_page, mid, n_parent_page);
    }

    // unpin page
    buffer_pool_manager_->UnpinPage(parent_page_id, true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if(IsEmpty()) return ;

  // remove
  auto leaf_raw_page = FindLeafPage(key, false, transaction, Operation::DELETE);
  B_PLUS_TREE_LEAF_PAGE_TYPE* leaf_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(leaf_raw_page->GetData());
  int cur_size = leaf_page->GetSize();
  int size_after_deletion = leaf_page->RemoveAndDeleteRecord(key, comparator_);

  // key not exist
  if(cur_size == size_after_deletion){
    UnlockParentPage(leaf_raw_page, transaction, Operation::DELETE);
    UnlockPage(leaf_raw_page, transaction, Operation::DELETE);
    return ;
  }

  bool res = false;
  if(size_after_deletion < leaf_page->GetMinSize()){
    // merge or redistribute
    res = CoalesceOrRedistribute(leaf_page, transaction);
  }

  // unlock
  if(res) UnlockAllPage(transaction, Operation::DELETE);
  else {
    UnlockParentPage(leaf_raw_page, transaction, Operation::DELETE);
    UnlockPage(leaf_raw_page, transaction, Operation::DELETE);
  }

  // delete page
  for(auto it = transaction->GetDeletedPageSet()->begin();
  it != transaction->GetDeletedPageSet()->end();
  it++) {
    assert(buffer_pool_manager_->DeletePage(*it));
  }
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  // root
  if(node->IsRootPage()) {
    bool res = AdjustRoot(node);
    switch (res)
    {
    case true:
      transaction->AddIntoDeletedPageSet(node->GetPageId());
    case false:
      return res;
    }
  }

  // get parent page
  int parent_page_id = node->GetParentPageId();
  auto parent_raw_page = buffer_pool_manager_->FetchPage(parent_page_id);
  auto parent_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, 
                                        KeyComparator>*>(parent_raw_page->GetData());
    
  // left brother
  auto idx = parent_page->ValueIndex(node->GetPageId());
  if(idx > 0) {
    // fetch page
    auto lft_bro_page_id = parent_page->ValueAt(idx - 1);
    auto lft_bro_raw_page = buffer_pool_manager_->FetchPage(lft_bro_page_id);
    N* lft_bro_page = reinterpret_cast<N*>(lft_bro_raw_page->GetData());

    if(lft_bro_page->GetSize() > lft_bro_page->GetMinSize()){
      // redistribute
      Redistribute(lft_bro_page, node, idx);
      buffer_pool_manager_->UnpinPage(lft_bro_page_id, true);
      buffer_pool_manager_->UnpinPage(parent_page_id, false);
      return false;
    } else {
      // merge
      Coalesce(node, lft_bro_page, parent_page, idx, transaction);
      buffer_pool_manager_->UnpinPage(lft_bro_page_id, true);
      buffer_pool_manager_->UnpinPage(parent_page_id, false);
      return true;
    }
  }

  // right brother
  if(idx < parent_page->GetMaxSize() - 1) {
    // fetch page
    auto rht_bro_page_id = parent_page->ValueAt(idx + 1);
    auto rht_bro_raw_page = buffer_pool_manager_->FetchPage(rht_bro_page_id);
    N* rht_bro_page = reinterpret_cast<N*>(rht_bro_raw_page->GetData());

    if(rht_bro_page->GetSize() > rht_bro_page->GetMinSize()){
      // redistribute
      Redistribute(rht_bro_page, node, idx);
      buffer_pool_manager_->UnpinPage(rht_bro_page_id, true);
      buffer_pool_manager_->UnpinPage(parent_page_id, false);
      return false;
    } else {
      // merge
      Coalesce(rht_bro_page, node, parent_page, idx, transaction);
      buffer_pool_manager_->UnpinPage(rht_bro_page_id, true);
      buffer_pool_manager_->UnpinPage(parent_page_id, false);
      return true;
    }
  }
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(
    N *&neighbor_node, N *&node,
    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *&parent,
    int index, Transaction *transaction) {
  // merge pages
  node->MoveAllTo(neighbor_node, index, buffer_pool_manager_);

  // delete empty page
  parent->Remove(index);
  transaction->AddIntoDeletedPageSet(node->GetPageId());
  if(parent->GetSize() < parent->GetMinSize()){
    return CoalesceOrRedistribute(parent, transaction);
  }

  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  // get index in parent
  auto parent_page_id = neighbor_node->GetParentPageId();
  auto page = buffer_pool_manager_->FetchPage(parent_page_id);
  assert(page == nullptr);
  auto parent_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, 
                                        KeyComparator>*>(page->GetData());
  auto index_in_parent = parent_page->ValueIndex(neighbor_node->GetPageId());
  buffer_pool_manager_->UnpinPage(parent_page_id, false);

  if(index == 0) 
    neighbor_node->MoveFirstToEndOf(node, buffer_pool_manager_);
  else 
    neighbor_node->MoveLastToFrontOf(node, index_in_parent, buffer_pool_manager_);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  // case 2
  if(old_root_node->IsLeafPage() && old_root_node->GetSize() == 0) {
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(false);
    return true;
  }

  // case 1
  if(old_root_node->GetSize() == 1) {
    // change root
    auto old_root = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, 
                                      KeyComparator>*>(old_root_node);
    root_page_id_ = old_root->ValueAt(0);
    UpdateRootPageId(true);

    // config new root
    auto root_raw_page = buffer_pool_manager_->FetchPage(root_page_id_);
    auto root_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,
                                      KeyComparator>*>(root_raw_page->GetData());
    root_page->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);

    return true;
  }

  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() {
  // get left most page
  KeyType key;
  auto leaf_raw_page = FindLeafPage(key, true, nullptr, Operation::SEARCH);
  B_PLUS_TREE_LEAF_PAGE_TYPE* leaf_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(leaf_raw_page->GetData());
  
  // construct a iterator
  page_id_t page_id = leaf_page->GetPageId();
  int index = 0;
  auto it = INDEXITERATOR_TYPE(page_id, index, buffer_pool_manager_);

  // unlock
  UnlockPage(leaf_raw_page, nullptr, Operation::SEARCH);

  return it;
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  // find leaf page
  auto leaf_raw_page = FindLeafPage(key, false, nullptr, Operation::SEARCH);
  B_PLUS_TREE_LEAF_PAGE_TYPE* leaf_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(leaf_raw_page->GetData());

  // construct iterator
  page_id_t page_id = leaf_page->GetPageId();
  int index = leaf_page->KeyIndex(key, comparator_);
  auto it = INDEXITERATOR_TYPE(page_id, index, buffer_pool_manager_);

  // unlock
  UnlockPage(leaf_raw_page, nullptr, Operation::SEARCH);

  return it;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key,
                                                         bool leftMost, 
                                                         Transaction* txn, 
                                                         Operation op) {
  // std::cout << "start find leaf page" << std::endl;
  // empty
  if(IsEmpty()) return nullptr;

  // check operation
  if(op != Operation::SEARCH)
    // if operation is write, all write latch lock
    LockRoot();
  // std::cout << "start find leaf page" << std::endl;

  // root
  auto root_raw_page = buffer_pool_manager_->FetchPage(root_page_id_);
  LockPage(root_raw_page, txn, op);
  BPlusTreePage* root_page = reinterpret_cast<BPlusTreePage*>(root_raw_page->GetData());

  // search
  auto child_raw_page = root_raw_page;
  BPlusTreePage* cur_page = root_page;
  while(!cur_page->IsLeafPage()){
    std::cout << "not leaf" << std::endl;
    // get child page id
    auto cur_in_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,
                                        KeyComparator>*>(cur_page);
    page_id_t child_page_id;
    if(leftMost)
      child_page_id = cur_in_page->ValueAt(0);
    else
      child_page_id = cur_in_page->Lookup(key, comparator_);
    
    // unping page
    if(txn == nullptr){
      child_raw_page->RUnlatch();  
      buffer_pool_manager_->UnpinPage(cur_in_page->GetPageId(), false);
    }

    // update cur page
    child_raw_page = buffer_pool_manager_->FetchPage(child_page_id);
    LockPage(child_raw_page, txn, op);
    cur_page = reinterpret_cast<BPlusTreePage*>(child_raw_page->GetData());

    if(txn != nullptr){
      if(op==Operation::SEARCH || 
      (op==Operation::INSERT && cur_page->GetSize() < cur_page->GetMaxSize())|| 
      (op==Operation::DELETE && cur_page->GetSize() > cur_page->GetMinSize())){
        // Search, or current page is safe
        UnlockParentPage(child_raw_page, txn, op);
      }
    }
  }

  return child_raw_page;
}


INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::LockPage(Page* page, Transaction* txn, Operation op){
    if(op == Operation::SEARCH){
        page->RLatch();
    }else{
        page->WLatch();
    }
    if(txn != nullptr)
        txn->GetPageSet()->push_back(page);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnlockPage(Page* page, Transaction* txn, Operation op){
    if(page->GetPageId() == root_page_id_){
        UnlockRoot();
    }
    if(op == Operation::SEARCH){
        page->RUnlatch();
        buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    }else{
        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    }
    if(txn != nullptr)
        txn->GetPageSet()->pop_front();
}

/*
 Used while removing element and the target leaf node is deleted 
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnlockAllPage(Transaction* txn, Operation op){
    if(txn == nullptr) return;
    while(!txn->GetPageSet()->empty()){
        auto front = txn->GetPageSet()->front();
        if(front->GetPageId() != INVALID_PAGE_ID){
            if(op == Operation::SEARCH){
                front->RUnlatch();
                buffer_pool_manager_->UnpinPage(front->GetPageId(), false);
            }else{
                if(front->GetPageId() == root_page_id_){
                    UnlockRoot();
                }
                front->WUnlatch(); 
                buffer_pool_manager_->UnpinPage(front->GetPageId(), true);
            }
        }
        txn->GetPageSet()->pop_front();
    }
}

/*
 If current node is safe, all the lock held by parent can be released
 Safe condition:
    Current size < Max Size
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnlockParentPage(Page* page, Transaction* txn, Operation op){
    if(txn == nullptr) return;
    if(txn->GetPageSet()->empty()) return;
    if(page->GetPageId() == INVALID_PAGE_ID){
        UnlockAllPage(txn, op);
    }else{
        while(!txn->GetPageSet()->empty() && txn->GetPageSet()->front()->GetPageId() != page->GetPageId()){
            auto front = txn->GetPageSet()->front();
            if(front->GetPageId() != INVALID_PAGE_ID){
                if(op == Operation::SEARCH){
                    front->RUnlatch();
                    buffer_pool_manager_->UnpinPage(front->GetPageId(), false);
                }else{
                    if(front->GetPageId() == root_page_id_){
                        UnlockRoot();
                    }
                    front->WUnlatch(); 
                    buffer_pool_manager_->UnpinPage(front->GetPageId(), true);
                }
            }
            txn->GetPageSet()->pop_front();
        }
    }
}


/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(
      buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record)
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  else
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for debug only
 * print out whole b+tree sturcture, rank by rank
 */
INDEX_TEMPLATE_ARGUMENTS
std::string BPLUSTREE_TYPE::ToString(bool verbose) {
  if (IsEmpty()) {
    return "Empty tree";
  }
  std::queue<BPlusTreePage *> todo, tmp;
  std::stringstream tree;
  auto node = reinterpret_cast<BPlusTreePage *>(
      buffer_pool_manager_->FetchPage(root_page_id_));
  if (node == nullptr) {
    throw Exception(EXCEPTION_TYPE_INDEX,
                    "all page are pinned while printing");
  }
  todo.push(node);
  bool first = true;
  while (!todo.empty()) {
    node = todo.front();
    if (first) {
      first = false;
      tree << "| ";
    }
    // leaf page, print all key-value pairs
    if (node->IsLeafPage()) {
      auto page = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(node);
      tree << page->ToString(verbose) << "| ";
    } else {
      auto page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(node);
      tree << page->ToString(verbose) << "| ";
      page->QueueUpChildren(&tmp, buffer_pool_manager_);
    }
    todo.pop();
    if (todo.empty() && !tmp.empty()) {
      todo.swap(tmp);
      tree << '\n';
      first = true;
    }
    // unpin node when we are done
    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
  }
  return tree.str();
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name,
                                    Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name,
                                    Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace scudb
