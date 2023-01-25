/**
 * b_plus_tree_internal_page.cpp
 */
#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "page/b_plus_tree_internal_page.h"

namespace scudb {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id,
                                          page_id_t parent_id) 
{
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetSize(0);
  SetPageType(IndexPageType::INTERNAL_PAGE);
  // why the BPlusTreeInternalPage is stored in the page
  SetMaxSize((PAGE_SIZE - sizeof(BPlusTreeInternalPage)) / sizeof(MappingType) - 1);                            
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  assert(index > 0 && index < GetMaxSize());
  KeyType key;
  key = array[index].first;
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  assert(index > 0 && index < GetMaxSize());
  array[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  int size = sizeof(array) / sizeof(MappingType);
  for(int i = 0; i < size; i++) {
    if(array[i].second == value)
      return i;
  }
  return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
  assert(index >= 0 && index < GetMaxSize());
  return array[index].second;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType
B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key,
                                       const KeyComparator &comparator) const {
  assert(GetSize() > 1);
  int lft = 0, rht = GetSize() - 1;
  // binary search
  while(lft < rht) {
    int mid = (lft + rht) / 2;
    auto res = comparator(key, array[mid].first);
    if(res < 0) {
      rht = mid - 1;
    } else if(res > 0) {
      lft = mid + 1;
    } else {
      return array[mid].second;
    }
  }
  return -1;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value) {
  array[0].second = old_value;
  array[1].first = new_key;
  array[1].second = new_value;
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value) {
  int index = ValueIndex(old_value);
  memmove(array + index + 1, array + index, (GetSize() - index)*(sizeof(MappingType)));
  array[index].first = new_key;
  array[index].second = new_value;
  IncreaseSize(1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager) {
  // detection
  assert(recipient != nullptr);
  assert(GetSize() == GetMaxSize() + 1);

  // copy from original page
  int hf_index = GetSize() / 2;
  int end = GetMaxSize() + 1;
  recipient->CopyHalfFrom(array + hf_index, end - hf_index, buffer_pool_manager);

  // change child info
  for(int i = hf_index; i < end; i++) {
    auto child_raw_page = buffer_pool_manager->FetchPage(array[i].second);
    auto child_page = reinterpret_cast<BPlusTreePage*>(child_raw_page->GetData());
    child_page->SetParentPageId(recipient->GetPageId());
    buffer_pool_manager->UnpinPage(array[i].second, true);
  }

  // update size
  SetSize(hf_index);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyHalfFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  // detection
  int cur_size = GetSize();
  assert(cur_size + size <= GetMaxSize());
  
  // copy from items
  memmove(array + cur_size, items, size * (sizeof(MappingType)));
  
  // update size
  IncreaseSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  // detection
  assert(index < GetMaxSize() && index >= 0);

  // remove pair from the page
  auto size = GetSize() - index - 1;
  memmove(array + index, array + index + 1, size * sizeof(MappingType));

  // update size
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  auto res = ValueAt(0);
  IncreaseSize(-1);
  assert(GetSize() == 0);
  return res;
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(
    BPlusTreeInternalPage *recipient, int index_in_parent,
    BufferPoolManager *buffer_pool_manager) {
  // fetch the parent page
  auto page = buffer_pool_manager->FetchPage(GetParentPageId());
  BPlusTreeInternalPage* parent = reinterpret_cast<BPlusTreeInternalPage*>(page->GetData());
  SetKeyAt(0, parent->KeyAt(index_in_parent)); // not understand
  buffer_pool_manager->UnpinPage(parent->GetPageId(), false);
  
  // copy from the original page
  int size = GetSize();
  recipient->CopyAllFrom(array, size, buffer_pool_manager);

  // update size
  SetSize(0);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyAllFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  int cur_size = GetSize();
  assert(cur_size + size <= GetMaxSize());
  memmove(array + cur_size, items, size*sizeof(MappingType));
  //change the childs
  for(int i = 0; i < size; i++){
    auto child_raw_page = buffer_pool_manager->FetchPage(items[i].second);
    assert(child_raw_page != nullptr);
    BPlusTreeInternalPage* child_page = reinterpret_cast<BPlusTreeInternalPage*>(child_raw_page->GetData());
    child_page->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(items[i].second, true);
  }
  IncreaseSize(size);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient"
 * page, then update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager) {
  // detection
  int size = GetSize();
  assert(size > GetMinSize());
  
  // change parent page info
  auto parent_page_id = GetParentPageId();
  auto page = buffer_pool_manager->FetchPage(parent_page_id);
  BPlusTreeInternalPage* parent_page = reinterpret_cast<BPlusTreeInternalPage*>(page->GetData());
  auto index_in_parent = parent_page->ValueIndex(GetPageId());
  parent_page->SetKeyAt(index_in_parent, array[1].first);
  buffer_pool_manager->UnpinPage(parent_page_id, true);

  // copy last from this page
  recipient->CopyLastFrom(array[0], buffer_pool_manager);

  // remove first pair from this page
  Remove(0);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(
    const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  // copy
  int size = GetSize();
  array[size] = pair;

  // change child page info
  auto child_raw_page = buffer_pool_manager->FetchPage(pair.second);
  BPlusTreeInternalPage* child_page = reinterpret_cast<BPlusTreeInternalPage*>(child_raw_page->GetData());
  child_page->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(pair.second, true);

  // update size
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to head of "recipient"
 * page, then update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(
    BPlusTreeInternalPage *recipient, int parent_index,
    BufferPoolManager *buffer_pool_manager) {
  // copy
  int end_index = GetSize() - 1;
  recipient->CopyFirstFrom(array[end_index], parent_index, buffer_pool_manager);

  // remove last pair
  Remove(end_index);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(
    const MappingType &pair, int parent_index,
    BufferPoolManager *buffer_pool_manager) {
  // replace first pair
  memmove(array + 1, array, GetSize() * sizeof(MappingType));
  array[0] = pair;

  // change the child info
  auto child_raw_page = buffer_pool_manager->FetchPage(pair.second);
  BPlusTreeInternalPage* child_page = reinterpret_cast<BPlusTreeInternalPage*>(child_raw_page->GetData());
  child_page->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(pair.second, true);

  // change the parent info
  auto parent_raw_page = buffer_pool_manager->FetchPage(GetParentPageId());
  BPlusTreeInternalPage* parent_page = reinterpret_cast<BPlusTreeInternalPage*>(parent_raw_page->GetData());
  parent_page->SetKeyAt(parent_index, pair.first);
  buffer_pool_manager->UnpinPage(GetParentPageId(), true);

  // update size
  IncreaseSize(1); 
}

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::QueueUpChildren(
    std::queue<BPlusTreePage *> *queue,
    BufferPoolManager *buffer_pool_manager) {
  for (int i = 0; i < GetSize(); i++) {
    auto *page = buffer_pool_manager->FetchPage(array[i].second);
    if (page == nullptr)
      throw Exception(EXCEPTION_TYPE_INDEX,
                      "all page are pinned while printing");
    BPlusTreePage *node =
        reinterpret_cast<BPlusTreePage *>(page->GetData());
    queue->push(node);
  }
}

INDEX_TEMPLATE_ARGUMENTS
std::string B_PLUS_TREE_INTERNAL_PAGE_TYPE::ToString(bool verbose) const {
  if (GetSize() == 0) {
    return "";
  }
  std::ostringstream os;
  if (verbose) {
    os << "[pageId: " << GetPageId() << " parentId: " << GetParentPageId()
       << "]<" << GetSize() << "> ";
  }

  int entry = verbose ? 0 : 1;
  int end = GetSize();
  bool first = true;
  while (entry < end) {
    if (first) {
      first = false;
    } else {
      os << " ";
    }
    os << std::dec << array[entry].first.ToString();
    if (verbose) {
      os << "(" << array[entry].second << ")";
    }
    ++entry;
  }
  return os.str();
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t,
                                           GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t,
                                           GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t,
                                           GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t,
                                           GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t,
                                           GenericComparator<64>>;
} // namespace scudb
