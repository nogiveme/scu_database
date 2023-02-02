/**
 * b_plus_tree_leaf_page.cpp
 */

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "page/b_plus_tree_leaf_page.h"
#include "page/b_plus_tree_internal_page.h"

namespace scudb {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetNextPageId(INVALID_PAGE_ID);
  SetMaxSize((PAGE_SIZE - sizeof(BPlusTreeLeafPage)) / sizeof(MappingType));
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const {
  return next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
}

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(
    const KeyType &key, const KeyComparator &comparator) const {
  int size = GetSize();

  // binary search
  int lft = 0, rht = size;
  while(lft < rht) {
    auto mid = (lft + rht) / 2;
    if(comparator(key, array[mid].first) <= 0) rht = mid;
    else lft = mid + 1;
  }
  return lft;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  // detection
  assert(index >= 0 && index < GetMaxSize());
  KeyType key;
  key = array[index].first;
  return key;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  // detection
  assert(index >= 0 && index < GetMaxSize());
  return array[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key,
                                       const ValueType &value,
                                       const KeyComparator &comparator) {
  // empty or bigger than last value in the page
  if (GetSize() == 0 || comparator(key, KeyAt(GetSize() - 1)) > 0) {
    array[GetSize()] = {key, value};
  } else if (comparator(key, array[0].first) < 0) {
    memmove(array + 1, array, static_cast<size_t>(GetSize()*sizeof(MappingType)));
    array[0] = {key, value};
  } else {
    int low = 0, high = GetSize() - 1, mid;
    while (low < high && low + 1 != high) {
      mid = low + (high - low)/2;
      if (comparator(key, array[mid].first) < 0) {
        high = mid;
      } else if (comparator(key, array[mid].first) > 0) {
        low = mid;
      } else {
        // only support unique key
        assert(0);
      }
    }
    memmove(array + high + 1, array + high,
            static_cast<size_t>((GetSize() - high)*sizeof(MappingType)));
    array[high] = {key, value};
  }

  IncreaseSize(1);
  assert(GetSize() <= GetMaxSize());
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(
    BPlusTreeLeafPage *recipient,
    __attribute__((unused)) BufferPoolManager *buffer_pool_manager) {
  // detection
  assert(recipient != nullptr);
  auto size = GetSize();
  assert(size == GetMaxSize());
  
  // copy
  int hf_index = size / 2;
  auto src = array + hf_index;
  size = size % 2 == 0 ? hf_index : hf_index + 1;
  recipient->CopyHalfFrom(src, size);

  // update size
  SetSize(hf_index);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyHalfFrom(MappingType *items, int size) {
  // detection
  int cur_size = GetSize();
  assert(cur_size + size <= GetMaxSize());

  // copy
  memmove(array + cur_size, items, size*sizeof(MappingType));
  
  // update size
  IncreaseSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType &value,
                                        const KeyComparator &comparator) const {
  auto idx = KeyIndex(key,comparator);
  if(comparator(key, array[idx].first) == 0) {
    value = array[idx].second;
  } else {
    auto n_idx = idx - 1;
    if(!(n_idx >= 0 && comparator(key, array[n_idx].first) == 0))  
      return false;
  }
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immdiately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(
    const KeyType &key, const KeyComparator &comparator) {
  ValueType value;
  int size = GetSize();
  if(Lookup(key, value, comparator)) {
    auto idx = KeyIndex(key, comparator);
    memmove(array + idx, array + idx + 1, (size - idx - 1) * sizeof(MappingType));
    IncreaseSize(-1);
  }
  return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update next page id
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient,
                                           int, BufferPoolManager *) {
  // copy
  recipient->CopyAllFrom(array, GetSize());
  
  // update next page id
  recipient->SetNextPageId(GetNextPageId());

  // update size
  SetSize(0);
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyAllFrom(MappingType *items, int size) {
  // detection
  int cur_size = GetSize();
  assert(cur_size + size <= GetMaxSize());
  
  // copy
  memmove(array + cur_size, items, size * sizeof(MappingType));
  
  // update size
  IncreaseSize(size);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeLeafPage *recipient,
    BufferPoolManager *buffer_pool_manager) {
  // detection
  assert(recipient != nullptr);

  // change parent info
  auto parent_page_id = GetParentPageId();
  auto page = buffer_pool_manager->FetchPage(parent_page_id);
  assert(page == nullptr);
  auto parent_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,
                                                    KeyComparator>*>(page->GetData());
  auto index_in_parent = parent_page->ValueIndex(GetPageId());
  parent_page->SetKeyAt(index_in_parent, array[1].first);
  buffer_pool_manager->UnpinPage(parent_page_id, true);

  // copy
  recipient->CopyLastFrom(array[0]);

  // remove the first pair
  memmove(array, array + 1, (GetSize() - 1) * sizeof(MappingType));
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
  // detection
  auto cur_size = GetSize();
  assert(cur_size + 1 <= GetMaxSize());

  // copy
  array[cur_size] = item;

  // update size
  IncreaseSize(1);
}
/*
 * Remove the last key & value pair from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(
    BPlusTreeLeafPage *recipient, int parentIndex,
    BufferPoolManager *buffer_pool_manager) {
  // detection
  int cur_size = GetSize();
  assert(cur_size > GetMinSize());
  assert(recipient != nullptr);

  // copy
  recipient->CopyFirstFrom(array[cur_size - 1], parentIndex, buffer_pool_manager);

  // remove
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(
    const MappingType &item, int parentIndex,
    BufferPoolManager *buffer_pool_manager) {
  // detecttion
  int cur_size = GetSize();
  assert(cur_size + 1 <= GetMaxSize());

  // copy
  memmove(array, array + 1, cur_size * sizeof(MappingType));
  array[0] = item;

  // change parent page info
  auto parent_page_id = GetParentPageId();
  auto page = buffer_pool_manager->FetchPage(parent_page_id);
  auto parent_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,
                                        KeyComparator> *>(page->GetData());
  parent_page->SetKeyAt(parentIndex, item.first);
  buffer_pool_manager->UnpinPage(parent_page_id, true);

  // update size
  IncreaseSize(1);
}

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
std::string B_PLUS_TREE_LEAF_PAGE_TYPE::ToString(bool verbose) const {
  if (GetSize() == 0) {
    return "";
  }
  std::ostringstream stream;
  if (verbose) {
    stream << "[pageId: " << GetPageId() << " parentId: " << GetParentPageId()
           << "]<" << GetSize() << "> ";
  }
  int entry = 0;
  int end = GetSize();
  bool first = true;

  while (entry < end) {
    if (first) {
      first = false;
    } else {
      stream << " ";
    }
    stream << std::dec << array[entry].first;
    if (verbose) {
      stream << "(" << array[entry].second << ")";
    }
    ++entry;
  }
  return stream.str();
}

template class BPlusTreeLeafPage<GenericKey<4>, RID,
                                       GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID,
                                       GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID,
                                       GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID,
                                       GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID,
                                       GenericComparator<64>>;
} // namespace scudb
