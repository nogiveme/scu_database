/**
 * index_iterator.cpp
 */
#include <cassert>

#include "index/index_iterator.h"

namespace scudb {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(page_id_t page_id, int index, BufferPoolManager* buffer_pool_manager):
    index_(index),
    buffer_pool_manager_(buffer_pool_manager){
        auto leaf_raw_page = buffer_pool_manager_->FetchPage(page_id);
        B_PLUS_TREE_LEAF_PAGE_TYPE* leaf_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(leaf_raw_page->GetData());
        leaf_page_ = leaf_page;
    }

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
    buffer_pool_manager_->UnpinPage(leaf_page_->GetPageId(), false);
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd(){
    // check
    return (leaf_page_->GetNextPageId() == INVALID_PAGE_ID && index_ >= leaf_page_->GetSize());
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType& INDEXITERATOR_TYPE::operator*() {
    // detection
    if(isEnd())
        throw Exception(ExceptionType::EXCEPTION_TYPE_INDEX, "operation *: out of range");

    const MappingType pair = leaf_page_->GetItem(index_);
    return pair;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE& INDEXITERATOR_TYPE::operator++(){
    // not reach tail of current leaf page
    index_++;
    if(index_ >= leaf_page_->GetSize()){
        if(leaf_page_->GetNextPageId() != INVALID_PAGE_ID){
            // unpin page
            buffer_pool_manager_->UnpinPage(leaf_page_->GetPageId(), false);

            // fetch new leaf page
            auto n_leaf_raw_page = buffer_pool_manager_->FetchPage(leaf_page_->GetNextPageId());
            B_PLUS_TREE_LEAF_PAGE_TYPE* n_leaf_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(n_leaf_raw_page->GetData());

            // update iter
            leaf_page_ = n_leaf_page;
            index_ = 0;
        }
    }

    return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;
template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;
template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;
template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace scudb
