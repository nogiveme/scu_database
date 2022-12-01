/**
 * b_plus_tree_page.h
 *
 * Both internal and leaf page are inherited from this page.
 *
 * It actually serves as a header part for each B+ tree page and
 * contains information shared by both leaf page and internal page.
 *
 * Header format (size in byte, 20 bytes in total):
 * ----------------------------------------------------------------------------
 * | PageType (4) | LSN (4) | CurrentSize (4) | MaxSize (4) |
 * ----------------------------------------------------------------------------
 * | ParentPageId (4) | PageId(4) |
 * ----------------------------------------------------------------------------
 */

#pragma once

#include <cassert>
#include <climits>
#include <cstdlib>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "index/generic_key.h"

namespace scudb {

#define MappingType std::pair<KeyType, ValueType>

#define INDEX_TEMPLATE_ARGUMENTS                                               \
  template <typename KeyType, typename ValueType, typename KeyComparator>

// define page type enum
enum class IndexPageType { INVALID_INDEX_PAGE = 0, LEAF_PAGE, INTERNAL_PAGE };

// Abstract class.
class BPlusTreePage {
public:
  //! \brief check current page wether the leaf page
  bool IsLeafPage() const;

  //! \brief check current page whether the root page
  bool IsRootPage() const;

  //! \brief set this page's type
  //! \param page_type the type of this page in the emun
  void SetPageType(IndexPageType page_type);

  //! \brief get the numbers of key & value of this page
  int GetSize() const;

  //! \brief set the numbers of key & value pairs of this page
  //! \param size the numbers of key & value
  void SetSize(int size);
  //! 
  void IncreaseSize(int amount);

  //! \brief get the max numbers of key & value pairs of current page
  int GetMaxSize() const;

  //! \brief set the max key & value numbers of current page
  //! \param max_size the maxmum numbers of key & value numbers of page
  void SetMaxSize(int max_size);

  //! \brief get the minimum size of current page
  int GetMinSize() const;

  //! \brief get the parent's page id
  page_id_t GetParentPageId() const;

  //! \brief set current page's parent page's id
  //! \param parent_page_id the page id that set to the parent page
  void SetParentPageId(page_id_t parent_page_id);

  //! \brief get page id of this page
  page_id_t GetPageId() const;

  //! \brief set the page id
  //! \param page_id page id that will be set
  void SetPageId(page_id_t page_id);

  //! \brief set the log sequence number
  //! \param lsn the log sequence number
  void SetLSN(lsn_t lsn = INVALID_LSN);

private:
  // member variable, attributes that both internal and leaf page share
  IndexPageType page_type_; //!< type of the page
  lsn_t lsn_;               //!< log sequence number
  int size_;                //!< numbers of key & value pairs in current page
  int max_size_;            //!< max numbers of key & value pairs in current page
  page_id_t parent_page_id_;//!< the parent page id of this page 
  page_id_t page_id_;       //!< the page id of this page
};

} // namespace scudb
