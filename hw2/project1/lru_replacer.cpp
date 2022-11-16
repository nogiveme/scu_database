/**
 * LRU implementation
 */
#include "buffer/lru_replacer.h"
#include "page/page.h"

namespace scudb {

template <typename T> LRUReplacer<T>::LRUReplacer() {
  head = std::make_shared<node>();
  tail = std::make_shared<node>();
  head->next = tail;
  tail->pre = head;
}

template <typename T> LRUReplacer<T>::~LRUReplacer() {}

/*
 * Insert value into LRU
 */
template <typename T> 
void LRUReplacer<T>::Insert(const T &value) {
  std::shared_ptr<node> cur;
  if(hashmap.find(value) != hashmap.end()) {
    cur = hashmap[value];
    std::shared_ptr<node> pre = cur->pre;
    std::shared_ptr<node> next = cur->next;
    pre->next = next;
    next->pre = pre;
  } else {
    cur = std::shared_ptr<node>(value);
  }
  cur->next = head->next;
  head->next->pre = cur;
  cur->pre = head;
  head->next = cur;
}

/* If LRU is non-empty, pop the head member from LRU to argument "value", and
 * return true. If LRU is empty, return false
 */
template <typename T> 
bool LRUReplacer<T>::Victim(T &value) {
  if(!hashmap.empty()){
    std::shared_ptr<node> cur = tail->pre;
    tail->pre = cur->pre;
    cur->pre->next = tail;
    value = cur->value;
    hashmap.erase(value);
    return true;
  }
  return false;
}

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false
 */
template <typename T> bool LRUReplacer<T>::Erase(const T &value) {
  if(hashmap.find(value) != hashmap.end()){
    std::shared_ptr<node> cur = hashmap[value];
    cur->next->pre = cur->pre;
    cur->pre->next = cur->next;
  }
  return hashmap.erase(value);
}

template <typename T> size_t LRUReplacer<T>::Size() { 
  return hashmap.size(); 
}

template class LRUReplacer<Page *>;
// test only
template class LRUReplacer<int>;

} // namespace scudb

