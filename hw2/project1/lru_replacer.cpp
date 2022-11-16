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
  std::lock_guard<std::mutex> lck(latch);
  std::shared_ptr<node> cur;
  if(hashmap.find(value) != hashmap.end()) {
    cur = hashmap[value];
    std::shared_ptr<node> pre = cur->pre;
    std::shared_ptr<node> next = cur->next;
    pre->next = next;
    next->pre = pre;
  } else {
    cur = std::make_shared<node>(value);
    hashmap.insert(std::make_pair(value, cur));
  }
  cur->next = head->next;
  head->next->pre = cur;
  cur->pre = head;
  head->next = cur;
  std::cout << "insert 1 elem, and the size is: " << hashmap.size() << std::endl;
}

/* If LRU is non-empty, pop the head member from LRU to argument "value", and
 * return true. If LRU is empty, return false
 */
template <typename T> 
bool LRUReplacer<T>::Victim(T &value) {
  std::lock_guard<std::mutex> lck(latch);
  if(!hashmap.empty()){
    std::shared_ptr<node> cur = tail->pre;
    tail->pre = cur->pre;
    cur->pre->next = tail;
    value = cur->value;
    hashmap.erase(value);
    std::cout << "find and erase the victim, the hash map size is " << hashmap.size() << std::endl;
    return true;
  }
  std::cout << "hash map is empty, so can't victim" << std::endl;
  return false;
}

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false
 */
template <typename T> bool LRUReplacer<T>::Erase(const T &value) {
  std::lock_guard<std::mutex> lck(latch);
  std::cout << "try to erase-----" << std::endl;
  if(hashmap.find(value) != hashmap.end()){
    std::shared_ptr<node> cur = hashmap[value];
    cur->next->pre = cur->pre;
    cur->pre->next = cur->next;
  }
  return hashmap.erase(value);
}

template <typename T> size_t LRUReplacer<T>::Size() { 
  std::lock_guard<std::mutex> lck(latch);
  return hashmap.size(); 
}

template class LRUReplacer<Page *>;
// test only
template class LRUReplacer<int>;

} // namespace scudb

