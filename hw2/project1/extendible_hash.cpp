#include <list>

#include "hash/extendible_hash.h"
#include "page/page.h"

namespace scudb {

/*
 * constructor
 * array_size: fixed array size for each bucket
 */
template <typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash(size_t size):
  array_size(size),
  global_dpt(0),
  dir()
  {
    dir.push_back(std::make_shared<bucket>(0));
  }

/*
 * helper function to calculate the hashing address of input key
 */
template <typename K, typename V>
size_t ExtendibleHash<K, V>::HashKey(const K &key) {
  // return the hash value of Key, using the STL hash
  return std::hash<K>{}(key);
}

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetGlobalDepth() const {
  std::lock_guard<std::mutex> lock(latch);
  return global_dpt;
}

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
  if(dir[bucket_id]){
    std::lock_guard<std::mutex> lck(dir[bucket_id]->latch);
    if(dir[bucket_id]->buffer.size() == 0) return -1;
    return dir[bucket_id]->local_dpt;
  }
  return -1;
}

/*
 * helper function to return current number of bucket in hash table
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const {
  std::lock_guard<std::mutex> lock(latch);
  return dir.size();
}

/*
 * lookup function to find value associate with input key
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K &key, V &value) {
  // get hash value of the key
  auto key_hash = HashKey(key);
  // get index from hash value
  size_t index = key_hash & ((1 << global_dpt) - 1);
  // check the validity of the index 
  std::lock_guard<std::mutex> lck(dir[index]->latch);
  if(dir[index]){
    auto iter = dir[index]->buffer.find(key);
    if(iter == dir[index]->buffer.end()) 
      return false;
    else 
      value = iter->second;
    return true;
  }
  return false;
}

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K &key) {
  // check the K
  auto key_hash = HashKey(key);
  size_t index = key_hash & ((1 << global_dpt) - 1);
  std::lock_guard<std::mutex> lck(dir[index]->latch);
  if(dir[index]) {
    auto delete_num = dir[index]->buffer.erase(key);
    return delete_num == 1;
  }
  return false;
}

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
template <typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {
  // get hash value of key
  std::cout << "key is: " << key << std::endl;
  std::cout << "array_size is: " << array_size << std::endl;
  std::cout << dir[0]->local_dpt << std::endl;
  auto key_hash = HashKey(key);
   get index from hash value
  size_t index = key_hash & (key_hash & (1 << global_dpt) - 1);
  std::cout << "index is: " << index << std::endl;

  std::lock_guard<std::mutex> lck(dir[index]->latch);
  std::lock_guard<std::mutex> lck2(latch);
  if(dir[index]->buffer.find(key) != dir[index]->buffer.end()){
    std::cout << "yes 2" << std::endl;
    dir[index]->buffer[key] = value;
  }
  else {
    dir[index]->buffer.insert(std::make_pair(key, value));
    std::cout << "buffer size is: " << dir[index]->buffer.size() << std::endl;
  }
  // check the overflow type
  while(true){
    if(dir[index]->buffer.size() <= array_size) {return ;}
    else if(dir[index]->local_dpt == global_dpt){
      // under this condition, directory expansion is needed as well as bucket splitting
      // directory expansion
      std::cout << "----dir expansion" << std::endl;
      global_dpt++;
      size_t len = dir.size();
      for(int i = 0; i < len; i++)
        dir.push_back(dir[i]);
      std::cout << "then dir size is:" << dir.size() << std::endl;
    }
    std::cout << "global depth is: " << global_dpt << std::endl;
    std::cout << "local depth is: " << dir[index]->local_dpt << std::endl;
    dir[index]->local_dpt++; 
    size_t new_idx_re;
    size_t i = 0;
    bool is_created = false;
    for(auto iter = dir[index]->buffer.begin(); iter != dir[index]->buffer.end(); iter++, i++){
      key_hash = HashKey(iter->first);
      size_t new_idx = key_hash & ((1 << dir[index]->local_dpt) - 1);
      if((dir[new_idx] == dir[index] && index != new_idx) || 
      (i == dir[index]->buffer.size() - 1 && index == new_idx && !is_created)) {
        std::cout << "create a new bucket" << std::endl;
        dir[new_idx_re] = std::make_shared<bucket>(dir[index]->local_dpt);
        if(new_idx != index)
          new_idx_re = new_idx;
        else 
          new_idx_re = (1 << (global_dpt - 1)) + new_idx;
        is_created = true;
      }
      if(new_idx == index) continue;
      dir[new_idx]->buffer.insert(std::make_pair(iter->first, iter->second));
      std::cout << "new index is: " << new_idx << std::endl;
      std::cout << "inserted! dir[new_idx]'s buffer size is: " << dir[new_idx]->buffer.size() << std::endl;
    }
    for(auto iter = dir[new_idx_re]->buffer.begin(); iter != dir[new_idx_re]->buffer.end(); iter++) {
      std::cout << "enter the iter " << std::endl;
      dir[index]->buffer.erase(iter->first);
    }
    std::cout << "end" << std::endl;
    if(dir[index]->buffer.size() <= array_size) {
      std::cout << "return" << std::endl;
      return;
    }
  }
}

template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
} // namespace scudb