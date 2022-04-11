use std::{sync::{atomic::Ordering, Arc}, ops::Deref};

use crossbeam_epoch::{Atomic, Shared, pin, Guard, Owned};
use crate::{Searchable, search};


#[derive(Debug)]
pub struct Node<K>{
    key:Option<K>,
    next:Atomic<Self>,
}

impl<K> Node<K> {
    fn sentinel(tail:Option<Atomic<Node<K>>>) -> Self { Self { key:None, next: tail.unwrap_or(Atomic::null()) } }
}
#[derive(Debug)]
pub struct SetList<K>{
    head:Arc<Atomic<Node<K>>>,
    tail:Arc<Atomic<Node<K>>>
}

impl<K> Clone for SetList<K> {
    fn clone(&self) -> Self {
        Self { head: Arc::clone(&self.head), tail:Arc::clone(&self.tail) }
    }
}

impl<K> SetList<K> {
    pub fn new() -> Self { 
        let tail = Atomic::new(Node::sentinel(None));   
        let head = Atomic::new(Node::sentinel(Some(tail.clone())));
        Self {head:head.into(),tail:tail.into()}
    }
}

impl<K:Ord+Clone> SetList<K> {
    pub fn insert(&self,key:&K)->bool{
        let g = &pin();
        insert(self.head.deref(), self.tail.deref(), key, g)
    }
    pub fn remove(&self,key:&K)->bool{
        let g = &pin();
        remove(self.head.deref(), self.tail.deref(), key, g)
    }
    pub fn find(&self,key:&K)->bool{
        let g = &pin();
        let tail_s = self.tail.load_consume(g);
        let mut rn = (self.tail.deref(),tail_s.clone());
        let sr = search((self.head.deref(),self.head.load_consume(g)), rn, key, g);
        dbg!(self.head.load_consume(g),sr,tail_s);
        rn = sr.1;
        if !std::ptr::eq(rn.1.as_raw(), tail_s.as_raw()) && unsafe{rn.1.as_ref().unwrap().key.as_ref().unwrap() == key}{
            true
        } else {false}
    }
    ///Technically should only be called when no other threads are currently editing the list
    ///Mostly used for testing at the moment, hence the need for the guard and not implementing into iter.
    pub fn iter<'g>(&'g self,g:&'g Guard)->ListIterator<'g,K>{
        ListIterator::new(self.head.deref(), g)
    }
}

// type NodeAtm<'g,K> = &'g Atomic<Node<K>>;
// type LoadedNode<'g,K> = (&'g Atomic<Node<K>>,Shared<'g,Node<K>>);



impl<K> Searchable<K> for Node<K> {
    fn get_key(&self) -> &K {
        self.key.as_ref().expect("Non-sentinal nodes always have `Some` key.")
    }
}

fn remove<'g,K:Ord>(head:NodeAtm<'g,K>,tail:NodeAtm<'g,K>,key:&K,g:&'g Guard)->bool{
    let (rn,rn_next) = loop {
        let tail_s = tail.load_consume(g);
        let ln = (head,head.load_consume(g));
        let mut rn = (tail,tail_s.clone());
        let sr = search(ln, rn, key, g);
        //ln = sr.0;
        rn = sr.1;
        if std::ptr::eq(rn.1.as_raw(), tail_s.as_raw()) || unsafe{rn.1.as_ref().unwrap().key.as_ref().unwrap() != key}{
            return false
        }
        let rn_next_atm = unsafe{&rn.1.as_ref().unwrap().next};
        let rn_next = (rn_next_atm,rn_next_atm.load_consume(g));
        if rn_next.1.tag() == 0 {
            if rn_next.0.compare_exchange(rn_next.1, rn_next.1.with_tag(1), Ordering::SeqCst, Ordering::SeqCst, g).is_ok(){
                //this can still fail, because the outer scope thinks it is unmarked
                //which means if other nodes became marked, our next node, might now be different
                break (rn,rn_next)//we drop the lock, other threads will see 'none' and return None to their callers
            }//else loop
        }//else loop
        
    };
    if rn.0.compare_exchange(rn.1, rn_next.1, Ordering::SeqCst, Ordering::SeqCst, g).is_err(){
        let _sr = search((head,head.load_consume(g)), (tail,tail.load_consume(g)), key, g);//this will remove the marked node if we failed above
        true
    }else{
        true
    }
    
}

///Returns Ok if the value has changed (and the previous value) or Err if the key exists and the old_value == new_value
fn insert<'g,K:Ord+Clone>(head:NodeAtm<'g,K>,tail:NodeAtm<'g,K>,key:&K,g:&'g Guard)->bool{
    'try_insert: loop {
        let tail_s = tail.load_consume(g);
        let ln = (head,head.load_consume(g));
        let mut rn = (tail,tail_s.clone());
        let sr = search(ln, rn, key, g);
        //ln = sr.0;
        rn = sr.1;
        let rn_node = unsafe{rn.1.as_ref().unwrap()};
        if !std::ptr::eq(rn.1.as_raw(), tail_s.as_raw()) && unsafe{rn.1.as_ref().unwrap().key.as_ref().unwrap() == key}{
            //key already exists
            return false
        }
        //key does not exist at this point, we must create a new node and attempt to add it
        debug_assert!(rn_node.key.is_none() || rn_node.key.as_ref().unwrap() > key);
        let next: Atomic<Node<K>> = rn.1.into();
        let new_node = Owned::new(Node{key:Some(key.clone()),next});
        match  rn.0.compare_exchange(rn.1, new_node, Ordering::SeqCst, Ordering::SeqCst, g) {
            Ok(_) => return true,
            Err(_) => {
                continue 'try_insert;
            },
        }
    };
}

pub struct ListIterator<'g,K>{
    a:&'g Atomic<Node<K>>,
    g:&'g Guard
}

impl<'g, K:Clone + Ord> Iterator for ListIterator<'g, K> {
    type Item = K;

    fn next(&mut self) -> Option<Self::Item> {
        let s = self.a.load_consume(self.g);
        if s.is_null(){None}
        else{
            let node = unsafe{s.as_ref().unwrap()};
            if node.key.is_none(){return None}//tail node
            let ret = Some(node.key.as_ref().unwrap().clone());
            self.a = &node.next;
            assert!(self.a.load_consume(self.g).tag() == 0);//cannot be garbage
            ret
        }
    }
}

impl<'g, K> ListIterator<'g, K> {
    pub fn new(atm: &'g Atomic<Node<K>>,g:&'g Guard) -> Self { 
        let s = atm.load_consume(g);
        let node = unsafe{s.as_ref().unwrap()};
        assert!(node.key.is_none());
        Self { a:&node.next,g } 
    }
}

#[cfg(test)]
mod test_super {
    use std::{sync::{Arc, Barrier}, collections::HashSet};

    use crossbeam_epoch::pin;

    use super::*;

    #[test]
    fn set_test_insert() {
        let list = SetList::new();
        let none = list.insert(&1);
        debug_assert!(none);
        debug_assert!(list.find(&1));
    }
    #[test]
    fn set_test_remove() {
        let list = SetList::new();
        let none = list.insert(&1);
        debug_assert!(list.find(&1));
        debug_assert!(none);
        debug_assert!(list.remove(&1));
        debug_assert!(!list.find(&1));
    }
    fn make_keys(num_keys:u32)->Vec<u32>{
        let mut _x = 0u32;
        let mut uniques = HashSet::new();
        let keys:Vec<u32> = (0..num_keys).map(|i|{
            _x = _x.wrapping_mul(i);
            loop {
                match uniques.insert(_x){
                    true => break,
                    false => _x += 1,
                }
            }
            _x
        }).collect();
        keys
    }
    #[test]
    fn set_test_many() {
        let list: SetList<u32> = SetList::new();
        let mut _x = 1u32;
        let keys= make_keys(10_000);
        for k in &keys {
            let _ = list.insert(k);
        }
        for k in &keys {
            let _ = list.remove(k);
        }
    }
    #[test]
    fn set_test_threads() {
        use std::thread;
        let list: SetList<u32> = SetList::new();
        let mut _x = 1u32;
        let keys= make_keys(100_000);
        let no_t = 8;
        //let barrier = Arc::new(Barrier::new(no_t));
        let mut threads = vec![];
        for i in 0..no_t {
            let t = list.clone();
            let t_keys = keys.clone();
            //let c = Arc::clone(&barrier);
            let handler = thread::spawn(move || {
                //c.wait();//maximum contention?
                let mut s = 0;
                for k in t_keys {
                    if i < 4 {
                        let r = t.insert(&k);
                        if r {s += 1}
                    }else{
                        let r = t.remove(&k);
                        if r {s -= 1}
                    }
                }
                s
            });
            threads.push(handler)
        }
        let mut ops = 0;
        for (i,handle) in threads.into_iter().enumerate(){
            let did = handle.join().unwrap();
            ops += did;
            dbg!(i,did);
        }
        dbg!(ops);
        for k in keys {
            let r = list.remove(&k);
            if r {ops -= 1}
        }
        dbg!(ops);
        assert!(ops == 0);
        // let g = &pin();
        // let mut iter = list.iter(g);
        // let mut remaining = 0;
        // for (traversal_node,(k,v)) in iter.enumerate() {
        //     remaining += 1;
        // }
        // assert!(remaining == 0)
    }
    #[test]
    fn set_threaded_inserts() {
        use std::thread;
        let list: SetList<u32> = SetList::new();
        let mut _x = 0u32;
        let mut keys= make_keys(10_000);
        let mut threads = vec![];
        let no_t = 8;
        let barrier = Arc::new(Barrier::new(no_t));
        for _ in 0..no_t {
            let t = list.clone();
            let t_keys = keys.clone();
            let c = Arc::clone(&barrier);
            let handler = thread::spawn(move || {
                let mut s = 0;
                c.wait();//maximum contention?
                for k in t_keys {
                    let r = t.insert(&k);
                    if r {s += 1}
                }
                s
            });
            threads.push(handler)
        }
        let mut ops = 0;
        for (i,handle) in threads.into_iter().enumerate(){
            let did = handle.join().unwrap();
            dbg!(i,did);
            ops += did;
        }
        let g = &pin();
        let iter = list.iter(g);
        let mut missing = vec![];
        for k in iter {
            let index = keys[..].iter().position(|value| value == &k);
            if let Some(index) = index {
                let _ = keys.swap_remove(index);
            }else{
                missing.push(k)
            }
        }
        dbg!(ops);
        dbg!(missing);
        dbg!(&keys);
        assert!(keys.len() == 0,"Some keys did not find inserted!")
    }
    #[test]
    fn set_threaded_remove() {
        use std::thread;
        let list: SetList<u32> = SetList::new();
        let mut _x = 0u32;
        let keys= make_keys(100000);
        for k in &keys {
            let _ = list.insert(k);
        }
        let no_t = 8;
        let mut threads = vec![];
        let barrier = Arc::new(Barrier::new(no_t));
        for thread_no in 0..no_t {
            let t = list.clone();
            let t_keys = keys.clone();
            let c = Arc::clone(&barrier);
            let handler = thread::spawn(move || {
                c.wait();
                let mut s = 0;
                for (i,k) in t_keys.iter().enumerate() {
                    if i == thread_no{ continue;}
                    let r = t.remove(&k);
                    if r {
                        //dbg!((thread_no,k));
                        s += 1
                    }
                }
                s
            });
            threads.push(handler)
        }
        let mut ops = 0;
        for (i,handle) in threads.into_iter().enumerate(){
            let did = handle.join().unwrap();
            dbg!(i,did);
            ops += did;
        }
        let g = &pin();
        let iter = list.iter(g);
        let mut missing = vec![];
        for k in iter {
            missing.push(k)
        }
        dbg!(missing.len());
        assert!(ops == keys.len(),"{}!={}",ops,keys.len())
    }
}