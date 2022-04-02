use std::{sync::{atomic::Ordering, Arc}, ops::Deref};

use crossbeam_epoch::{Atomic, Shared, pin, Guard, Owned};
use parking_lot::RwLock;


//pub mod search;


#[derive(Debug)]
pub struct Node<K,V>{
    key:Option<K>,
    value:RwLock<Option<V>>,
    next:Atomic<Self>,
}

impl<K, V> Node<K, V> {
    fn sentinel(tail:Option<Atomic<Node<K,V>>>) -> Self { Self { key:None, value:RwLock::new(None), next: tail.unwrap_or(Atomic::null()) } }
}
pub struct CompareAndSwapError<'a,V>{
    current:Option<V>,
    proposed:Option<&'a V>,
}
#[derive(Debug)]
pub struct List<K,V>{
    head:Arc<Atomic<Node<K,V>>>,
    tail:Arc<Atomic<Node<K,V>>>
}

impl<K, V> Clone for List<K, V> {
    fn clone(&self) -> Self {
        Self { head: Arc::clone(&self.head), tail:Arc::clone(&self.tail) }
    }
}

impl<K, V> List<K, V> {
    pub fn new() -> Self { 
        let tail = Atomic::new(Node::sentinel(None));   
        let head = Atomic::new(Node::sentinel(Some(tail.clone())));
        Self {head:head.into(),tail:tail.into()}
    }
}

impl<K:Ord+Clone, V:Eq + Clone> List<K, V> {
    pub fn insert(&self,key:&K,value:V)->Result<Option<V>,()>{
        let g = &pin();
        match modify(self.head.deref(), self.tail.deref(), key, Some(value), None, g){
            ModifyRes::Success { old_value } => Ok(old_value),
            ModifyRes::NoOp => Err(()),
            ModifyRes::MisMatch(_) => unreachable!(),
        }
    }
    pub fn remove(&self,key:&K)->Option<V>{
        let g = &pin();
        match modify(self.head.deref(), self.tail.deref(), key, None, None, g){
            ModifyRes::Success { old_value } => old_value,
            ModifyRes::NoOp => None,
            ModifyRes::MisMatch(_) => unreachable!(),
        }
    }
    pub fn get(&self,key:&K)->Option<V>{
        let g = &pin();
        let tail_s = self.tail.load_consume(g);
        let mut rn = (self.tail.deref(),tail_s.clone());
        let sr = search((self.head.deref(),self.head.load_consume(g)), rn, key, g);
        dbg!(self.head.load_consume(g),sr,tail_s);
        rn = sr.1;
        if !std::ptr::eq(rn.1.as_raw(), tail_s.as_raw()) && unsafe{rn.1.as_ref().unwrap().key.as_ref().unwrap() == key}{
            let rn_node = unsafe{rn.1.as_ref().unwrap()};
            rn_node.value.read().deref().clone()
        } else {None}
    }
    pub fn compare_and_swap<'a>(&self,key:&K,value:Option<V>,expected:Option<&'a V>)->ModifyRes<'a,V>{
        let g = &pin();
        modify(self.head.deref(), self.tail.deref(), key, value, Some(expected), g)
    }
    ///Technically should only be called when no other threads are currently editing the list
    ///Mostly used for testing at the moment, hence the need for the guard and not implementing into iter.
    pub fn iter<'g>(&'g self,g:&'g Guard)->ListIterator<'g,K,V>{
        ListIterator::new(self.head.deref(), g)
    }
}

type NodeAtm<'g,K,V> = &'g Atomic<Node<K,V>>;
type LoadedNode<'g,K,V> = (&'g Atomic<Node<K,V>>,Shared<'g,Node<K,V>>);

fn search<'g,K:Ord,V>(head:LoadedNode<'g,K,V>,tail:LoadedNode<'g,K,V>,search_key:&K,g:&'g Guard)->(LoadedNode<'g,K,V>,LoadedNode<'g,K,V>){
    'search_again: loop {
        let tail_s = tail.1.clone();
        let mut ln = head;
        let mut ln_next = tail;
        let mut t = ln.clone();
        let t_next_atm = unsafe{&t.1.as_ref().unwrap().next};
        let mut t_next = (t_next_atm,t_next_atm.load_consume(g));
        let rn = loop {//find active nodes
            if t_next.1.tag() == 0 {
                ln = t;
                ln_next = t_next
            }
            t = (t_next.0,t_next.1.with_tag(0));//with 'unmarked' reference
            if std::ptr::eq(t.1.as_raw(), tail_s.as_raw()){
                break t
            }
            let t_next_atm = unsafe{&t.1.as_ref().unwrap().next};
            t_next = (t_next_atm,t_next_atm.load_consume(g));
            let t_n = unsafe{t.1.as_ref().unwrap()};
            if t_next.1.tag() == 0 && t_n.key.as_ref().unwrap() >= search_key{
                break t
            }
        };
        if std::ptr::eq(ln_next.1.as_raw(), rn.1.as_raw()){//if adjacent
            if (!std::ptr::eq(rn.1.as_raw(), tail_s.as_raw())) && unsafe{rn.1.as_ref().unwrap().next.load_consume(g).tag() == 1}{
                //if rn is not tail and rn.next is marked -> try again
                continue 'search_again;
            }else{
                //else we have finished search
                return (ln,rn)
            }
        }
        if ln_next.0.compare_exchange(ln_next.1, rn.1, Ordering::SeqCst, Ordering::SeqCst, g).is_ok(){
            unsafe{g.defer_destroy(ln_next.1)}//safe because we just took it out of the list by winning the exchange
            if (!std::ptr::eq(rn.1.as_raw(), tail_s.as_raw())) && unsafe{rn.1.as_ref().unwrap().next.load_consume(g).tag() == 1}{
                //if rn is not tail and rn.next is marked -> try again
                continue 'search_again;
            }else{
                //else we have finished search 
                return (ln,rn)
            }
        }
    }
}

pub enum ModifyRes<'a, V>{
    Success{old_value:Option<V>},
    MisMatch(CompareAndSwapError<'a,V>),
    NoOp
}

impl<'a, V> ModifyRes<'a, V> {
    pub fn is_ok(&self)->bool{
        match self {
            ModifyRes::Success { .. } => true,
            _ => false,
        }
    }
}

///Requires V to be clone, since we cannot return a lifetime outside of the guard, so we can only convey what it currently is by cloning and returning owned in the CASError
///If expected is None, it does not check the value before performing operation (except equality, if == returns NoOp) (value.is_some() = insert/update; value.is_none() = remove key)
fn modify<'a,'g,K:Ord+Clone,V:Eq+Clone>(head:NodeAtm<'g,K,V>,tail:NodeAtm<'g,K,V>,key:&K,mut value:Option<V>,expected:Option<Option<&'a V>>,g:&'g Guard)->ModifyRes<'a,V>{
    debug_assert!(expected.is_none() || Some(value.as_ref()) != expected, "Must be trying to modify the tree!");
    let (rn,rn_next,ret_value) = loop {
        //handle search
        let tail_s = tail.load_consume(g);
        let mut rn = (tail,tail_s.clone());
        let sr = search((head,head.load_consume(g)), rn, key, g);
        rn = sr.1;
        //handle the insert modify cases
        if value.is_some(){
            //if this is an insert op
            let rn_node = unsafe{rn.1.as_ref().unwrap()};
            let already_exists = !std::ptr::eq(rn.1.as_raw(), tail_s.as_raw()) && unsafe{rn.1.as_ref().unwrap().key.as_ref().unwrap() == key};
            let mut value_lock = rn_node.value.write();
            if value_lock.is_none() && already_exists {
                //someone else deleted this, retry
                continue;
            }
            let expects_none = expected == Some(None);
            let expected_none_was_some = expected.is_some() && already_exists && expects_none;
            let expected_some_was_none = expected.is_some() && !already_exists && expected.as_ref().unwrap().is_some();

            if expected_none_was_some || expected_some_was_none{
                //high order mis match
                return ModifyRes::MisMatch(CompareAndSwapError{proposed:None,current:value_lock.as_ref().cloned()})
            }else if already_exists{ //handle the exists cases
                match expected {
                    Some(ex_v) if ex_v == value_lock.as_ref() => {
                        debug_assert!(ex_v.is_some());
                        debug_assert!(value_lock.is_some());//we already bailed if is was none
                        //same do update
                        return ModifyRes::Success { old_value: value_lock.replace(value.unwrap())}
                    },
                    None if value_lock.as_ref() != value.as_ref() => {
                        //no expected, and update will change the value
                        return ModifyRes::Success { old_value: value_lock.replace(value.unwrap())}
                        
                    },
                    Some(proposed) => {
                        //value mismatch
                        return ModifyRes::MisMatch(CompareAndSwapError{proposed,current:value_lock.as_ref().cloned()})
                    },
                    None => {
                        //don't update, 
                        return ModifyRes::NoOp
                    }
                }
            }else if expected.is_none() || expected == Some(None){
                //update, no key in list
                //try to update the list and add the value
                let next: Atomic<Node<K,V>> = rn.1.into();
                let new_node = Owned::new(Node{key:Some(key.clone()),value:RwLock::new(value),next});
                match  rn.0.compare_exchange(rn.1, new_node, Ordering::SeqCst, Ordering::SeqCst, g) {
                    Ok(_) => return ModifyRes::Success { old_value: None},
                    Err(mut err) => {
                        value = Some(err.new.as_mut().value.get_mut().take().unwrap());//reclaim ownership of the value to avoid cloning
                        continue;
                    },
                }
            }else{//make sure we didn't miss anything
                unreachable!()
            }
            
        }
        //handle the modify remove cases
        debug_assert!(value.is_none());//if we are this far, we are trying to remove the key
        if std::ptr::eq(rn.1.as_raw(), tail_s.as_raw()) || unsafe{rn.1.as_ref().unwrap().key.as_ref().unwrap() != key}{
            //the key isn't in the list, early return
            return ModifyRes::NoOp
        }
        let rn_node = unsafe{rn.1.as_ref().unwrap()};
        //start added code for map impl
        let mut value_lock = rn_node.value.write();
        //we have exclusive owner ship of the value
        let rn_next_atm = unsafe{&rn.1.as_ref().unwrap().next};
        if value_lock.is_none() {
            //someone else deleted it before us, but after we read the tag
            debug_assert!(rn_next_atm.load_consume(g).tag() == 1);//the tag SHOULD be marked now
            return ModifyRes::NoOp //nothing to do, it would have returned none earlier if this was a set and not a map
        }
        //end added code for map impl
        let rn_next = (rn_next_atm,rn_next_atm.load_consume(g));
        //the following assertion is a slight change since we have another synchronization primitive in here.
        //because we own exclusive access to value, no other remove operation can mark the next node before us
        //so we remove the if statement, since the value_lock.is_none() does that for us.
        debug_assert!(rn_next.1.tag() == 0);
        if rn_next.0.compare_exchange(rn_next.1, rn_next.1.with_tag(1), Ordering::SeqCst, Ordering::SeqCst, g).is_ok(){
            //this can still fail, because the outer scope thinks it is unmarked
            //which means if other nodes became marked, our next node, might now be different
            break (rn,rn_next,value_lock.take())//we drop the lock, other threads will see 'none' and return NoOp to their callers (if remove, or re-insert key if insert)
        }//else try again (continue looping)
    };
    //try to do the garbage collection now, saving a search iteration.
    if rn.0.compare_exchange(rn.1, rn_next.1, Ordering::SeqCst, Ordering::SeqCst, g).is_err(){
        //this additional search keeps the constraint that each operation can make up to 2 changes, so far we only made one (marking the node)
        //this second call will gurantee that the marked node is removed. Without this, there is a possibility for it to NOT get removed by another thread
        let _ = search((head,head.load_consume(g)), (tail,tail.load_consume(g)), key, g);//this WILL remove the marked node if we failed above
        ModifyRes::Success { old_value: ret_value}
    }else{
        unsafe{g.defer_destroy(rn.1)}//safe because we just took it out of the list by winning the exchange
        ModifyRes::Success { old_value: ret_value}
    }
}


//Really only 'safe' if the list is not shared amongst threads, no guards made
//Really just made this to help with testing
pub struct ListIterator<'g,K,V>{
    a:&'g Atomic<Node<K,V>>,
    g:&'g Guard
}

impl<'g, K:Clone + Ord, V:Clone> Iterator for ListIterator<'g, K, V> {
    type Item = (K,V);

    fn next(&mut self) -> Option<Self::Item> {
        let s = self.a.load_consume(self.g);
        if s.is_null(){None}
        else{
            let node = unsafe{s.as_ref().unwrap()};
            if node.key.is_none(){return None}//tail node
            let ret = Some((node.key.as_ref().unwrap().clone(),node.value.read().clone().unwrap()));
            self.a = &node.next;
            assert!(self.a.load_consume(self.g).tag() == 0);//cannot be garbage
            ret
        }
    }
}

impl<'g, K, V> ListIterator<'g, K, V> {
    pub fn new(atm: &'g Atomic<Node<K,V>>,g:&'g Guard) -> Self { 
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
    fn map_test_insert() {
        let list = List::new();
        let none = list.insert(&1, true);
        debug_assert!(none.is_ok());
        debug_assert!(list.get(&1).is_some());
    }
    #[test]
    fn map_test_remove() {
        let list = List::new();
        let none = list.insert(&1, true);
        debug_assert!(list.get(&1).is_some());
        debug_assert!(none.is_ok());
        debug_assert!(list.remove(&1).is_some());
        debug_assert!(list.get(&1).is_none());
    }
    #[test]
    fn map_test_cas() {
        let list = List::new();
        let none = list.insert(&1, true);
        debug_assert!(none.is_ok());
        debug_assert!(!list.compare_and_swap(&1, Some(false),None).is_ok());
        debug_assert!(list.compare_and_swap(&1, Some(false),Some(&true)).is_ok());
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
    fn map_test_many() {
        let list: List<u32,bool> = List::new();
        let mut _x = 1u32;
        let keys= make_keys(10_000);
        for k in &keys {
            let _ = list.insert(k, true);
        }
        for k in &keys {
            let _ = list.remove(k);
        }
    }
    #[test]
    fn map_test_threads() {
        use std::thread;
        let list: List<u32,bool> = List::new();
        let mut _x = 1u32;
        let keys= make_keys(100_000);
        let no_t = 8;
        let barrier = Arc::new(Barrier::new(no_t));
        let mut threads = vec![];
        for i in 0..no_t {
            let t = list.clone();
            let t_keys = keys.clone();
            let c = Arc::clone(&barrier);
            let handler = thread::spawn(move || {
                c.wait();//maximum contention?
                let mut s = 0;
                for k in t_keys {
                    if i < 4 {
                        let r = t.compare_and_swap(&k, Some(true),None);
                        if r.is_ok() {s += 1}
                    }else{
                        let r = t.compare_and_swap(&k, None,Some(&true));
                        if r.is_ok() {s -= 1}
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
            let r = list.compare_and_swap(&k, None,Some(&true));
            if r.is_ok() {ops -= 1}
        }
        dbg!(ops);
        assert!(ops == 0);
    }
    #[test]
    fn map_threaded_inserts() {
        use std::thread;
        let list: List<u32,bool> = List::new();
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
                    let r = t.compare_and_swap(&k, Some(true),None);
                    if r.is_ok() {s += 1}
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
        for (k,_) in iter {
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
        assert!(keys.len() == 0,"Some keys did not get inserted!")
    }
    #[test]
    fn map_threaded_remove() {
        use std::thread;
        let list: List<u32,bool> = List::new();
        let mut _x = 0u32;
        let keys= make_keys(100_000);
        for k in &keys {
            let _ = list.insert(k, true);
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
                    let r = t.compare_and_swap(&k, None,Some(&true));
                    if r.is_ok() {
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
        for (k,_) in iter {
            missing.push(k)
        }
        dbg!(missing.len());
        assert!(ops == keys.len(),"{}!={}",ops,keys.len())
    }
}