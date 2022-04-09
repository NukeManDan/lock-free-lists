
pub mod map;
pub mod set;


pub(crate) type NodeAtm<'g,N> = &'g Atomic<N>;
pub(crate) type LoadedNode<'g,N> = (&'g Atomic<N>,Shared<'g,N>);

// N is any type for your nodes, we want a struct for maps and prim. for sets
// K is the key for seraching and add/reomve items in the ordered list of nodes.
pub(crate) trait Searchable<K> {
	fn get_key(&self) -> &K;
}

pub(crate) fn search<'g,K:Ord,N:Searchable<K>>(head:LoadedNode<'g,N>,tail:LoadedNode<'g,N>,search_key:&K,g:&'g Guard)->(LoadedNode<'g,K>,LoadedNode<'g,K>){

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
            //t = (t_next.0,t_next.1);//with 'unmarked' reference
            if std::ptr::eq(t.1.as_raw(), tail_s.as_raw()){
                break t
            }
            let t_next_atm = unsafe{&t.1.as_ref().unwrap().next};
            t_next = (t_next_atm,t_next_atm.load_consume(g));
            let t_n = unsafe{t.1.as_ref().unwrap()};
            if t_next.1.tag() == 0 && t_n.get_key() >= search_key{
                break t
            }
        };
        if std::ptr::eq(ln_next.1.as_raw(), rn.1.as_raw()){//if adjacent
            if (!std::ptr::eq(rn.1.as_raw(), tail_s.as_raw())) && unsafe{rn.1.as_ref().unwrap().next.load_consume(g).tag() == 1}{
                //if rn is not tail and rn.next is marked -> try again
                //Note: This must short circuit or the unsafe block WILL be unsafe
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
                //Note: This must short circuit or the unsafe block WILL be unsafe
                continue 'search_again;
            }else{
                //else we have finished search 
                //debug_assert!(std::ptr::eq(ln_next.1.as_raw(), rn.1.as_raw()));//(based on the lemmas in the paper, these must be adjacent)
                return (ln,rn)
            }
        }
    }
}