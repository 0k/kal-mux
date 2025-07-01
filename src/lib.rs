#![doc = include_str!(concat!(env!("OUT_DIR"), "/README.md"))]

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use futures::Stream;

pub fn mux_by<S, T, F>(streams: Vec<S>, cmp: F) -> MuxBy<S, T, F>
where
    S: Stream<Item = T> + Unpin,
    F: Fn(&T, &T) -> Ordering + Unpin + Sync + 'static,
{
    let n = streams.len();
    MuxBy {
        streams,
        cmp: Arc::new(cmp),
        heap: BinaryHeap::new(),
        in_heap: vec![false; n],
        done: vec![false; n],
    }
}

pub struct MuxBy<S, T, F>
where
    S: Stream<Item = T> + Unpin,
    F: Fn(&T, &T) -> Ordering + Unpin + Sync + 'static,
{
    streams: Vec<S>,
    cmp: Arc<F>,
    heap: BinaryHeap<HeapItem<T, F>>,
    in_heap: Vec<bool>,
    done: Vec<bool>,
}

struct HeapItem<T, F>
where
    F: Fn(&T, &T) -> Ordering + Unpin + Sync + 'static,
{
    value: T,
    src: usize,
    cmp: Arc<F>,
}

impl<T, F> PartialEq for HeapItem<T, F>
where
    F: Fn(&T, &T) -> Ordering + Unpin + Sync + 'static,
{
    // This code is never called by the heap, but we need to implement
    // it to satisfy the trait.
    fn eq(&self, other: &Self) -> bool {
        (self.cmp)(&self.value, &other.value) == Ordering::Equal && self.src == other.src
    }
}

impl<T, F> Eq for HeapItem<T, F>
where
    F: Fn(&T, &T) -> Ordering + Unpin + Sync + 'static,
{}


impl<T, F> PartialOrd for HeapItem<T, F>
where
    F: Fn(&T, &T) -> Ordering + Unpin + Sync + 'static,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}


impl<T, F> Ord for HeapItem<T, F>
where F: Fn(&T,&T)->Ordering + Unpin + Sync + 'static
{
    fn cmp(&self, other: &Self) -> Ordering {
        (self.cmp)(&self.value, &other.value)
            .reverse()
            .then_with(|| self.src.cmp(&other.src).reverse())
    }
}

impl<S, T, F> Stream for MuxBy<S, T, F>
where
    S: Stream<Item = T> + Unpin,
    F: Fn(&T, &T) -> Ordering + Unpin + Sync + 'static,
    T: Unpin,

{
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        // Poll each stream to fill the heap with the next item if it's empty.
        for (idx, stream) in this.streams.iter_mut().enumerate() {
            if !this.in_heap[idx] && !this.done[idx] {
                match Pin::new(stream).poll_next(cx) {
                    Poll::Ready(Some(item)) => {
                        this.heap.push(HeapItem {
                            value: item,
                            src: idx,
                            cmp: this.cmp.clone(),
                        });
                        this.in_heap[idx] = true;
                    }
                    Poll::Ready(None) => {  // Stream is exhausted
                        this.done[idx] = true;
                    }
                    Poll::Pending => {}     // Stream is not ready
                }
            }
        }

        let alive = this.done.iter().filter(|&&d| !d).count();
        if alive == 0 {
            return Poll::Ready(None);
        }
        let heads = this.in_heap.iter().zip(&this.done)
            .filter(|&(&in_h, &d)| in_h && !d).count();
        if heads < alive {
            return Poll::Pending;
        }

        debug_assert_eq!(this.heap.len(), heads, "heap/in_heap mismatch");
        if let Some(HeapItem { value, src, .. }) = this.heap.pop() {
            this.in_heap[src] = false;
            return Poll::Ready(Some(value))
        }
        // unreachable
        return Poll::Pending;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{stream, StreamExt};

    #[tokio::test(flavor = "current_thread")]
    async fn merge_two_streams_ascending() {
        let s1 = stream::iter(vec![1, 3, 5]);
        let s2 = stream::iter(vec![2, 4, 6]);

        let merged = mux_by(vec![s1, s2], |a: &i32, b: &i32| a.cmp(b));
        let out: Vec<_> = merged.collect().await;

        assert_eq!(out, vec![1, 2, 3, 4, 5, 6]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn merge_three_streams_ascending() {
        let s1 = stream::iter(vec![1, 10, 11]);
        let s2 = stream::iter(vec![2, 3, 12]);
        let s3 = stream::iter(vec![4, 5, 6, 13]);

        let merged = mux_by(vec![s1, s2, s3], |a: &i32, b: &i32| a.cmp(b));
        let out: Vec<_> = merged.collect().await;

        assert_eq!(out, vec![1, 2, 3, 4, 5, 6, 10, 11, 12, 13]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn merge_descending_order() {
        // Each source individually descending; overall merge should be descending.
        let s1 = stream::iter(vec![9, 7, 5]);
        let s2 = stream::iter(vec![8, 6, 0]);

        let merged = mux_by(vec![s1, s2], |a: &i32, b: &i32| b.cmp(a)); // desc
        let out: Vec<_> = merged.collect().await;

        assert_eq!(out, vec![9, 8, 7, 6, 5, 0]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn handles_empty_streams() {
        let s1 = stream::iter(Vec::<i32>::new());
        let s2 = stream::iter(vec![1, 2, 3]);
        let s3 = stream::iter(Vec::<i32>::new());

        let merged = mux_by(vec![s1, s2, s3], |a: &i32, b: &i32| a.cmp(b));
        let out: Vec<_> = merged.collect().await;

        assert_eq!(out, vec![1, 2, 3]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn handles_all_empty() {
        let merged = mux_by(vec![stream::empty::<i32>()], |a, b| a.cmp(b));
        let out: Vec<_> = merged.collect().await;
        assert!(out.is_empty());

        let merged_none: Vec<stream::Iter<std::vec::IntoIter<i32>>> = Vec::new();
        let merged = mux_by(merged_none, |a: &i32, b: &i32| a.cmp(b));
        let out: Vec<_> = merged.collect().await;
        assert!(out.is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn deterministic_tie_break_by_source_index() {
        // Same values across streams ⇒ order is deterministic by source index (lower src first).
        let s0 = stream::iter(vec![1, 1, 1]);
        let s1 = stream::iter(vec![1, 1]);

        let merged = mux_by(vec![s0, s1], |a: &i32, b: &i32| a.cmp(b));
        let out: Vec<_> = merged.collect().await;

        // With the current implementation, ties prefer lower `src` first.
        // This yields all s0 1s before s1 1s.
        assert_eq!(out, vec![1, 1, 1, 1, 1]);
    }

    #[test]
    fn waits_for_missing_head_then_unblocks_in_order() {
        use futures::{
            channel::mpsc,
            stream::Stream,
            task::{Context, noop_waker},
        };
        use std::{pin::Pin, task::Poll};

        // Two controllable streams
        let (tx_a, rx_a) = mpsc::unbounded::<i32>();
        let (tx_b, rx_b) = mpsc::unbounded::<i32>();

        // Build mux (ascending)
        let mut mux = mux_by(vec![rx_a, rx_b], |a, b| a.cmp(b));

        // Manual polling setup
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut mux = Pin::new(&mut mux);

        // Make A ready, B empty  → must NOT yield yet (Pending)
        tx_a.unbounded_send(1).unwrap();
        assert!(matches!(Stream::poll_next(mux.as_mut(), &mut cx), Poll::Pending));

        // Unlock B with a smaller head (0)  → now it should yield 0
        tx_b.unbounded_send(0).unwrap();
        match Stream::poll_next(mux.as_mut(), &mut cx) {
            Poll::Ready(Some(v)) => assert_eq!(v, 0),
            other => panic!("expected Ready(Some(0)), got {:?}", other),
        }

        // After popping 0, B has no head → must block again (Pending)
        assert!(matches!(Stream::poll_next(mux.as_mut(), &mut cx), Poll::Pending));

        // Provide B's next head 3 (so A's 1 should come next)
        tx_b.unbounded_send(3).unwrap();
        match Stream::poll_next(mux.as_mut(), &mut cx) {
            Poll::Ready(Some(v)) => assert_eq!(v, 1),
            other => panic!("expected Ready(Some(1)), got {:?}", other),
        }

        // Provide A's next head 2 → next yield is 2 (since 2 < 3)
        tx_a.unbounded_send(2).unwrap();
        match Stream::poll_next(mux.as_mut(), &mut cx) {
            Poll::Ready(Some(v)) => assert_eq!(v, 2),
            other => panic!("expected Ready(Some(2)), got {:?}", other),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn tie_break_prefers_lower_src_index() {
        use futures::{stream, StreamExt};

        // Both streams yield equal keys (1), but we tag items with their src.
        // Expectation: lower src (0) should come before higher src (1) on ties.
        let s0 = stream::iter(vec![(1, 10usize), (1, 11usize)]); // src 0 tags
        let s1 = stream::iter(vec![(1, 20usize), (1, 21usize)]); // src 1 tags

        let merged = mux_by(vec![s0, s1], |a, b| a.0.cmp(&b.0)); // compare by key only
        let tags: Vec<usize> = merged.map(|(_, tag)| tag).collect().await;

        // With correct tie-break (reverse also on src), this passes:
        assert_eq!(tags, vec![10, 11, 20, 21]);

        // With the old Ord (primary reversed but tie-break not reversed),
        // this would come out as [20, 21, 10, 11] and the test fails.
    }
}
