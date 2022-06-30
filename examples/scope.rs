use yastl::Pool;

fn main() {
    let pool = Pool::new(3,Vec::from([0,1,2]));
    let mut list = vec![1, 2, 3, 4, 5];

    pool.scoped(|scope| {
        // since the `scope` guarantees that the threads are finished if it drops,
        // we can safely borrow `list` inside here.
        for x in list.iter_mut() {
            scope.execute(move || {
                for _i in 0..100000{
                    *x += 2;
                }
            });
        }
    });

    assert_eq!(list, vec![3, 4, 5, 6, 7]);
}
