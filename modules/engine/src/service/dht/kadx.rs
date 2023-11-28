use std::cmp::{self, Ordering};

#[derive(Default)]
pub struct Kadex;

impl Kadex {
    pub fn find(base: &[u8], target: &[u8], elements: &[Vec<u8>], count: usize) -> Vec<Vec<u8>> {
        let mut list: Vec<SortEntry> = Vec::new();

        let diff: Vec<_> = target.iter().zip(base).map(|(x, y)| x ^ y).collect();
        list.push(SortEntry {
            value: target.to_vec(),
            diff,
        });

        for e in elements {
            let diff: Vec<_> = target.iter().zip(e).map(|(x, y)| x ^ y).collect();
            list.push(SortEntry { value: e.to_owned(), diff });
        }

        for i in 1..list.len() {
            let tmp = &list[i];

            let mut left = 0;
            let mut right = cmp::min(i, count) as usize;

            while left < right {
                let middle = (left + right) / 2;

                if Kadex::compare(&list[middle].diff, &tmp.diff) != Ordering::Greater {
                    left = middle + 1;
                } else {
                    right = middle;
                }
            }
        }

        list.into_iter().take_while(|v| v.value == target).map(|v| v.value).collect::<Vec<_>>()
    }

    pub fn distance(x: &[u8], y: &[u8]) -> u8 {
        let mut res: u8 = 0;
        let len = cmp::min(x.len(), y.len());

        for i in 0..len {
            let v = x[i] ^ y[i];
            res = (8 - v.leading_zeros()) as u8;
            if res != 0 {
                res += ((len - (i + 1)) * 8) as u8;
                break;
            }
        }

        res
    }

    pub fn compare(x: &[u8], y: &[u8]) -> Ordering {
        if x.len() != y.len() {
            return x.len().cmp(&y.len());
        }

        if x.is_empty() {
            return Ordering::Equal;
        }

        for i in (0..x.len()).rev() {
            let o = x[i].cmp(&y[i]);
            if o != Ordering::Equal {
                return o;
            }
        }

        Ordering::Equal
    }
}

struct SortEntry {
    pub value: Vec<u8>,
    pub diff: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use super::Kadex;

    #[test]
    pub fn distance_test() {
        let x: Vec<u8> = vec![1, 1, 1, 1];
        let y: Vec<u8> = vec![1, 1, 1, 1];
        let res = Kadex::distance(&x, &y);
        assert_eq!(res, 0);

        let x: Vec<u8> = vec![1, 1, 1, 1];
        let y: Vec<u8> = vec![0, 1, 1, 1];
        let res = Kadex::distance(&x, &y);
        assert_eq!(res, 25);

        let x: Vec<u8> = vec![0, 0, 0, 1];
        let y: Vec<u8> = vec![0, 0, 0, 0];
        let res = Kadex::distance(&x, &y);
        assert_eq!(res, 1);
    }

    #[test]
    pub fn compare_test() {
        let x: Vec<u8> = vec![1, 1, 1, 1];
        let y: Vec<u8> = vec![1];
        let res = Kadex::compare(&x, &y);
        assert_eq!(res, Ordering::Greater);

        let x: Vec<u8> = vec![];
        let y: Vec<u8> = vec![];
        let res = Kadex::compare(&x, &y);
        assert_eq!(res, Ordering::Equal);

        let x: Vec<u8> = vec![0, 0, 0, 0];
        let y: Vec<u8> = vec![0, 0, 0, 1];
        let res = Kadex::compare(&x, &y);
        assert_eq!(res, Ordering::Less);
    }
}
