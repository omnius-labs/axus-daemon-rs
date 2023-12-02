use std::cmp::{self, Ordering};

#[derive(Default)]
pub struct Kadex;

impl Kadex {
    pub fn find(base: &[u8], target: &[u8], elements: &[Vec<u8>], count: usize) -> Vec<Vec<u8>> {
        let mut list: Vec<SortEntry> = Vec::new();

        let diff: Vec<u8> = target.iter().zip(base).map(|(x, y)| x ^ y).collect();
        list.push(SortEntry { value: base.to_vec(), diff });

        for element in elements {
            let diff: Vec<u8> = target.iter().zip(element).map(|(x, y)| x ^ y).collect();
            list.push(SortEntry {
                value: element.to_owned(),
                diff,
            });
        }

        let mut tmp_list: Vec<&SortEntry> = Vec::with_capacity(count);

        // append dummy
        for _ in 0..count {
            tmp_list.push(&list[0]);
        }

        for entry in list.iter().skip(1) {
            let mut left = 0;
            let mut right = tmp_list.len();

            while left < right {
                let middle = (left + right) / 2;

                if Kadex::compare(&tmp_list[middle].diff, &entry.diff) != Ordering::Greater {
                    left = middle + 1;
                } else {
                    right = middle;
                }
            }

            if left == tmp_list.len() {
                continue;
            }

            for j in ((left + 1)..(tmp_list.len() - 1)).rev() {
                tmp_list.swap(j - 1, j);
            }

            tmp_list[left] = entry;
        }

        tmp_list
            .into_iter()
            .take_while(|v| v.value != base)
            .map(|v| v.value.to_owned())
            .collect::<Vec<Vec<u8>>>()
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
    pub fn find_test() {
        let base: Vec<u8> = vec![0, 0, 0, 0];
        let target: Vec<u8> = vec![1, 1, 1, 1];
        let elements: Vec<Vec<u8>> = vec![vec![1, 1, 1, 1], vec![0, 1, 1, 1], vec![0, 0, 1, 1]];
        let res = Kadex::find(&base, &target, &elements, 3);
        assert_eq!(res, vec![vec![1, 1, 1, 1], vec![0, 1, 1, 1], vec![0, 0, 1, 1]]);

        let base: Vec<u8> = vec![0, 0, 0, 0];
        let target: Vec<u8> = vec![1, 1, 1, 1];
        let elements: Vec<Vec<u8>> = vec![vec![1, 1, 1, 1], vec![0, 1, 1, 1], vec![0, 0, 1, 1]];
        let res = Kadex::find(&base, &target, &elements, 2);
        assert_eq!(res, vec![vec![1, 1, 1, 1], vec![0, 1, 1, 1]]);

        let base: Vec<u8> = vec![0, 0, 0, 0];
        let target: Vec<u8> = vec![1, 1, 1, 1];
        let elements: Vec<Vec<u8>> = vec![vec![1, 1, 1, 1], vec![0, 1, 1, 1], vec![0, 0, 1, 1]];
        let res = Kadex::find(&base, &target, &elements, 1);
        assert_eq!(res, vec![vec![1, 1, 1, 1]]);
    }

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
