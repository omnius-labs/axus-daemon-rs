use std::cmp;

#[derive(Default)]
pub struct Kadex;

impl Kadex {
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
}

#[cfg(test)]
mod tests {
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
}
