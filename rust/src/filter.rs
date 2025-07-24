

#[derive(Debug, Clone)]
pub struct ContentFilter {
    min: usize,
    max: usize,
}


const DEFAULT_MIN_CONTENT_LENGTH: usize = 0;
const DEFAULT_MAX_CONTENT_LENGTH: usize = usize::MAX;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FilterResult {
    Accept,
    TooShort,
    TooLong,
}


impl ContentFilter {
    pub fn new(min: Option<usize>, max: Option<usize>) -> Option<Self> {
        let min = min.unwrap_or(DEFAULT_MIN_CONTENT_LENGTH);
        let max = max.unwrap_or(DEFAULT_MAX_CONTENT_LENGTH);

        if min <= max {
            Some(Self { min, max })
        } else {
            None
        }
    }

    pub fn with_defaults() -> Self {
        Self {
            min: DEFAULT_MIN_CONTENT_LENGTH,
            max: DEFAULT_MAX_CONTENT_LENGTH
        }
    }

    pub fn check(&self, content: &str) -> FilterResult {
        let length = content.len();
        if length < self.min {
            FilterResult::TooShort
        } else if length > self.max {
            FilterResult::TooLong
        } else {
            FilterResult::Accept
        }
    }
}
