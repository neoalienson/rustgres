use super::{SimpleExecutor, SimpleTuple as Tuple, ExecutorError};

pub struct Sort {
    input: Box<dyn SimpleExecutor>,
    sorted_tuples: Vec<Tuple>,
    index: usize,
    sorted: bool,
}

impl Sort {
    pub fn new(input: Box<dyn SimpleExecutor>) -> Self {
        Self {
            input,
            sorted_tuples: Vec::new(),
            index: 0,
            sorted: false,
        }
    }
    
    fn sort_tuples(&mut self) -> Result<(), ExecutorError> {
        while let Some(tuple) = self.input.next()? {
            self.sorted_tuples.push(tuple);
        }
        
        self.sorted_tuples.sort_by_key(|t| {
            t.data.get(0).copied().unwrap_or(0)
        });
        
        self.sorted = true;
        Ok(())
    }
}

impl SimpleExecutor for Sort {
    fn open(&mut self) -> Result<(), ExecutorError> {
        self.input.open()
    }
    
    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        if !self.sorted {
            self.sort_tuples()?;
        }
        
        if self.index < self.sorted_tuples.len() {
            let tuple = self.sorted_tuples[self.index].clone();
            self.index += 1;
            Ok(Some(tuple))
        } else {
            Ok(None)
        }
    }
    
    fn close(&mut self) -> Result<(), ExecutorError> {
        self.input.close()
    }
}
