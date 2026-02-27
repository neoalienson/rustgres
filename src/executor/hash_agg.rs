use super::{SimpleExecutor, SimpleTuple as Tuple, ExecutorError};
use std::collections::HashMap;

pub struct HashAgg {
    input: Box<dyn SimpleExecutor>,
    groups: HashMap<i64, AggState>,
    results: Vec<Tuple>,
    index: usize,
    aggregated: bool,
}

#[derive(Clone)]
struct AggState {
    count: u64,
    sum: i64,
}

impl HashAgg {
    pub fn new(input: Box<dyn SimpleExecutor>) -> Self {
        Self {
            input,
            groups: HashMap::new(),
            results: Vec::new(),
            index: 0,
            aggregated: false,
        }
    }
    
    fn aggregate(&mut self) -> Result<(), ExecutorError> {
        while let Some(tuple) = self.input.next()? {
            let key = tuple.data.get(0).copied().unwrap_or(0) as i64;
            let value = tuple.data.get(1).copied().unwrap_or(0) as i64;
            
            let state = self.groups.entry(key).or_insert(AggState { count: 0, sum: 0 });
            state.count += 1;
            state.sum += value;
        }
        
        for (key, state) in &self.groups {
            self.results.push(Tuple {
                data: vec![*key as u8, state.count as u8, state.sum as u8],
            });
        }
        
        self.aggregated = true;
        Ok(())
    }
}

impl SimpleExecutor for HashAgg {
    fn open(&mut self) -> Result<(), ExecutorError> {
        self.input.open()
    }
    
    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        if !self.aggregated {
            self.aggregate()?;
        }
        
        if self.index < self.results.len() {
            let tuple = self.results[self.index].clone();
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
