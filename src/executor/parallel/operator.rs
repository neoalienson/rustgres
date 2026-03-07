use crate::executor::operators::executor::ExecutorError;
use crate::executor::parallel::morsel::Morsel;

pub trait ParallelOperator: Send + Sync {
    fn process_morsel(&self, morsel: Morsel) -> Result<Morsel, ExecutorError>;
    fn degree_of_parallelism(&self) -> usize {
        num_cpus::get()
    }
}
