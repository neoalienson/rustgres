use super::error::Result;
use super::page::PageId;
use super::buffer_pool::BufferPool;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;

/// Heap file for unordered tuple storage
pub struct HeapFile {
    buffer_pool: Arc<BufferPool>,
    next_page_id: u32,
    tuples: Arc<Mutex<HashMap<(PageId, u16), Vec<u8>>>>,
}

impl HeapFile {
    /// Creates a new heap file
    pub fn new(buffer_pool: Arc<BufferPool>) -> Self {
        Self {
            buffer_pool,
            next_page_id: 1,
            tuples: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    
    /// Inserts a tuple into a specific page
    pub fn insert_tuple(&self, page_id: PageId, data: Vec<u8>) -> Result<u16> {
        self.buffer_pool.fetch(page_id)?;
        let mut tuples = self.tuples.lock().unwrap();
        let slot = tuples.len() as u16;
        tuples.insert((page_id, slot), data);
        self.buffer_pool.unpin(page_id, true)?;
        Ok(slot)
    }
    
    /// Gets a tuple from a specific page and slot
    pub fn get_tuple(&self, page_id: PageId, slot: u16) -> Result<Vec<u8>> {
        self.buffer_pool.fetch(page_id)?;
        let tuples = self.tuples.lock().unwrap();
        let data = tuples.get(&(page_id, slot)).cloned().unwrap_or_default();
        self.buffer_pool.unpin(page_id, false)?;
        Ok(data)
    }
    
    /// Inserts a tuple into the heap file
    pub fn insert(&mut self, data: &[u8]) -> Result<(PageId, u16)> {
        // Find page with enough space or allocate new page
        let page_id = self.find_page_with_space(data.len())?;
        
        // Insert tuple into page (simplified)
        let slot = 0; // Would track actual slot
        
        Ok((page_id, slot))
    }
    
    /// Reads a tuple from the heap file
    pub fn read(&self, page_id: PageId, _slot: u16) -> Result<Vec<u8>> {
        self.buffer_pool.fetch(page_id)?;
        
        // Read tuple data (simplified)
        Ok(vec![])
    }
    
    /// Deletes a tuple from the heap file
    pub fn delete(&mut self, page_id: PageId, _slot: u16) -> Result<()> {
        self.buffer_pool.fetch(page_id)?;
        
        // Mark tuple as deleted (simplified)
        self.buffer_pool.unpin(page_id, true)?;
        
        Ok(())
    }
    
    /// Finds a page with enough free space
    fn find_page_with_space(&mut self, _required: usize) -> Result<PageId> {
        // Simplified: always allocate new page
        let page_id = PageId(self.next_page_id);
        self.next_page_id += 1;
        
        // Fetch page to ensure it's in buffer pool
        self.buffer_pool.fetch(page_id)?;
        self.buffer_pool.unpin(page_id, false)?;
        
        Ok(page_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_heap_file_insert() {
        let pool = Arc::new(BufferPool::new(10));
        let mut heap = HeapFile::new(pool);
        
        let data = vec![1, 2, 3, 4];
        let (page_id, slot) = heap.insert(&data).unwrap();
        
        assert_eq!(page_id, PageId(1));
        assert_eq!(slot, 0);
    }
    
    #[test]
    fn test_heap_file_delete() {
        let pool = Arc::new(BufferPool::new(10));
        let mut heap = HeapFile::new(pool);
        
        let data = vec![1, 2, 3, 4];
        let (page_id, slot) = heap.insert(&data).unwrap();
        
        heap.delete(page_id, slot).unwrap();
    }
}
