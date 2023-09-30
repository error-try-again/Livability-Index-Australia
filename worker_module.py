def worker(file_queue, process_xlsx):
    """Worker function to process files from the queue."""
    while True:
        file_path = file_queue.get()
        if file_path == "STOP":
            break
        process_xlsx(file_path)
