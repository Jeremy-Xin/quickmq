package main

// RunPutChan : start the put chan
func RunPutChan(namechan chan string, ptrchan chan string) {
	for {
		queueName := <-namechan
		putpos := readQueuePutPtr(queueName)
		ptrchan <- putpos
	}
}

// RunGetChan : start the get chan
func RunGetChan(namechan chan string, ptrchan chan string) {
	for {
		quequeName := <-namechan
		getpos := readQueueGetPtr(quequeName)
		ptrchan <- getpos
	}
}
