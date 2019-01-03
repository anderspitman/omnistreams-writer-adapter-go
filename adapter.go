package omniwriter

import (
        //"fmt"
        "io"
        omnicore "github.com/anderspitman/omnistreams-core-go"
)

type WriterAdapter struct {
        finished bool
        dataChan chan []byte
        RequestCallback func(numElements uint32)
        cancelCallback func()
        finishedCallback func()
        writer io.Writer
        bufferSize uint32
        id uint32
}

func (a *WriterAdapter) Write(element []byte) {
        a.dataChan <- element
}

func (a *WriterAdapter) End() {
        a.finished = true
        close(a.dataChan)
}

func (a *WriterAdapter) OnRequest(callback func(numElements uint32)) {
        a.RequestCallback = callback
}

func (a *WriterAdapter) OnFinished(callback func()) {
        a.finishedCallback = callback
}

func (a *WriterAdapter) Cancel() {
        a.finished = true
}

func (a *WriterAdapter) OnCancel(callback func()) {
        a.cancelCallback = callback
}

var nextId uint32 = 0

func CreateWriterAdapter(writer io.Writer, bufferSize uint32) omnicore.Consumer {

        dataChan := make(chan []byte, bufferSize)

        adapter := WriterAdapter {
                finished: false,
                dataChan: dataChan,
                id: nextId,
        }

        nextId++

        go func() {
	        for data := range dataChan {
		        if !adapter.finished {
                                // WARNING!!! Putting print statements in here can have disastrous
                                // consequences, because print can cause the goroutine to sleep, which
                                // creates a space for the writer to get shut down after the safety
                                // check on the previous line.
				writer.Write(data)
				adapter.RequestCallback(1)
			}
		}

                adapter.finishedCallback()
	}()

        return &adapter
}
