package pubsub

import "github.com/goware/logger"

// unbounded buffered channel implementation
// inspired by https://medium.com/capital-one-tech/building-an-unbounded-channel-in-go-789e175cd2cd

// converts a blocking unbuffered send channel into a non-blocking unbounded buffered one
func MakeUnboundedBufferedChan[M any](sendCh chan<- M, log logger.Logger, bufferLimitWarning int) chan<- M {
	ch := make(chan M)

	go func() {
		var buffer []M

		for {
			if len(buffer) == 0 {
				if blocks, ok := <-ch; ok {
					buffer = append(buffer, blocks)
					if len(buffer) > bufferLimitWarning {
						log.Warnf("channel buffer holds %v > %v messages", len(buffer), bufferLimitWarning)
					}
				} else {
					close(sendCh)
					break
				}
			} else {
				select {
				case sendCh <- buffer[0]:
					buffer = buffer[1:]

				case blocks, ok := <-ch:
					if ok {
						buffer = append(buffer, blocks)
						if len(buffer) > bufferLimitWarning {
							log.Warnf("channel buffer holds %v > %v messages", len(buffer), bufferLimitWarning)
						}
					}
				}
			}
		}
	}()

	return ch
}
