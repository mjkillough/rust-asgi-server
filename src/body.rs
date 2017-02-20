use std;

use hyper;
use futures;
use futures::{Async, BoxFuture, Future, Poll, Stream};

use msgs;
use channels::{RedisChannelLayer, ReplyPump};


pub enum BodyStream {
    Error(ErrorBodyStream),
    Response(ResponseBodyStream),
}

impl BodyStream {
    pub fn response(pump: ReplyPump<RedisChannelLayer>,
                channel: String,
                initial_chunk: msgs::http::ResponseBodyChunk)
                -> Self {
        BodyStream::Response(ResponseBodyStream {
            pump: pump,
            channel: channel,
            future: Some(futures::future::ok(initial_chunk).boxed()),
        })
    }

    pub fn error(body: String) -> Self {
        BodyStream::Error(ErrorBodyStream(Some(body)))
    }
}

impl Stream for BodyStream {
    type Item = Vec<u8>;
    type Error = hyper::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        match self {
            &mut BodyStream::Error(ref mut resp) => resp.poll(),
            &mut BodyStream::Response(ref mut resp) => resp.poll(),
        }
    }
}


pub struct ErrorBodyStream(Option<String>);

impl Stream for ErrorBodyStream {
    type Item = Vec<u8>;
    type Error = hyper::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        match std::mem::replace(&mut self.0, None) {
            Some(body) => Ok(Async::Ready(Some(body.into_bytes()))),
            None => Ok(Async::Ready(None)),
        }
    }
}


pub struct ResponseBodyStream {
    pump: ReplyPump<RedisChannelLayer>,
    channel: String,
    future: Option<BoxFuture<msgs::http::ResponseBodyChunk, ()>>,
}

impl Stream for ResponseBodyStream {
    type Item = Vec<u8>;
    type Error = hyper::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        match std::mem::replace(&mut self.future, None) {
            // We are waiting for the next chunk to be ready - poll the future.
            Some(mut future) => {
                match future.poll() {
                    // We have a chunk. If it isn't our last, then start waiting for the next chunk,
                    // whilst we yield this one.
                    Ok(Async::Ready(resp)) => {
                        self.future = match resp.more_content {
                            true => Some(self.pump.wait_for_reply_async(self.channel.clone())),
                            false => None,
                        };
                        // Yield the chunk we've received.
                        Ok(Async::Ready(Some(resp.content.into())))
                    }
                    // Our future isn't ready - remember to poll it next time.
                    Ok(Async::NotReady) => {
                        self.future = Some(future);
                        Ok(Async::NotReady)
                    }
                    // XXX What error should we be sending here?
                    Err(()) => Err(hyper::Error::Incomplete),
                }
            }
            // The last chunk was our last. Indicate end-of-stream.
            None => Ok(Async::Ready(None)),
        }
    }
}
