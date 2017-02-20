use std;
use std::clone::Clone;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use crossbeam::sync::MsQueue;
use futures::{BoxFuture, Future};
use futures::sync::oneshot;

use serde::Deserialize;

use channels::{ChannelLayer, ChannelReply};


// A reply channel that we should be listening on. We complete the provided sender once we have
// a reply, which will resolve a future elsewhere.
struct ReplyChannel {
    channel: String,
    sender: oneshot::Sender<ChannelReply>,
}


// These requests are sent to the Reply Pump's thread via a queue, to ask it to do things.
enum PumpRequest {
    Listen(ReplyChannel),
    Join,
}


// Shared context between the Reply Pump and its thread.
struct PumpContext {
    queue: MsQueue<PumpRequest>,
}


// We need a PhantomData in order to make ReplyPump generic without actually using it's C.
// If C is not Sync, then PhantomData<C> is not sync. This is sad and unncessary, given we never
// actually use C in a way that requires it be Sync - we send it over to another thread.
struct SyncPhantomData<T>(PhantomData<T>);
unsafe impl<T> Sync for SyncPhantomData<T> {}


pub struct ReplyPump<C>
    where C: 'static + ChannelLayer + Send
{
    context: Arc<PumpContext>,
    phantom: SyncPhantomData<C>,
}

impl<C> Clone for ReplyPump<C>
    where C: 'static + ChannelLayer + Send
{
    fn clone(&self) -> Self {
        return ReplyPump {
            context: self.context.clone(),
            phantom: SyncPhantomData(PhantomData),
        };
    }
}

impl<C> ReplyPump<C>
    where C: 'static + ChannelLayer + Send
{
    pub fn new(channel_layer: C) -> Self {
        let reply_pump = ReplyPump {
            context: Arc::new(PumpContext { queue: MsQueue::new() }),
            phantom: SyncPhantomData(PhantomData),
        };

        let context = reply_pump.context.clone();
        std::thread::spawn(move || Self::thread_func(&context, channel_layer));

        reply_pump
    }

    pub fn wait_for_reply_async<D>(&self, channel: String) -> BoxFuture<D, ()>
        where D: Deserialize
    {
        let (tx, rx) = oneshot::channel::<ChannelReply>();

        self.context.queue.push(PumpRequest::Listen(ReplyChannel {
            channel: channel,
            sender: tx,
        }));

        // TODO: Propogate canceled error? We should probably wrap it in our own type first.
        rx.map_err(|_| ())
            .map(|reply| C::deserialize(reply).unwrap())
            .boxed()
    }

    fn thread_func(ctx: &PumpContext, channel_layer: C) {
        let mut reply_channels: HashMap<String, oneshot::Sender<ChannelReply>> = HashMap::new();
        loop {
            // Process any new requests from the outside world. If we have no channels to wait on,
            // then block until we receive a request.
            loop {
                let request = match reply_channels.is_empty() {
                    true => ctx.queue.pop(),
                    false => {
                        match ctx.queue.try_pop() {
                            Some(request) => request,
                            None => break,
                        }
                    }
                };
                match request {
                    PumpRequest::Listen(reply_channel) => {
                        reply_channels.insert(reply_channel.channel, reply_channel.sender);
                    }
                    PumpRequest::Join => return,
                }
            }

            let option = channel_layer.receive(reply_channels.keys(), false)
                .expect("Failed to wait on reply");
            match option {
                Some((channel_name, reply)) => {
                    match reply_channels.remove(&channel_name) {
                        Some(sender) => sender.complete(reply),
                        None => {
                            panic!("Received reply for a channel we weren't listening on: {}",
                                   &channel_name)
                        }
                    }
                }
                None => {
                    // Delay a little bit if we didn't receive something, to avoid burning CPU.
                    // We sleep 10ms because Daphne sleeps 10ms.
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
            }

            // It would be nice to poll each sender in reply_channels and remove any that have been
            // cancelled, however we can't do that outside of a Task.
        }
    }
}
