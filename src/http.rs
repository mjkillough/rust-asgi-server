use std;
use std::clone::Clone;

use futures_cpupool::CpuPool;
use futures;
use futures::{BoxFuture, Future, Stream};
use hyper;
use hyper::{Headers, HttpVersion, Method, Uri};
use hyper::header::ContentType;
use hyper::mime::{Attr, Mime, SubLevel, TopLevel, Value};
use hyper::server::{NewService, Response, Request, Service};
use hyper::status::StatusCode;
use r2d2;
use serde::bytes::{ByteBuf, Bytes};

use body::BodyStream;
use channels::{ChannelError, ChannelLayer, RedisChannelLayer, RedisChannelLayerManager, ReplyPump};
use msgs;


pub struct AsgiHttpServiceFactory<C>
    where C: ChannelLayer
{
    reply_pump: ReplyPump<C>,
    channel_pool: r2d2::Pool<C::Manager>,
}

impl<C> AsgiHttpServiceFactory<C>
    where C: ChannelLayer
{
    pub fn new() -> AsgiHttpServiceFactory<RedisChannelLayer> {
        let connection_info = "redis://127.0.0.1";

        // Make a ReplyPump with its own dedicated channel layer.
        let channel_layer = RedisChannelLayer::new(connection_info.clone()).unwrap();
        let reply_pump = ReplyPump::new(channel_layer);

        // Make a pool of channel layers that we can use to send requests on.
        let config = r2d2::Config::builder()
            .pool_size(15)
            .build();
        let manager = RedisChannelLayerManager::new(connection_info).unwrap();
        let pool = r2d2::Pool::new(config, manager).unwrap();

        AsgiHttpServiceFactory {
            reply_pump: reply_pump,
            channel_pool: pool,
        }
    }
}

impl<C> NewService for AsgiHttpServiceFactory<C>
    where C: ChannelLayer
{
    type Request = Request;
    type Response = Response<BodyStream<C>>;
    type Error = hyper::Error;
    type Instance = AsgiHttpService<C>;

    fn new_service(&self) -> Result<Self::Instance, std::io::Error> {
        Ok(AsgiHttpService {
            reply_pump: self.reply_pump.clone(),
            channel_pool: self.channel_pool.clone(),
        })
    }
}


pub struct AsgiHttpService<C>
    where C: ChannelLayer
{
    reply_pump: ReplyPump<C>,
    channel_pool: r2d2::Pool<C::Manager>,
}

impl<C> Service for AsgiHttpService<C>
    where C: ChannelLayer
{
    type Request = Request;
    type Response = Response<BodyStream<C>>;
    type Error = hyper::Error;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn call(&self, req: Request) -> Self::Future {
        let (method, uri, version, headers, body) = req.deconstruct();
        let cpu_pool = CpuPool::new(4);

        let reply_pump = self.reply_pump.clone();
        let channel_pool: r2d2::Pool<C::Manager> = self.channel_pool.clone();

        // We chain a series of futures together in order to handle the request/response async.
        // We don't actually care about the errors of the individual stages, as we'll return a
        // generic error response to the client, so we keep it simple and  map all errors to ().
        body
            // Wait for the entire body of the request to be in memory before proceeding. It feels
            // like it would be nice to send each chunk over ASGI separately, but once Channels
            // receives a http.request, it blocks while it waits for its body. Buffer the entire
            // body here to avoid blocking in the sync back-end Channels worker processes.
            .collect()
            .map_err(|_| ())
            // Convert our Vec<Chunk> to a Vec<u8>.
            .map(|body| {
                body.iter()
                    .fold(Vec::new(), |mut vec, chunk| {
                        vec.extend_from_slice(&chunk);
                        vec
                    })
            })
            // Send our body down the channel. To keep the code simple we do this in one
            // synchronous operation on the thread-pool.
            .and_then(move |body| {
                cpu_pool.spawn_fn(move || {
                    send_request_sync::<C>(channel_pool, method, uri, version, headers, body)
                })
                .map_err(|_| ())
            })
            .map_err(|_| ())
            // We wait for the initial response on the request's reply channel. We'll wait for
            // subsequent chunks inside the body stream.
            .and_then(move |reply_channel| {
                reply_pump.wait_for_reply_async::<msgs::http::Response>(reply_channel.clone())
                    .map(move |asgi_response| (reply_pump, reply_channel, asgi_response))
                    .map_err(|_| ())
            })
            // Start sending the response to the client. If this is a streaming response, we'll
            // return a body stream which continues to send chunks as we receive them.
            .and_then(send_response)
            // If we encountered any error along the way, give a generic error response to the
            // client. Note that we do not propogate the error to Hyper - the request has succeeded
            // as far as it's concerned.
            .or_else(|_| {
                futures::future::ok(
                    error_response(StatusCode::InternalServerError, "Unknown server error")
                )
            })
            .boxed()
    }
}


fn send_request_sync<C>(channel_pool: r2d2::Pool<C::Manager>,
                        method: Method,
                        uri: Uri,
                        version: HttpVersion,
                        headers: Headers,
                        body: Vec<u8>)
                        -> Result<String, ChannelError>
    where C: ChannelLayer
{
    let channels = channel_pool.get().unwrap();

    // If the body of the request is over a certain size, then we must break it up and send each
    // chunk separately on a per request http.request.body? channel.
    let chunk_size = 1 * 1024 * 1024; // 1 MB
    let mut chunks = body.chunks(chunk_size).peekable();
    let initial_chunk = match chunks.next() {
        Some(chunk) => chunk,
        None => &[0; 0],
    };
    let body_channel = match chunks.len() > 0 {
        true => Some(channels.new_channel("http.request.body?")?),
        false => None,
    };

    let version = match version {
        HttpVersion::Http10 => "1.0",
        HttpVersion::Http11 => "1.1",
        _ => panic!("Unsupported HTTP version"),
    };

    // Lower-case the header names (as per ASGI spec) and convert to UTF-8 byte strings.
    let headers: Vec<(ByteBuf, ByteBuf)> = headers.iter()
        .map(|header| {
            let name = header.name().to_lowercase().into_bytes();
            let value = header.value_string().into_bytes();
            (ByteBuf::from(name), ByteBuf::from(value))
        })
        .collect();

    // Send the initial chunk of the request to http.request. We must use an extra scope for this
    // because otherwise we'll find reply_channel is borrowed for longer than necessary.
    let reply_channel = channels.new_channel("http.response!")?;
    {
        channels.send("http.request",
                  &msgs::http::Request {
                      reply_channel: &reply_channel,
                      http_version: &version,
                      method: method.as_ref(),
                      path: uri.path(),
                      query_string: uri.query().unwrap_or(""),
                      headers: headers,
                      body: Bytes::from(initial_chunk),
                      // Dance to turn Option<String> to Option<&str>:
                      body_channel: body_channel.as_ref().map(String::as_ref),
                  })?;
    }

    // If we have more than one chunk, send each in a separate message down the body_channel.
    // We must do some iterator dancing, as we must know which chunk is the last.
    if let Some(body_channel) = body_channel {
        loop {
            match chunks.next() {
                Some(chunk) => {
                    channels.send(&body_channel,
                              &msgs::http::RequestBodyChunk {
                                  content: Bytes::from(chunk),
                                  closed: false,
                                  more_content: !chunks.peek().is_none(),
                              })?
                }
                None => break,
            }
        }
    }

    Ok(reply_channel)
}


fn send_response<C>((pump, channel, asgi_resp): (ReplyPump<C>, String, msgs::http::Response))
                    -> Result<Response<BodyStream<C>>, ()>
    where C: ChannelLayer
{
    let mut resp: Response<BodyStream<C>> = Response::new();
    resp.set_status(StatusCode::from_u16(asgi_resp.status));
    for (name, value) in asgi_resp.headers {
        let name = String::from_utf8(name.into()).unwrap();
        let value: Vec<u8> = value.into();
        resp.headers_mut().set_raw(name, value);
    }

    let initial_chunk = msgs::http::ResponseBodyChunk {
        content: asgi_resp.content,
        more_content: asgi_resp.more_content,
    };
    let stream = BodyStream::response(pump, channel, initial_chunk);
    Ok(resp.with_body(stream))
}


fn error_response<C>(status: StatusCode, body: &str) -> Response<BodyStream<C>>
    where C: ChannelLayer
{
    let body = format!(include_str!("error.html"), status = status, body = body);
    Response::new()
        .with_status(status)
        .with_header(ContentType(Mime(TopLevel::Text,
                                      SubLevel::Html,
                                      vec![(Attr::Charset, Value::Utf8)])))
        .with_body(BodyStream::error(body))
}
