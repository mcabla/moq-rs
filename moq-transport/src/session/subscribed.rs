// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-FileCopyrightText: 2023-2024 Luke Curley and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::ops;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::stream::FuturesUnordered;
use futures::StreamExt;

use crate::coding::{Encode, Location, ReasonPhrase};
use crate::mlog;
use crate::serve::{ServeError, TrackReaderMode};
use crate::watch::State;
use crate::{data, message, serve};

use super::{Publisher, SessionError, SubscribeInfo, Writer, DELIVERY_TIMEOUT_PARAM};

const DELIVERY_TIMEOUT_RESET_CODE: u64 = 0x2;

// This file defines Publisher handling of inbound Subscriptions

#[derive(Debug)]
struct SubscribedState {
    largest_location: Option<Location>,
    effective_delivery_timeout: Option<Duration>,
    closed: Result<(), ServeError>,
}

impl SubscribedState {
    fn update_largest_location(&mut self, group_id: u64, object_id: u64) -> Result<(), ServeError> {
        if let Some(current_largest_location) = self.largest_location {
            let update_largest_location = Location::new(group_id, object_id);
            if update_largest_location > current_largest_location {
                self.largest_location = Some(update_largest_location);
            }
        }

        Ok(())
    }
}

impl Default for SubscribedState {
    fn default() -> Self {
        Self {
            largest_location: None,
            effective_delivery_timeout: None,
            closed: Ok(()),
        }
    }
}

pub struct Subscribed {
    /// The sessions Publisher manager, used to send control messages,
    /// create new QUIC streams, and send datagrams
    publisher: Publisher,

    /// The tracknamespace and trackname for the subscription.
    pub info: SubscribeInfo,

    state: State<SubscribedState>,

    /// Tracks if SubscribeOk has been sent yet or not. Used to send
    /// SubscribeDone vs SubscribeError on drop.
    ok: bool,

    /// Optional mlog writer for logging transport events
    mlog: Option<Arc<Mutex<mlog::MlogWriter>>>,
}

impl Subscribed {
    pub(super) fn new(
        publisher: Publisher,
        msg: message::Subscribe,
        mlog: Option<Arc<Mutex<mlog::MlogWriter>>>,
    ) -> (Self, SubscribedRecv) {
        let effective_delivery_timeout = parse_delivery_timeout_param(&msg.params);
        let (send, recv): (State<SubscribedState>, State<SubscribedState>) =
            State::default().split();
        if let Some(mut state) = send.lock_mut() {
            state.effective_delivery_timeout = effective_delivery_timeout;
        }
        let info = SubscribeInfo::new_from_subscribe(&msg);
        let send = Self {
            publisher,
            state: send,
            info,
            ok: false,
            mlog,
        };

        // Prevents updates after being closed
        let recv = SubscribedRecv { state: recv };

        (send, recv)
    }

    pub async fn serve(mut self, track: serve::TrackReader) -> Result<(), SessionError> {
        let res = self.serve_inner(track).await;
        if let Err(err) = &res {
            self.close(err.clone().into())?;
        }

        res
    }

    async fn serve_inner(&mut self, track: serve::TrackReader) -> Result<(), SessionError> {
        // Update largest location before sending SubscribeOk
        let largest_location = track.largest_location();
        let effective_delivery_timeout = {
            let mut state = self.state.lock_mut().ok_or(ServeError::Cancel)?;
            state.largest_location = largest_location;
            state.effective_delivery_timeout
        };

        // Send SubscribeOk using send_message_and_wait to ensure it is sent at least to the QUIC stack before
        // we start serving the track.  If a subscriber gets the stream before SubscribeOk
        // then they won't recognize the track_alias in the stream header.
        self.publisher
            .send_message_and_wait(message::SubscribeOk {
                id: self.info.id,
                track_alias: self.info.id, // use subscription id as track alias
                expires: 0,                // TODO SLG
                group_order: message::GroupOrder::Descending, // TODO: resolve correct value from publisher / subscriber prefs
                content_exists: largest_location.is_some(),
                largest_location,
                params: subscribe_ok_params(effective_delivery_timeout),
            })
            .await;

        self.ok = true; // So we send SubscribeDone on drop

        // Serve based on track mode
        match track.mode().await? {
            // TODO cancel track/datagrams on closed
            TrackReaderMode::Stream(_stream) => panic!("deprecated"),
            TrackReaderMode::Subgroups(subgroups) => self.serve_subgroups(subgroups).await,
            TrackReaderMode::Datagrams(datagrams) => self.serve_datagrams(datagrams).await,
        }
    }

    pub fn close(self, err: ServeError) -> Result<(), ServeError> {
        let state = self.state.lock();
        state.closed.clone()?;

        let mut state = state.into_mut().ok_or(ServeError::Done)?;
        state.closed = Err(err);

        Ok(())
    }

    pub async fn closed(&self) -> Result<(), ServeError> {
        loop {
            {
                let state = self.state.lock();
                state.closed.clone()?;

                match state.modified() {
                    Some(notify) => notify,
                    None => return Ok(()),
                }
            }
            .await;
        }
    }
}

impl ops::Deref for Subscribed {
    type Target = SubscribeInfo;

    fn deref(&self) -> &Self::Target {
        &self.info
    }
}

impl Drop for Subscribed {
    fn drop(&mut self) {
        let state = self.state.lock();
        let err = state
            .closed
            .as_ref()
            .err()
            .cloned()
            .unwrap_or(ServeError::Done);
        drop(state); // Important to avoid a deadlock

        if self.ok {
            self.publisher.send_message(message::PublishDone {
                id: self.info.id,
                status_code: err.code(),
                stream_count: 0, // TODO SLG
                reason: ReasonPhrase(err.to_string()),
            });
        } else {
            self.publisher.send_message(message::SubscribeError {
                id: self.info.id,
                error_code: err.code(),
                reason_phrase: ReasonPhrase(err.to_string()),
            });
        };
    }
}

impl Subscribed {
    async fn serve_subgroups(
        &mut self,
        mut subgroups: serve::SubgroupsReader,
    ) -> Result<(), SessionError> {
        let mut tasks = FuturesUnordered::new();
        let mut done: Option<Result<(), ServeError>> = None;
        let effective_delivery_timeout = self.state.lock().effective_delivery_timeout;

        loop {
            tokio::select! {
                res = subgroups.next(), if done.is_none() => match res {
                    Ok(Some(subgroup)) => {
                        let header = data::SubgroupHeader {
                            header_type: data::StreamHeaderType::SubgroupIdExt,  // SubGroupId = Yes, Extensions = Yes, ContainsEndOfGroup = No
                            track_alias: self.info.id, // use subscription id as track_alias
                            group_id: subgroup.group_id,
                            subgroup_id: Some(subgroup.subgroup_id),
                            publisher_priority: subgroup.priority,
                        };

                        let publisher = self.publisher.clone();
                        let state = self.state.clone();
                        let info = subgroup.info.clone();
                        let mlog = self.mlog.clone();

                        tasks.push(async move {
                            if let Err(err) = Self::serve_subgroup(
                                header,
                                subgroup,
                                publisher,
                                state,
                                effective_delivery_timeout,
                                mlog,
                            )
                            .await
                            {
                                tracing::warn!("failed to serve subgroup: {:?}, error: {}", info, err);
                            }
                        });
                    },
                    Ok(None) => done = Some(Ok(())),
                    Err(err) => done = Some(Err(err)),
                },
                res = self.closed(), if done.is_none() => done = Some(res),
                _ = tasks.next(), if !tasks.is_empty() => {},
                else => return Ok(done.unwrap()?),
            }
        }
    }

    async fn serve_subgroup(
        header: data::SubgroupHeader,
        mut subgroup_reader: serve::SubgroupReader,
        mut publisher: Publisher,
        state: State<SubscribedState>,
        effective_delivery_timeout: Option<Duration>,
        mlog: Option<Arc<Mutex<mlog::MlogWriter>>>,
    ) -> Result<(), SessionError> {
        tracing::debug!(
            "[PUBLISHER] serve_subgroup: starting - group_id={}, subgroup_id={:?}, priority={}",
            subgroup_reader.group_id,
            subgroup_reader.subgroup_id,
            subgroup_reader.priority
        );

        let mut writer: Option<Writer> = None;

        let mut object_count = 0;
        while let Some(mut subgroup_object_reader) = subgroup_reader.next().await? {
            if is_expired(subgroup_object_reader.received_at, effective_delivery_timeout) {
                tracing::info!(
                    "[PUBLISHER] serve_subgroup: delivery timeout hit for subgroup (group_id={}, subgroup_id={}) before sending object #{}",
                    subgroup_reader.group_id,
                    subgroup_reader.subgroup_id,
                    object_count + 1
                );

                if writer.is_some() {
                    return Err(delivery_timeout_reset_error());
                }

                return Ok(());
            }

            if writer.is_none() {
                let mut send_stream = publisher.open_uni().await?;
                tracing::trace!("[PUBLISHER] serve_subgroup: opened unidirectional stream");

                // TODO figure out u32 vs u64 priority
                send_stream.set_priority(subgroup_reader.priority as i32);

                let mut stream_writer = Writer::new(send_stream);

                tracing::debug!(
                    "[PUBLISHER] serve_subgroup: sending header - track_alias={}, group_id={}, subgroup_id={:?}, priority={}, header_type={:?}",
                    header.track_alias,
                    header.group_id,
                    header.subgroup_id,
                    header.publisher_priority,
                    header.header_type
                );

                stream_writer.encode(&header).await?;

                if let Some(ref mlog) = mlog {
                    if let Ok(mut mlog_guard) = mlog.lock() {
                        let time = mlog_guard.elapsed_ms();
                        let stream_id = 0; // TODO: Placeholder, need actual QUIC stream ID
                        let event = mlog::subgroup_header_created(time, stream_id, &header);
                        let _ = mlog_guard.add_event(event);
                    }
                }

                writer = Some(stream_writer);
            }

            let subgroup_object = data::SubgroupObjectExt {
                object_id_delta: 0, // before delta logic, used to be subgroup_object_reader.object_id,
                extension_headers: subgroup_object_reader.extension_headers.clone(), // Pass through extension headers
                payload_length: subgroup_object_reader.size,
                status: if subgroup_object_reader.size == 0 {
                    // Only set status if payload length is zero
                    Some(subgroup_object_reader.status)
                } else {
                    None
                },
            };

            tracing::debug!(
                "[PUBLISHER] serve_subgroup: sending object #{} - object_id={}, object_id_delta={}, payload_length={}, status={:?}, extension_headers={:?}",
                object_count + 1,
                subgroup_object_reader.object_id,
                subgroup_object.object_id_delta,
                subgroup_object.payload_length,
                subgroup_object.status,
                subgroup_object.extension_headers
            );

            writer
                .as_mut()
                .expect("writer must exist after lazy stream creation")
                .encode(&subgroup_object)
                .await?;

            // Log subgroup object created/sent
            if let Some(ref mlog) = mlog {
                if let Ok(mut mlog_guard) = mlog.lock() {
                    let time = mlog_guard.elapsed_ms();
                    let stream_id = 0; // TODO: Placeholder, need actual QUIC stream ID
                    let event = mlog::subgroup_object_ext_created(
                        time,
                        stream_id,
                        subgroup_reader.group_id,
                        subgroup_reader.subgroup_id,
                        subgroup_object_reader.object_id,
                        &subgroup_object,
                    );
                    let _ = mlog_guard.add_event(event);
                }
            }

            state
                .lock_mut()
                .ok_or(ServeError::Done)?
                .update_largest_location(
                    subgroup_reader.group_id,
                    subgroup_object_reader.object_id,
                )?;

            let mut chunks_sent = 0;
            let mut bytes_sent = 0;
            while let Some(chunk) = subgroup_object_reader.read().await? {
                if is_expired(subgroup_object_reader.received_at, effective_delivery_timeout) {
                    tracing::info!(
                        "[PUBLISHER] serve_subgroup: delivery timeout hit mid-object (group_id={}, subgroup_id={}, object_id={})",
                        subgroup_reader.group_id,
                        subgroup_reader.subgroup_id,
                        subgroup_object_reader.object_id
                    );
                    return Err(delivery_timeout_reset_error());
                }

                tracing::trace!(
                    "[PUBLISHER] serve_subgroup: sending payload chunk #{} for object #{} ({} bytes)",
                    chunks_sent + 1,
                    object_count + 1,
                    chunk.len()
                );
                bytes_sent += chunk.len();
                writer
                    .as_mut()
                    .expect("writer must exist while sending payload")
                    .write(&chunk)
                    .await?;
                chunks_sent += 1;
            }

            tracing::trace!(
                "[PUBLISHER] serve_subgroup: completed object #{} ({} chunks, {} bytes total)",
                object_count + 1,
                chunks_sent,
                bytes_sent
            );
            object_count += 1;
        }

        tracing::info!(
            "[PUBLISHER] serve_subgroup: completed subgroup (group_id={}, subgroup_id={:?}, {} objects sent)",
            subgroup_reader.group_id,
            subgroup_reader.subgroup_id,
            object_count
        );

        Ok(())
    }

    async fn serve_datagrams(
        &mut self,
        mut datagrams: serve::DatagramsReader,
    ) -> Result<(), SessionError> {
        tracing::debug!("[PUBLISHER] serve_datagrams: starting");
        let effective_delivery_timeout = self.state.lock().effective_delivery_timeout;

        let mut datagram_count = 0;
        while let Some(datagram) = datagrams.read().await? {
            if is_expired(datagram.received_at, effective_delivery_timeout) {
                tracing::debug!(
                    "[PUBLISHER] serve_datagrams: dropping expired datagram group_id={} object_id={}",
                    datagram.group_id,
                    datagram.object_id
                );
                continue;
            }

            // Determine datagram type based on extension headers presence
            let has_extension_headers = !datagram.extension_headers.is_empty();
            let datagram_type = if has_extension_headers {
                data::DatagramType::ObjectIdPayloadExt
            } else {
                data::DatagramType::ObjectIdPayload
            };

            let encoded_datagram = data::Datagram {
                datagram_type,
                track_alias: self.info.id, // use subscription id as track_alias
                group_id: datagram.group_id,
                object_id: Some(datagram.object_id),
                publisher_priority: datagram.priority,
                extension_headers: if has_extension_headers {
                    Some(datagram.extension_headers.clone())
                } else {
                    None
                },
                status: None,
                payload: Some(datagram.payload),
            };

            let payload_len = encoded_datagram
                .payload
                .as_ref()
                .map(|p| p.len())
                .unwrap_or(0);
            let mut buffer = bytes::BytesMut::with_capacity(payload_len + 100);
            encoded_datagram.encode(&mut buffer)?;

            tracing::debug!(
                "[PUBLISHER] serve_datagrams: sending datagram #{} - track_alias={}, group_id={}, object_id={}, priority={}, payload_len={}, extension_headers={:?}, total_encoded_len={}",
                datagram_count + 1,
                encoded_datagram.track_alias,
                encoded_datagram.group_id,
                encoded_datagram.object_id.unwrap(),
                encoded_datagram.publisher_priority,
                payload_len,
                encoded_datagram.extension_headers,
                buffer.len()
            );

            // Create mlog event for datagram created
            if let Some(ref mlog) = self.mlog {
                if let Ok(mut mlog_guard) = mlog.lock() {
                    let time = mlog_guard.elapsed_ms();
                    let stream_id = 0; // TODO: Placeholder, need actual QUIC stream ID
                    let _ = mlog_guard.add_event(mlog::object_datagram_created(
                        time,
                        stream_id,
                        &encoded_datagram,
                    ));
                }
            }

            self.publisher.send_datagram(buffer.into()).await?;

            self.state
                .lock_mut()
                .ok_or(ServeError::Done)?
                .update_largest_location(
                    encoded_datagram.group_id,
                    encoded_datagram.object_id.unwrap(),
                )?;

            datagram_count += 1;
        }

        tracing::info!(
            "[PUBLISHER] serve_datagrams: completed ({} datagrams sent)",
            datagram_count
        );

        Ok(())
    }
}

pub(super) struct SubscribedRecv {
    state: State<SubscribedState>,
}

impl SubscribedRecv {
    pub fn recv_unsubscribe(&mut self) -> Result<(), ServeError> {
        let state = self.state.lock();
        state.closed.clone()?;

        if let Some(mut state) = state.into_mut() {
            state.closed = Err(ServeError::Cancel);
        }

        Ok(())
    }
}

fn parse_delivery_timeout_param(params: &crate::coding::KeyValuePairs) -> Option<Duration> {
    let kvp = params.get(DELIVERY_TIMEOUT_PARAM)?;
    let crate::coding::Value::IntValue(timeout_ms) = &kvp.value else {
        tracing::warn!("ignoring non-integer DELIVERY_TIMEOUT parameter");
        return None;
    };

    if *timeout_ms == 0 {
        return None;
    }

    Some(Duration::from_millis(*timeout_ms))
}

fn subscribe_ok_params(effective_delivery_timeout: Option<Duration>) -> crate::coding::KeyValuePairs {
    let mut params = crate::coding::KeyValuePairs::default();
    if let Some(timeout) = effective_delivery_timeout {
        let timeout_ms = timeout.as_millis().min(u128::from(u64::MAX)) as u64;
        if timeout_ms > 0 {
            params.set_intvalue(DELIVERY_TIMEOUT_PARAM, timeout_ms);
        }
    }
    params
}

fn delivery_timeout_reset_error() -> SessionError {
    SessionError::Serve(ServeError::Closed(DELIVERY_TIMEOUT_RESET_CODE))
}

fn is_expired(received_at: Instant, effective_delivery_timeout: Option<Duration>) -> bool {
    let Some(timeout) = effective_delivery_timeout else {
        return false;
    };

    received_at.elapsed() >= timeout
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coding::KeyValuePairs;

    #[test]
    fn parse_delivery_timeout_param_int_value() {
        let mut params = KeyValuePairs::default();
        params.set_intvalue(DELIVERY_TIMEOUT_PARAM, 1234);

        assert_eq!(
            parse_delivery_timeout_param(&params),
            Some(Duration::from_millis(1234))
        );
    }

    #[test]
    fn parse_delivery_timeout_param_zero_is_none() {
        let mut params = KeyValuePairs::default();
        params.set_intvalue(DELIVERY_TIMEOUT_PARAM, 0);

        assert_eq!(parse_delivery_timeout_param(&params), None);
    }

    #[test]
    fn parse_delivery_timeout_param_non_integer_is_none() {
        let mut params = KeyValuePairs::default();
        params.set_bytesvalue(DELIVERY_TIMEOUT_PARAM, vec![1, 2, 3]);

        assert_eq!(parse_delivery_timeout_param(&params), None);
    }

    #[test]
    fn subscribe_ok_params_roundtrip_timeout() {
        let params = subscribe_ok_params(Some(Duration::from_millis(2500)));
        let parsed = parse_delivery_timeout_param(&params);

        assert_eq!(parsed, Some(Duration::from_millis(2500)));
    }

    #[test]
    fn is_expired_respects_timeout() {
        let old = Instant::now() - Duration::from_millis(20);
        let recent = Instant::now();

        assert!(is_expired(old, Some(Duration::from_millis(10))));
        assert!(!is_expired(recent, Some(Duration::from_secs(1))));
        assert!(!is_expired(old, None));
    }

    #[test]
    fn mid_object_timeout_maps_to_delivery_timeout_reset_code() {
        assert!(matches!(
            delivery_timeout_reset_error(),
            SessionError::Serve(ServeError::Closed(DELIVERY_TIMEOUT_RESET_CODE))
        ));
    }
}
