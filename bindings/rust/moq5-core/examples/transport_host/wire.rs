//! An in-memory transport: QUIC-like streams and datagrams between two
//! sides, with the knobs a test needs (write and read limits, refusals of
//! bytes, of one stream and of resets, held FINs, a side that reads
//! nothing, a receive side dropped after a stop) and, optionally, seeded
//! fragmentation. Deterministic: no clock, no threads.

use std::collections::VecDeque;

/// Bytes in flight per stream direction.
pub const PIPE_CAP: usize = 4096;
/// Datagrams in flight per direction.
pub const DGRAM_SLOTS: usize = 8;

/// A small deterministic generator (xorshift64*), for seeded fragmentation.
#[derive(Clone, Debug)]
pub struct Rng(u64);

impl Rng {
    /// A generator from a nonzero seed (zero is mapped to one).
    pub fn new(seed: u64) -> Rng {
        Rng(seed.max(1))
    }

    /// The next value.
    pub fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    /// A value in `1..=max` (`max` at least 1).
    pub fn between_one_and(&mut self, max: usize) -> usize {
        let max = max.max(1) as u64;
        (1 + self.next() % max) as usize
    }
}

/// One direction of one stream.
#[derive(Debug, Default)]
pub struct Pipe {
    pub buf: VecDeque<u8>,
    /// Bytes the reader has taken so far.
    pub read_off: u64,
    pub fin: bool,
    /// The writer finished but the transport holds the FIN back.
    pub fin_held: bool,
    pub fin_read: bool,
    pub reset: Option<u64>,
    pub reset_read: bool,
    /// The reader told the writer to stop.
    pub stop: Option<u64>,
    pub stop_read: bool,
    /// The reader's transport dropped this direction after its stop.
    pub dropped: bool,
    pub retire_read: bool,
    /// Writes the transport took bytes of.
    pub writes: u32,
    /// Bytes the transport took, in all.
    pub taken: u64,
    /// When the FIN and the reset were taken, in the wire's sequence.
    pub fin_at: Option<u64>,
    pub reset_at: Option<u64>,
    /// The reliable size the reset declared.
    pub reset_reliable: Option<u64>,
}

#[derive(Debug)]
pub struct WireStream {
    pub bidi: bool,
    pub opener: usize,
    /// `dir[x]`: what side x writes.
    pub dir: [Pipe; 2],
}

/// The transport between side 0 and side 1.
#[derive(Debug, Default)]
pub struct Wire {
    pub streams: Vec<WireStream>,
    pub max_streams: usize,
    /// Most bytes one write takes (0: no limit).
    pub write_limit: usize,
    /// Most bytes one read returns (0: no limit).
    pub read_limit: usize,
    /// A receive side its reader stopped is dropped: nothing more arrives,
    /// and the transport reports the stream finished instead.
    pub drop_after_stop: bool,
    /// Refuse every stream byte side x writes (a FIN with no byte and
    /// resets are still taken).
    pub refuse_bytes: [bool; 2],
    pub refuse_datagrams: [bool; 2],
    /// Hold back every FIN side x writes.
    pub fin_hold: [bool; 2],
    /// Streams on which side x's bytes and FIN are refused (its resets are
    /// still taken), and whether the next stream side x opens joins them.
    pub refuse_stream: [Vec<usize>; 2],
    pub refuse_next_stream: [bool; 2],
    /// Bytes a refused stream of side x still takes before its refusal
    /// applies (0: refused from its first byte). Its FIN is refused either
    /// way.
    pub refusal_allowance: [u64; 2],
    /// Data stream resets side x writes that are refused, counted down, and
    /// how many were. Resets of bidirectional streams are always taken.
    pub refuse_resets: [u32; 2],
    pub resets_refused: [u32; 2],
    /// Side x reads nothing: no byte, end, stop or datagram.
    pub hold_input: [bool; 2],
    /// The order FINs and resets were taken in.
    pub seq: u64,
    /// The code side x closed the transport with.
    pub closed: [Option<u64>; 2],
    /// Datagrams sent by side x, oldest first.
    pub dgrams: [VecDeque<Vec<u8>>; 2],
    /// Resets side x's halves took, and repeats ignored.
    pub resets: [u32; 2],
    pub reset_repeats: [u32; 2],
    pub last_reset_code: [u64; 2],
    /// Resets side x wrote declaring a reliable size larger than the bytes
    /// the transport took: a prefix promised and never written.
    pub overclaimed: [u32; 2],
    /// Seeded fragmentation of writes and reads.
    pub rng: Option<Rng>,
}

impl Wire {
    pub fn new(max_streams: usize) -> Wire {
        Wire {
            max_streams,
            ..Wire::default()
        }
    }

    /// Open a stream; `None` when the transport has no stream left.
    pub fn open(&mut self, opener: usize, bidi: bool) -> Option<usize> {
        if self.streams.len() == self.max_streams {
            return None;
        }
        self.streams.push(WireStream {
            bidi,
            opener,
            dir: [Pipe::default(), Pipe::default()],
        });
        let index = self.streams.len() - 1;
        if self.refuse_next_stream[opener] {
            self.refuse_next_stream[opener] = false;
            self.refuse_stream[opener].push(index);
        }
        Some(index)
    }

    fn limit(&mut self, configured: usize, wanted: usize) -> usize {
        let mut take = wanted;
        if configured != 0 {
            take = take.min(configured);
        }
        if let Some(rng) = &mut self.rng {
            take = take.min(rng.between_one_and(wanted.max(1)));
        }
        take
    }

    /// How many of the bytes (the concatenation of `parts`) the transport
    /// takes now from `side` on stream `index`.
    pub fn write(&mut self, index: usize, side: usize, parts: [&[u8]; 2]) -> usize {
        let total = parts[0].len() + parts[1].len();
        let mut room = total;
        {
            let pipe = &self.streams[index].dir[side];
            if pipe.fin || pipe.fin_held || pipe.reset.is_some() || self.refuse_bytes[side] {
                return 0;
            }
            if self.refuse_stream[side].contains(&index) {
                let left = self.refusal_allowance[side].saturating_sub(pipe.taken);
                if left == 0 {
                    return 0;
                }
                room = room.min(usize::try_from(left).unwrap_or(usize::MAX));
            }
        }
        let mut take = self.limit(self.write_limit, room);
        let pipe = &mut self.streams[index].dir[side];
        if take > 0 {
            pipe.writes += 1;
        }
        if !pipe.dropped {
            take = take.min(PIPE_CAP - pipe.buf.len());
            let first = take.min(parts[0].len());
            pipe.buf.extend(&parts[0][..first]);
            pipe.buf.extend(&parts[1][..take - first]);
        } // else accepted, never delivered
        pipe.taken += take as u64;
        take
    }

    /// Take a FIN; false when the stream refuses it.
    pub fn fin(&mut self, index: usize, side: usize) -> bool {
        if self.refuse_stream[side].contains(&index) {
            return false;
        }
        let held = self.fin_hold[side];
        self.seq += 1;
        let seq = self.seq;
        let pipe = &mut self.streams[index].dir[side];
        if pipe.reset.is_some() {
            return true;
        }
        if held {
            pipe.fin_held = true;
        } else {
            pipe.fin = true;
            pipe.fin_at = Some(seq);
        }
        true
    }

    /// A reset a side writes, which the transport may refuse; false when
    /// it did.
    pub fn try_reset(&mut self, index: usize, side: usize, code: u64, reliable: u64) -> bool {
        let pipe = &self.streams[index].dir[side];
        let ended = (pipe.fin && pipe.buf.is_empty() && pipe.fin_read) || pipe.reset.is_some();
        if self.refuse_resets[side] > 0 && !ended && !self.streams[index].bidi {
            self.refuse_resets[side] -= 1;
            self.resets_refused[side] += 1;
            return false;
        }
        self.reset(index, side, code, reliable);
        true
    }

    /// A reset abandons what the reader has not read, except the leading
    /// `reliable` bytes of the stream, which are still delivered first.
    pub fn reset(&mut self, index: usize, side: usize, code: u64, reliable: u64) {
        let pipe = &mut self.streams[index].dir[side];
        if pipe.fin && pipe.buf.is_empty() && pipe.fin_read {
            return; // already ended
        }
        if pipe.reset.is_some() {
            self.reset_repeats[side] += 1;
            return;
        }
        self.resets[side] += 1;
        self.last_reset_code[side] = code;
        if reliable > pipe.taken {
            self.overclaimed[side] += 1;
        }
        self.seq += 1;
        pipe.reset_at = Some(self.seq);
        pipe.reset = Some(code);
        pipe.reset_reliable = Some(reliable);
        pipe.fin = false;
        pipe.fin_held = false;
        let keep = reliable.saturating_sub(pipe.read_off);
        if keep < pipe.buf.len() as u64 {
            pipe.buf.truncate(keep as usize);
        }
    }

    /// The reader of the direction `1 - reader` writes asks its writer to stop.
    pub fn stop(&mut self, index: usize, reader: usize, code: u64) {
        let drop = self.drop_after_stop;
        let pipe = &mut self.streams[index].dir[1 - reader];
        if pipe.stop.is_some() {
            return;
        }
        pipe.stop = Some(code);
        if drop {
            pipe.dropped = true;
            pipe.buf.clear();
        }
    }

    /// Read what the peer of `reader` wrote, at most `cap` bytes.
    pub fn read(&mut self, index: usize, reader: usize, cap: usize) -> Vec<u8> {
        let available = self.streams[index].dir[1 - reader].buf.len();
        if self.streams[index].dir[1 - reader].dropped || available == 0 || cap == 0 {
            return Vec::new();
        }
        let take = self.limit(self.read_limit, available.min(cap));
        let pipe = &mut self.streams[index].dir[1 - reader];
        pipe.read_off += take as u64;
        pipe.buf.drain(..take).collect()
    }

    /// Release the FINs `side` wrote while held, except on a direction its
    /// reader has since dropped.
    pub fn release_fins(&mut self, side: usize) {
        self.fin_hold[side] = false;
        for stream in &mut self.streams {
            let pipe = &mut stream.dir[side];
            if pipe.fin_held {
                pipe.fin_held = false;
                if pipe.reset.is_none() && !pipe.dropped {
                    self.seq += 1;
                    pipe.fin = true;
                    pipe.fin_at = Some(self.seq);
                }
            }
        }
    }

    /// Send a datagram from `side`; false when the transport refused it.
    pub fn send_datagram(&mut self, side: usize, bytes: &[u8]) -> Option<bool> {
        if self.refuse_datagrams[side] {
            return None;
        }
        if self.dgrams[side].len() == DGRAM_SLOTS {
            return Some(false); // dropped in flight
        }
        self.dgrams[side].push_back(bytes.to_vec());
        Some(true)
    }
}
