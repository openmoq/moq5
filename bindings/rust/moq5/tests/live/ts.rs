//! A generated single-program MPEG-2 transport stream for the MPEG-TS
//! rows, built packet by packet as the service's own tests build theirs
//! (`service/tests/ts_stream.h` over `media/mpeg2ts/tests/ts_builders.h`,
//! written from ISO/IEC 13818-1 field by field). Each GOP is: the PAT, the
//! PMT, a video PES start carrying the random access indicator and a PCR,
//! `video_cont` video continuation packets, and one audio PES packet.
//! Continuity counters advance per PID; 192-octet packets carry a
//! four-octet prefix that counts up.

#![allow(dead_code)]

pub const PAT_PID: u16 = 0x0000;
pub const PMT_PID: u16 = 0x0100;
pub const VIDEO_PID: u16 = 0x0101;
pub const AUDIO_PID: u16 = 0x0102;

/// The bitwise CRC_32/MPEG-2 of PSI sections.
fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for byte in data {
        crc ^= u32::from(*byte) << 24;
        for _ in 0..8 {
            crc = if crc & 0x8000_0000 != 0 {
                (crc << 1) ^ 0x04C1_1DB7
            } else {
                crc << 1
            };
        }
    }
    crc
}

/// A long-form section: `body` from table_id up to the CRC, with its
/// section_length filled in and the CRC appended.
fn finish_section(mut body: Vec<u8>) -> Vec<u8> {
    let section_length = body.len() - 3 + 4;
    body[1] = (body[1] & 0xF0) | ((section_length >> 8) & 0x0F) as u8;
    body[2] = section_length as u8;
    let crc = crc32(&body);
    body.extend_from_slice(&crc.to_be_bytes());
    body
}

fn pat(transport_stream_id: u16, program: u16, pmt_pid: u16) -> Vec<u8> {
    let mut sec = vec![0x00, 0xB0, 0x00];
    sec.extend_from_slice(&transport_stream_id.to_be_bytes());
    sec.extend_from_slice(&[0xC1, 0x00, 0x00]);
    sec.extend_from_slice(&program.to_be_bytes());
    sec.push(0xE0 | ((pmt_pid >> 8) & 0x1F) as u8);
    sec.push(pmt_pid as u8);
    finish_section(sec)
}

fn pmt(program: u16, pcr_pid: u16, streams: &[(u8, u16)]) -> Vec<u8> {
    let mut sec = vec![0x02, 0xB0, 0x00];
    sec.extend_from_slice(&program.to_be_bytes());
    sec.extend_from_slice(&[0xC1, 0x00, 0x00]);
    sec.push(0xE0 | ((pcr_pid >> 8) & 0x1F) as u8);
    sec.push(pcr_pid as u8);
    sec.extend_from_slice(&[0xF0, 0x00]);
    for (stream_type, pid) in streams {
        sec.push(*stream_type);
        sec.push(0xE0 | ((pid >> 8) & 0x1F) as u8);
        sec.push(*pid as u8);
        sec.extend_from_slice(&[0xF0, 0x00]);
    }
    finish_section(sec)
}

/// A PTS or DTS field with its four-bit prefix.
fn timestamp(prefix: u8, ts: u64) -> [u8; 5] {
    [
        (prefix << 4) | ((((ts >> 30) & 7) as u8) << 1) | 1,
        (ts >> 22) as u8,
        ((((ts >> 15) & 0x7F) as u8) << 1) | 1,
        (ts >> 7) as u8,
        (((ts & 0x7F) as u8) << 1) | 1,
    ]
}

/// A PES header start with its PTS (and DTS), then a few octets of
/// elementary stream data.
fn pes(stream_id: u8, pts: u64, dts: Option<u64>) -> Vec<u8> {
    let mut out = vec![0x00, 0x00, 0x01, stream_id, 0x00, 0x00, 0x80];
    out.push(0x80 | if dts.is_some() { 0x40 } else { 0 });
    out.push(if dts.is_some() { 10 } else { 5 });
    out.extend_from_slice(&timestamp(if dts.is_some() { 3 } else { 2 }, pts));
    if let Some(dts) = dts {
        out.extend_from_slice(&timestamp(1, dts));
    }
    out.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]);
    out
}

/// One packet's header fields.
#[derive(Default)]
struct Packet<'a> {
    pid: u16,
    pusi: bool,
    rai: bool,
    pcr: Option<u64>,
    payload: &'a [u8],
}

pub struct Generator {
    packet_size: usize,
    program: u16,
    cc: Vec<u8>,
    pcr_base: u64,
    prefix_clock: u32,
}

impl Generator {
    /// A stream of `packet_size` (188 or 192) octet packets for `program`.
    pub fn new(packet_size: usize, program: u16) -> Generator {
        assert!(packet_size == 188 || packet_size == 192);
        Generator {
            packet_size,
            program,
            cc: vec![0; 0x2000],
            pcr_base: 900_000,
            prefix_clock: 0,
        }
    }

    pub fn packet_size(&self) -> usize {
        self.packet_size
    }

    fn put(&mut self, out: &mut Vec<u8>, packet: Packet<'_>) {
        let cc = self.cc[usize::from(packet.pid)] & 0x0F;
        self.cc[usize::from(packet.pid)] = self.cc[usize::from(packet.pid)].wrapping_add(1);
        if self.packet_size == 192 {
            out.extend_from_slice(&self.prefix_clock.to_be_bytes());
            self.prefix_clock += 1;
        }
        let mut p = [0xFFu8; 188];
        let adaptation = packet.rai || packet.pcr.is_some();
        let control: u8 = if adaptation { 3 } else { 1 };
        p[0] = 0x47;
        p[1] = (if packet.pusi { 0x40 } else { 0 }) | ((packet.pid >> 8) & 0x1F) as u8;
        p[2] = packet.pid as u8;
        p[3] = (control << 4) | cc;
        let mut offset = 4;
        if adaptation {
            let length = 1 + if packet.pcr.is_some() { 6 } else { 0 };
            p[4] = length as u8;
            p[5] =
                (if packet.rai { 0x40 } else { 0 }) | (if packet.pcr.is_some() { 0x10 } else { 0 });
            if let Some(base) = packet.pcr {
                p[6] = (base >> 25) as u8;
                p[7] = (base >> 17) as u8;
                p[8] = (base >> 9) as u8;
                p[9] = (base >> 1) as u8;
                p[10] = (((base & 1) as u8) << 7) | 0x7E;
                p[11] = 0;
            }
            offset = 5 + length;
        }
        let n = packet.payload.len().min(188 - offset);
        p[offset..offset + n].copy_from_slice(&packet.payload[..n]);
        out.extend_from_slice(&p);
    }

    /// The PAT and the PMT: the stream's PSI, as an initialization-data run.
    pub fn psi(&mut self) -> Vec<u8> {
        let mut out = Vec::new();
        let mut payload = vec![0x00];
        payload.extend_from_slice(&pat(1, self.program, PMT_PID));
        self.put(
            &mut out,
            Packet {
                pid: PAT_PID,
                pusi: true,
                payload: &payload,
                ..Packet::default()
            },
        );
        let mut payload = vec![0x00];
        payload.extend_from_slice(&pmt(
            self.program,
            VIDEO_PID,
            &[(0x1B, VIDEO_PID), (0x0F, AUDIO_PID)],
        ));
        self.put(
            &mut out,
            Packet {
                pid: PMT_PID,
                pusi: true,
                payload: &payload,
                ..Packet::default()
            },
        );
        out
    }

    /// One GOP of `video_cont` video continuation packets: `4 + video_cont`
    /// packets, one second of PCR time.
    pub fn gop(&mut self, video_cont: usize) -> Vec<u8> {
        let mut out = self.psi();
        let video = pes(0xE0, self.pcr_base + 9000, Some(self.pcr_base + 6000));
        let pcr = self.pcr_base;
        self.put(
            &mut out,
            Packet {
                pid: VIDEO_PID,
                pusi: true,
                rai: true,
                pcr: Some(pcr),
                payload: &video,
            },
        );
        for i in 0..video_cont {
            let es = [0x11, i as u8, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77];
            self.put(
                &mut out,
                Packet {
                    pid: VIDEO_PID,
                    payload: &es,
                    ..Packet::default()
                },
            );
        }
        let audio = pes(0xC0, self.pcr_base + 9000, None);
        self.put(
            &mut out,
            Packet {
                pid: AUDIO_PID,
                pusi: true,
                payload: &audio,
                ..Packet::default()
            },
        );
        self.pcr_base += 90_000;
        out
    }
}

/// The PID of a 188-octet packet.
pub fn pid(packet: &[u8]) -> u16 {
    (u16::from(packet[1] & 0x1F) << 8) | u16::from(packet[2])
}

/// The packets of `data`, each without the four-octet prefix of a
/// 192-octet source packet.
pub fn packets(data: &[u8], packet_size: usize) -> Vec<&[u8]> {
    data.chunks(packet_size)
        .map(|chunk| &chunk[packet_size - 188..])
        .collect()
}
