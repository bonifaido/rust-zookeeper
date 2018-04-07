// Copyright (c) 2014 Carl Lerche and other MIO contributors
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

//! TryRead and TryWrite traits from mio 0.5.1

use std::io::{Read, Write, Result};
use bytes::{Buf, MutBuf};

pub trait TryRead {
    fn try_read_buf<B: MutBuf>(&mut self, buf: &mut B) -> Result<Option<usize>>
        where Self : Sized
    {
        // Reads the length of the slice supplied by buf.mut_bytes into the buffer
        // This is not guaranteed to consume an entire datagram or segment.
        // If your protocol is msg based (instead of continuous stream) you should
        // ensure that your buffer is large enough to hold an entire segment (1532 bytes if not jumbo
        // frames)
        let res = self.try_read(unsafe { buf.mut_bytes() });

        if let Ok(Some(cnt)) = res {
            unsafe { buf.advance(cnt); }
        }

        res
    }

    fn try_read(&mut self, buf: &mut [u8]) -> Result<Option<usize>>;
}

pub trait TryWrite {
    fn try_write_buf<B: Buf>(&mut self, buf: &mut B) -> Result<Option<usize>>
        where Self : Sized
    {
        let res = self.try_write(buf.bytes());

        if let Ok(Some(cnt)) = res {
            buf.advance(cnt);
        }

        res
    }

    fn try_write(&mut self, buf: &[u8]) -> Result<Option<usize>>;
}

impl<T: Read> TryRead for T {
    fn try_read(&mut self, dst: &mut [u8]) -> Result<Option<usize>> {
        self.read(dst).map_non_block()
    }
}

impl<T: Write> TryWrite for T {
    fn try_write(&mut self, src: &[u8]) -> Result<Option<usize>> {
        self.write(src).map_non_block()
    }
}

pub trait TryAccept {
    type Output;

    fn accept(&self) -> Result<Option<Self::Output>>;
}

/*
 *
 * ===== Helpers =====
 *
 */

/// A helper trait to provide the map_non_block function on Results.
pub trait MapNonBlock<T> {
    /// Maps a `Result<T>` to a `Result<Option<T>>` by converting
    /// operation-would-block errors into `Ok(None)`.
    fn map_non_block(self) -> Result<Option<T>>;
}

impl<T> MapNonBlock<T> for Result<T> {
    fn map_non_block(self) -> Result<Option<T>> {
        use std::io::ErrorKind::WouldBlock;

        match self {
            Ok(value) => Ok(Some(value)),
            Err(err) => {
                if let WouldBlock = err.kind() {
                    Ok(None)
                } else {
                    Err(err)
                }
            }
        }
    }
}
