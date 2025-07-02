use std::io;
use std::pin::Pin;
use std::str::FromStr;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};

/// Defines the target line ending format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndOfLine {
    /// Line Feed (`\n`).
    Lf,
    /// Carriage Return + Line Feed (`\r\n`).
    Crlf,
}

impl EndOfLine {
    /// Returns the byte representation of the line ending.
    fn as_bytes(&self) -> &'static [u8] {
        match self {
            EndOfLine::Lf => b"\n",
            EndOfLine::Crlf => b"\r\n",
        }
    }
}

impl FromStr for EndOfLine {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "lf" => Ok(EndOfLine::Lf),
            "crlf" => Ok(EndOfLine::Crlf),
            _ => {
                eprintln!("Unknown EOL: {s}");
                Err(())
            }
        }
    }
}

/// An `AsyncRead` adapter that normalizes various line endings to a target format.
///
/// This struct wraps an existing `AsyncRead` stream and transforms input line endings
/// such as LF (`\n`), CR (`\r`), and CRLF (`\r\n`) into the configured `EndOfLine` format.
/// It correctly handles line endings that are split across buffer boundaries.
#[derive(Debug)]
pub struct EndOfLineReadTransformer<R: AsyncRead + Unpin> {
    /// The inner reader.
    inner: R,
    /// The target line ending as a byte slice (e.g., b"\n" or b"\r\n").
    target: &'static [u8],
    /// This buffer holds data that has been read from the inner reader and transformed,
    /// but not yet copied to the output buffer.
    internal_buf: Vec<u8>,
    /// The position in `internal_buf` from which the next read should start.
    pos: usize,
    /// State flag to track if the last byte processed in the previous read
    /// was a Carriage Return (CR). This is crucial for correctly handling CRLF pairs
    /// that span across two separate reads.
    saw_cr: bool,
}

impl<R: AsyncRead + Unpin> EndOfLineReadTransformer<R> {
    /// Creates a new `EndOfLineTransformer`.
    ///
    /// # Arguments
    ///
    /// * `inner` - The async reader to wrap.
    /// * `target` - The desired line ending format for the output.
    pub fn new(inner: R, target: EndOfLine) -> Self {
        EndOfLineReadTransformer {
            inner,
            target: target.as_bytes(),
            internal_buf: Vec::with_capacity(1024),
            pos: 0,
            saw_cr: false,
        }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for EndOfLineReadTransformer<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // If we have processed all data in our internal buffer, it's time to read more.
        if self.pos >= self.internal_buf.len() {
            // Clear the internal buffer to save memory and reset position.
            self.internal_buf.clear();
            self.pos = 0;

            // Use the user-provided buffer's unfilled space as a temporary place to read into.
            // This is an optimization to avoid allocating a new temporary buffer on every read.
            let mut temp_read_buf = ReadBuf::new(buf.initialize_unfilled());

            match Pin::new(&mut self.inner).poll_read(cx, &mut temp_read_buf) {
                Poll::Ready(Ok(())) => {
                    let read_data = temp_read_buf.filled();
                    if read_data.is_empty() {
                        // EOF. If we last saw a CR, it was a lone CR at the end of the stream.
                        // The separator for it was already written.
                        return Poll::Ready(Ok(()));
                    }

                    // Process the data we just read, normalizing line endings.
                    for &byte in read_data {
                        if self.saw_cr {
                            // The previous read ended with a CR.
                            self.saw_cr = false;
                            if byte == b'\n' {
                                // This is the LF of a CRLF pair. The separator was already
                                // written when we saw the CR, so we just consume this LF.
                                continue;
                            }
                            // It was a lone CR followed by some other character. The separator
                            // for the CR was already written. Now we process the current byte.
                        }

                        match byte {
                            b'\r' => {
                                // This is a line ending. Write the target separator.
                                let target_eol = self.target;
                                self.internal_buf.extend_from_slice(target_eol);
                                // Set state in case this CR is the last byte of the chunk.
                                self.saw_cr = true;
                            }
                            b'\n' => {
                                // This is a standalone LF. The state machine ensures it wasn't
                                // preceded by a CR that we are tracking.
                                let target_eol = self.target;
                                self.internal_buf.extend_from_slice(target_eol);
                            }
                            _ => {
                                // Any other character.
                                self.internal_buf.push(byte);
                            }
                        }
                        // If we see any character other than CR, the saw_cr state is implicitly false
                        // for the *next* character, so we reset it here.
                        if byte != b'\r' {
                            self.saw_cr = false;
                        }
                    }
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }

        // Now, copy from our processed internal buffer to the output buffer.
        let bytes_to_copy = std::cmp::min(buf.remaining(), self.internal_buf.len() - self.pos);
        if bytes_to_copy > 0 {
            buf.put_slice(&self.internal_buf[self.pos..self.pos + bytes_to_copy]);
            self.pos += bytes_to_copy;
        }

        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::{EndOfLine, EndOfLineReadTransformer};
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::ReadBuf;
    use tokio::io::{AsyncRead, AsyncReadExt};

    /// A helper reader that returns data in small, controlled chunks to test boundary conditions.
    struct ChunkedReader<'a> {
        data: &'a [u8],
        chunk_size: usize,
        pos: usize,
    }

    impl<'a> AsyncRead for ChunkedReader<'a> {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            let bytes_to_read = std::cmp::min(self.chunk_size, self.data.len() - self.pos);
            if bytes_to_read > 0 {
                let end = self.pos + bytes_to_read;
                buf.put_slice(&self.data[self.pos..end]);
                self.pos = end;
            }
            Poll::Ready(Ok(()))
        }
    }

    async fn test_transform(input: &[u8], target: EndOfLine, expected: &[u8]) {
        let mut reader = EndOfLineReadTransformer::new(input, target);
        let mut output = Vec::new();
        reader.read_to_end(&mut output).await.unwrap();
        assert_eq!(output, expected);
    }

    async fn test_transform_chunked(
        input: &[u8],
        target: EndOfLine,
        expected: &[u8],
        chunk_size: usize,
    ) {
        let chunked_reader = ChunkedReader {
            data: input,
            chunk_size,
            pos: 0,
        };
        let mut reader = EndOfLineReadTransformer::new(chunked_reader, target);
        let mut output = Vec::new();
        reader.read_to_end(&mut output).await.unwrap();
        assert_eq!(output, expected, "Failed with chunk size {}", chunk_size);
    }

    // --- Tests for LF output ---
    #[tokio::test]
    async fn lf_to_lf() {
        test_transform(b"a\nb\nc", EndOfLine::Lf, b"a\nb\nc").await;
    }

    #[tokio::test]
    async fn crlf_to_lf() {
        test_transform(b"a\r\nb\r\nc", EndOfLine::Lf, b"a\nb\nc").await;
    }

    #[tokio::test]
    async fn cr_to_lf() {
        test_transform(b"a\rb\rc", EndOfLine::Lf, b"a\nb\nc").await;
    }

    #[tokio::test]
    async fn mixed_to_lf() {
        test_transform(b"a\nb\r\nc\rd\n\r\ne", EndOfLine::Lf, b"a\nb\nc\nd\ne").await;
    }

    // --- Tests for CRLF output ---
    #[tokio::test]
    async fn lf_to_crlf() {
        test_transform(b"a\nb\nc", EndOfLine::Crlf, b"a\r\nb\r\nc").await;
    }

    #[tokio::test]
    async fn crlf_to_crlf() {
        test_transform(b"a\r\nb\r\nc", EndOfLine::Crlf, b"a\r\nb\r\nc").await;
    }

    #[tokio::test]
    async fn cr_to_crlf() {
        test_transform(b"a\rb\rc", EndOfLine::Crlf, b"a\r\nb\r\nc").await;
    }

    #[tokio::test]
    async fn mixed_to_crlf() {
        test_transform(
            b"a\nb\r\nc\rd\n\r\ne",
            EndOfLine::Crlf,
            b"a\r\nb\r\nc\r\nd\r\ne",
        )
        .await;
    }

    // --- Boundary condition tests ---
    #[tokio::test]
    async fn cr_at_end_of_stream() {
        test_transform(b"hello\r", EndOfLine::Lf, b"hello\n").await;
        test_transform(b"hello\r", EndOfLine::Crlf, b"hello\r\n").await;
    }

    #[tokio::test]
    async fn crlf_split_across_reads() {
        let input = b"hello\r\nworld";
        // Test with various chunk sizes to ensure the split happens at different places.
        for i in 1..input.len() {
            test_transform_chunked(input, EndOfLine::Lf, b"hello\nworld", i).await;
            test_transform_chunked(input, EndOfLine::Crlf, b"hello\r\nworld", i).await;
        }
    }

    #[tokio::test]
    async fn cr_at_end_of_chunk() {
        let input = b"a\rb";
        test_transform_chunked(input, EndOfLine::Lf, b"a\nb", 2).await;
        test_transform_chunked(input, EndOfLine::Crlf, b"a\r\nb", 2).await;
    }
}
