import io

class PartialStream(io.IOBase):
    """
    A stream wrapper that provides a limited view of another stream,
    reading only a specified number of bytes from a starting position.
    """
    
    def __init__(self, src: io.IOBase, max_bytes: int):
        """
        Initialize a PartialStream.
        
        Args:
            src: The source stream to wrap
            max_bytes: Maximum number of bytes to read from the source stream
        """
        self._src = src
        self._start_pos = src.tell()
        self._max_bytes = max_bytes
    
    @property
    def readable(self) -> bool:
        """Returns True if the stream supports reading."""
        return self._src.readable()
    
    @property
    def writable(self) -> bool:
        """Returns True if the stream supports writing."""
        return self._src.writable()
    
    @property
    def seekable(self) -> bool:
        """Returns True if the stream supports seeking."""
        return self._src.seekable()
    
    def tell(self) -> int:
        """Return the current position relative to the start position."""
        return self._src.tell() - self._start_pos
    
    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        """
        Seek to a position in the partial stream.
        
        Args:
            offset: Offset position
            whence: How to interpret the offset (SEEK_SET, SEEK_CUR, SEEK_END)
        
        Returns:
            The new absolute position
        """
        if whence == io.SEEK_SET:
            return self._src.seek(self._start_pos + offset, whence)
        elif whence == io.SEEK_CUR:
            return self._src.seek(offset, whence)
        elif whence == io.SEEK_END:
            # Seek from end of the partial stream
            return self._src.seek(self._start_pos + self._get_length() + offset, io.SEEK_SET)
        else:
            raise ValueError(f"Invalid whence value: {whence}")
    
    def _get_length(self) -> int:
        """Get the effective length of this partial stream."""
        try:
            # Try to get the source stream length
            current_pos = self._src.tell()
            self._src.seek(0, io.SEEK_END)
            src_length = self._src.tell()
            self._src.seek(current_pos)  # Restore position
            
            available_bytes = src_length - self._start_pos
            return min(self._max_bytes, max(0, available_bytes))
        except (io.UnsupportedOperation, OSError):
            # If we can't determine length, assume max_bytes
            return self._max_bytes
    
    def read(self, size: int = -1) -> bytes:
        """
        Read up to size bytes from the partial stream.
        
        Args:
            size: Maximum number of bytes to read (-1 for all available)
        
        Returns:
            Bytes read from the stream
        """
        if self.tell() >= self._get_length():
            return b''
        
        if size == -1:
            # Read all remaining bytes in the partial stream
            remaining = self._get_length() - self.tell()
            size = remaining
        else:
            # Limit read size to what's available in the partial stream
            remaining = self._get_length() - self.tell()
            size = min(size, remaining)
        
        if size <= 0:
            return b''
        
        return self._src.read(size)
    
    def readline(self, size: int = -1) -> bytes:
        """Read a line from the partial stream."""
        if self.tell() >= self._get_length():
            return b''
        
        remaining = self._get_length() - self.tell()
        if size == -1:
            size = remaining
        else:
            size = min(size, remaining)
        
        if size <= 0:
            return b''
        
        # Read up to size bytes or until newline
        line = b''
        for _ in range(size):
            char = self._src.read(1)
            if not char:
                break
            line += char
            if char == b'\n':
                break
        
        return line
    
    def write(self, data: bytes) -> int:
        """
        Write data to the underlying stream.
        
        Args:
            data: Bytes to write
        
        Returns:
            Number of bytes written
        """
        return self._src.write(data)
    
    def flush(self) -> None:
        """Flush the underlying stream."""
        self._src.flush()
    
    def close(self) -> None:
        """Close the partial stream (but not the underlying stream)."""
        super().close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def __repr__(self) -> str:
        return f"PartialStream(max_bytes={self._max_bytes}, current_pos={self.tell()})"