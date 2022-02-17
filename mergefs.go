// Copyright (c) 2022 Meng Huang (mhboy@outlook.com)
// This package is licensed under a MIT license that can be found in the LICENSE file.

// Package mergefs merges the file systems into one.
package mergefs

import (
	"encoding/binary"
	"io"
	"os"
)

type seg struct {
	off  uint64
	size uint64
	pos  uint64
}

// SegFile is a segmented file.
type SegFile struct {
	file *os.File
	off  uint64
	segs []seg
}

// OpenSegFile opens the named file for reading. If successful, methods on
// the returned file can be used for reading; the associated file
// descriptor has mode O_RDONLY.
// If there is an error, it will be of type *PathError.
func OpenSegFile(name string) (*SegFile, error) {
	file, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, err
	}
	f := &SegFile{file: file}
	var a [16]byte
	var off int64
	for {
		buf := a[:]
		n, err := f.file.ReadAt(buf, off)
		if err != nil {
			if err != io.EOF {
				return nil, err
			}
			break
		} else if n != 16 {
			break
		}
		var s seg
		s.off = binary.LittleEndian.Uint64(buf)
		s.size = binary.LittleEndian.Uint64(buf[8:])
		s.pos = uint64(off) + 16
		f.segs = append(f.segs, s)
		f.off += 16 + s.size
		off = int64(f.off)
	}
	return f, nil
}

// WriteAt writes len(b) bytes to the File starting at byte offset off.
// It returns the number of bytes written and an error, if any.
// WriteAt returns a non-nil error when n != len(b).
//
// If file was opened with the O_APPEND flag, WriteAt returns an error.
func (f *SegFile) WriteAt(b []byte, off int64) (n int, err error) {
	s := seg{off: uint64(off), size: uint64(len(b)), pos: f.off + 16}
	var a [16]byte
	buf := a[:]
	binary.LittleEndian.PutUint64(buf, s.off)
	binary.LittleEndian.PutUint64(buf[8:], s.size)
	f.file.WriteAt(buf, int64(f.off))
	n, err = f.file.WriteAt(b, int64(f.off+16))
	f.segs = append(f.segs, s)
	f.off += 16 + s.size
	return
}

func (f *SegFile) size() int64 {
	if len(f.segs) == 0 {
		return 0
	}
	s := f.segs[len(f.segs)-1]
	return int64(s.off + s.size)
}

// ReadAt reads up to len(b) bytes from the File.
// It returns the number of bytes read and any error encountered.
// At end of file, Read returns 0, io.EOF.
func (f *SegFile) ReadAt(b []byte, off int64) (n int, err error) {
	if off > f.size() {
		return 0, io.EOF
	}
	var idx int
	for i := len(f.segs) - 1; i > 0; i-- {
		if f.segs[i].off <= uint64(off) {
			idx = i
			break
		}
	}
	var num int
	length := len(b)
	for n < length && idx < len(f.segs) {
		seg := f.segs[idx]
		segOff := off - int64(seg.off)
		segRemain := int(seg.size - uint64(segOff))
		readSize := segRemain
		if segRemain > length-n {
			readSize = length - n
		}
		num, err = f.file.ReadAt(b[n:n+readSize], int64(seg.pos)+segOff)
		off += int64(num)
		n += num
		idx++
	}
	return n, err
}

// Close closes the File, rendering it unusable for I/O.
// On files that support SetDeadline, any pending I/O operations will
// be canceled and return immediately with an error.
// Close will return an error if it has already been called.
func (f *SegFile) Close() error {
	return f.file.Close()
}
