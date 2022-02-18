// Copyright (c) 2022 Meng Huang (mhboy@outlook.com)
// This package is licensed under a MIT license that can be found in the LICENSE file.

// Package mergefs merges the file systems into one.
package mergefs

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"sort"
	"unsafe"
)

const (
	numFiles = 2
)

const frameHeaderSize = int(unsafe.Sizeof(frameHeader{}))

type frameHeader struct {
	off  uint64
	size uint64
}

func (h *frameHeader) marshal() []byte {
	var b []byte
	hdr := (*struct {
		data uintptr
		len  int
		cap  int
	})(unsafe.Pointer(&b))
	hdr.data = uintptr(unsafe.Pointer(h))
	hdr.len = frameHeaderSize
	hdr.cap = frameHeaderSize
	return b
}

func (h *frameHeader) unmarshal(b []byte) {
	*h = *(*frameHeader)(unsafe.Pointer(&b[0]))
}

type frame struct {
	frameHeader
	pos uint64
}

// Frame represents a frame data.
type Frame struct {
	frameHeader
	data []byte
}

func mergeFrames(a []Frame, b []Frame) []Frame {
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}
	m, n := a, b
	if len(a) < len(b) {
		m, n = b, a
	}
	i, j, k := len(m)-1, len(n)-1, len(m)+len(n)-1
	m = append(m, n...)
	for i >= 0 && j >= 0 {
		if m[i].off > n[j].off || m[i].off == n[j].off && (m[i].size == n[j].size || m[i].size > n[j].size) {
			m[k] = m[i]
			i--
		} else {
			m[k] = n[j]
			j--
		}
		k--
	}
	for j >= 0 {
		m[k] = n[j]
		j--
		k--
	}
	return m
}

// Remove removes the named file or (empty) directory.
// If there is an error, it will be of type *PathError.
func Remove(name string) error {
	for i := 0; i < numFiles; i++ {
		err := os.Remove(fmt.Sprintf("%s-%d", name, i))
		if err != nil {
			return err
		}
	}
	return nil
}

// File represents a merged file.
type File struct {
	files []SegFile
}

// OpenFile opens the named file for reading. If successful, methods on
// the returned file can be used for reading; the associated file
// descriptor has mode O_RDONLY.
// If there is an error, it will be of type *PathError.
func OpenFile(name string) (*File, error) {
	var files = make([]SegFile, numFiles)
	for i := 0; i < len(files); i++ {
		file, err := OpenSegFile(fmt.Sprintf("%s-%d", name, i))
		if err != nil {
			return nil, err
		}
		files[i] = file
	}
	return Open(files...)
}

// Open opens the merged file for reading. If successful, methods on
// the returned file can be used for reading; the associated file
// descriptor has mode O_RDONLY.
// If there is an error, it will be of type *PathError.
func Open(files ...SegFile) (*File, error) {
	return &File{files: files}, nil
}

func (f *File) file() SegFile {
	return f.files[rand.Intn(len(f.files))]
}

// WriteAt writes len(p) bytes from p to the underlying data stream
// at offset off. It returns the number of bytes written from p (0 <= n <= len(p))
// and any error encountered that caused the write to stop early.
// WriteAt must return a non-nil error if it returns n < len(p).
func (f *File) WriteAt(b []byte, off int64) (n int, err error) {
	n, err = f.file().WriteAt(b, off)
	if err != nil {
		return
	}
	return
}

// ReadAt reads len(p) bytes into p starting at offset off in the
// underlying input source. It returns the number of bytes
// read (0 <= n <= len(p)) and any error encountered.
func (f *File) ReadAt(b []byte, off int64) (n int, err error) {
	var frames []Frame
	for i := 0; i < len(f.files); i++ {
		ss, err := f.files[i].ReadAt(off, int64(len(b)))
		if err != nil && err != io.EOF {
			return 0, err
		}
		frames = mergeFrames(frames, ss)
	}
	length := len(b)
	for i := 0; i < len(frames) && n < length; i++ {
		s := frames[i]
		segOff := off - int64(s.off)
		segRemain := int(s.size - uint64(segOff))
		readSize := segRemain
		if segRemain > length-n {
			readSize = length - n
		}
		num := copy(b[n:n+readSize], s.data[segOff:])
		off += int64(num)
		n += num
	}
	return n, err
}

// Close closes the File, rendering it unusable for I/O.
// On files that support SetDeadline, any pending I/O operations will
// be canceled and return immediately with an error.
// Close will return an error if it has already been called.
func (f *File) Close() error {
	for i := 0; i < len(f.files); i++ {
		err := f.files[i].Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// SegFile represents a segmented file.
type SegFile interface {
	// WriteAt writes len(p) bytes from p to the underlying data stream
	// at offset off. It returns the number of bytes written from p (0 <= n <= len(p))
	// and any error encountered that caused the write to stop early.
	// WriteAt must return a non-nil error if it returns n < len(p).
	WriteAt(b []byte, off int64) (n int, err error)
	// ReadAt reads frames starting at offset off in the
	// underlying input source. It returns the frames and any error encountered.
	ReadAt(off, size int64) (frames []Frame, err error)
	// Close closes the File, rendering it unusable for I/O.
	// On files that support SetDeadline, any pending I/O operations will
	// be canceled and return immediately with an error.
	// Close will return an error if it has already been called.
	Close() error
}

type segFile struct {
	file   *os.File
	off    int64
	frames []frame
}

// OpenSegFile opens the segmented file for reading. If successful, methods on
// the returned file can be used for reading; the associated file
// descriptor has mode O_RDONLY.
// If there is an error, it will be of type *PathError.
func OpenSegFile(name string) (SegFile, error) {
	file, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, err
	}
	f := &segFile{file: file}
	var a [frameHeaderSize]byte
	var off int64
	for {
		buf := a[:]
		n, err := f.file.ReadAt(buf, off)
		if err != nil {
			if err != io.EOF {
				return nil, err
			}
			break
		}
		if n != frameHeaderSize {
			break
		}
		var s frame
		h := &frameHeader{}
		h.unmarshal(buf)
		s.frameHeader = *h
		s.pos = uint64(off) + uint64(frameHeaderSize)
		f.frames = append(f.frames, s)
		f.off += int64(frameHeaderSize) + int64(s.size)
		off = int64(f.off)
	}
	return f, nil
}

// WriteAt writes len(p) bytes from p to the underlying data stream
// at offset off. It returns the number of bytes written from p (0 <= n <= len(p))
// and any error encountered that caused the write to stop early.
// WriteAt must return a non-nil error if it returns n < len(p).
func (f *segFile) WriteAt(b []byte, off int64) (n int, err error) {
	s := frame{frameHeader: frameHeader{off: uint64(off), size: uint64(len(b))}, pos: uint64(f.off + int64(frameHeaderSize))}
	f.file.WriteAt(s.frameHeader.marshal(), int64(f.off))
	n, err = f.file.WriteAt(b, int64(s.pos))
	f.frames = append(f.frames, s)
	f.off += int64(frameHeaderSize) + int64(s.size)
	return
}

// ReadAt reads frames starting at offset off in the
// underlying input source. It returns the frames and any error encountered.
func (f *segFile) ReadAt(off, size int64) (frames []Frame, err error) {
	idx := sort.Search(len(f.frames), func(i int) bool {
		return f.frames[i].off > uint64(off)
	})
	start := idx - 1
	if start < 0 {
		start = 0
	}
	for i := start; i < len(f.frames); i++ {
		s := f.frames[i]
		if int64(s.off+s.size) <= off {
			continue
		}
		if int64(s.off) >= off+size {
			break
		}
		b := make([]byte, s.size)
		_, err = f.file.ReadAt(b, int64(s.pos))
		var r = Frame{frameHeader: s.frameHeader}
		r.data = b
		frames = append(frames, r)
	}
	return frames, err
}

// Close closes the File, rendering it unusable for I/O.
// On files that support SetDeadline, any pending I/O operations will
// be canceled and return immediately with an error.
// Close will return an error if it has already been called.
func (f *segFile) Close() error {
	return f.file.Close()
}
