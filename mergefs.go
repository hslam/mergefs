// Copyright (c) 2022 Meng Huang (mhboy@outlook.com)
// This package is licensed under a MIT license that can be found in the LICENSE file.

// Package mergefs merges the file systems into one.
package mergefs

import (
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"os"
)

const (
	numFiles = 2
)

type seg struct {
	off  uint64
	size uint64
	pos  uint64
	data []byte
}

func mergeSegs(a []seg, b []seg) []seg {
	if len(a) == 0 {
		return b
	} else if len(b) == 0 {
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

// File is a merge file.
type File struct {
	files []*SegFile
	size  int64
}

// OpenFile opens the named file for reading. If successful, methods on
// the returned file can be used for reading; the associated file
// descriptor has mode O_RDONLY.
// If there is an error, it will be of type *PathError.
func OpenFile(name string) (*File, error) {
	var size int64
	var files = make([]*SegFile, numFiles)
	for i := 0; i < len(files); i++ {
		file, err := OpenSegFile(fmt.Sprintf("%s-%d", name, i))
		if err != nil {
			return nil, err
		}
		files[i] = file
		if int64(file.size) > size {
			size = int64(file.size)
		}
	}
	return &File{files: files, size: size}, nil
}

func (f *File) file() *SegFile {
	return f.files[rand.Intn(len(f.files))]
}

// WriteAt writes len(b) bytes to the File starting at byte offset off.
// It returns the number of bytes written and an error, if any.
// WriteAt returns a non-nil error when n != len(b).
//
// If file was opened with the O_APPEND flag, WriteAt returns an error.
func (f *File) WriteAt(b []byte, off int64) (n int, err error) {
	n, err = f.file().WriteAt(b, off)
	if err != nil {
		return
	}
	f.size += int64(len(b))
	return
}

// ReadAt reads up to len(b) bytes from the File.
// It returns the number of bytes read and any error encountered.
// At end of file, Read returns 0, io.EOF.
func (f *File) ReadAt(b []byte, off int64) (n int, err error) {
	if off > f.size {
		return 0, io.EOF
	}
	var segs []seg
	for i := 0; i < len(f.files); i++ {
		ss, err := f.files[i].ReadAt(off, int64(len(b)))
		if err != nil && err != io.EOF {
			return 0, err
		}
		segs = mergeSegs(segs, ss)
	}
	length := len(b)
	for i := 0; i < len(segs) && n < length; i++ {
		s := segs[i]
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

// SegFile is a segmented file.
type SegFile struct {
	name string
	file *os.File
	size int64
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
	f := &SegFile{name: name, file: file}
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
		f.size = int64(s.off + s.size)
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
	f.size += int64(s.size)
	return
}

// ReadAt reads up to len(b) bytes from the File.
// It returns the number of bytes read and any error encountered.
// At end of file, Read returns 0, io.EOF.
func (f *SegFile) ReadAt(off, size int64) (segs []seg, err error) {
	for i := 0; i < len(f.segs); i++ {
		s := f.segs[i]
		if int64(s.off+s.size) <= off {
			continue
		} else if int64(s.off) >= off+size {
			break
		}
		b := make([]byte, s.size)
		_, err = f.file.ReadAt(b, int64(s.pos))
		var cp = s
		cp.data = b
		segs = append(segs, cp)
	}
	return segs, err
}

// Close closes the File, rendering it unusable for I/O.
// On files that support SetDeadline, any pending I/O operations will
// be canceled and return immediately with an error.
// Close will return an error if it has already been called.
func (f *SegFile) Close() error {
	return f.file.Close()
}
