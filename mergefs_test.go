// Copyright (c) 2022 Meng Huang (mhboy@outlook.com)
// This package is licensed under a MIT license that can be found in the LICENSE file.

package mergefs

import (
	"io"
	"math/rand"
	"testing"
)

func TestFile(t *testing.T) {
	name := "mergefs"
	{
		Remove(name)
		defer Remove(name)
	}
	content := make([]byte, 1024*64)
	{
		n, err := rand.Read(content)
		if err != nil {
			t.Error(err)
		} else if n != len(content) {
			t.Errorf("expect %d, got %d", len(content), n)
		}
	}
	{
		f, err := OpenFile(name)
		if err != nil {
			t.Error(err)
		}
		{
			offset := int64(0)
			length := len(content) / 2
			remain := length
			for remain > 0 {
				off := int64(length - remain)
				size := rand.Int63n(128)
				if off+size > int64(length) {
					size = int64(length) - off
				}
				n, err := f.WriteAt(content[off:off+size], off+offset)
				if err != nil {
					t.Error(err)
				} else if n != int(size) {
					t.Errorf("expect %d, got %d", size, n)
				}
				remain -= n
			}
		}
		for off := int64(0); off < int64(len(content)/2); off++ {
			buf := make([]byte, rand.Intn(256))
			n, err := f.ReadAt(buf, off)
			if err != nil && err != io.EOF {
				t.Error(err)
			} else if string(buf[:n]) != string(content[off:off+int64(n)]) {
				t.Errorf("expect %v, got %v", content[off:off+int64(n)], buf[:n])
			}
		}
		f.Close()
	}

	{
		f, err := OpenFile(name)
		if err != nil {
			t.Error(err)
		}
		{
			offset := int64(0)
			length := len(content) / 2
			remain := length
			for remain > 0 {
				off := int64(length - remain)
				size := rand.Int63n(128)
				if off+size > int64(length) {
					size = int64(length) - off
				}
				off += int64(length)
				n, err := f.WriteAt(content[off:off+size], off+offset)
				if err != nil {
					t.Error(err)
				} else if n != int(size) {
					t.Errorf("expect %d, got %d", size, n)
				}
				remain -= n
			}
		}
		for off := int64(0); off < int64(len(content)); off++ {
			buf := make([]byte, rand.Intn(512))
			n, err := f.ReadAt(buf, off)
			if err != nil && err != io.EOF {
				t.Error(err)
			} else if string(buf[:n]) != string(content[off:off+int64(n)]) {
				t.Errorf("expect %v, got %v", content[off:off+int64(n)], buf[:n])
			}
		}
		f.Close()
	}
}
