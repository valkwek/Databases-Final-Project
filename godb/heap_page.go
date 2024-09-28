package godb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"unsafe"
)

/* HeapPage implements the Page interface for pages of HeapFiles. We have
provided our interface to HeapPage below for you to fill in, but you are not
required to implement these methods except for the three methods that the Page
interface requires.  You will want to use an interface like what we provide to
implement the methods of [HeapFile] that insert, delete, and iterate through
tuples.

In GoDB all tuples are fixed length, which means that given a TupleDesc it is
possible to figure out how many tuple "slots" fit on a given page.

In addition, all pages are PageSize bytes.  They begin with a header with a 32
bit integer with the number of slots (tuples), and a second 32 bit integer with
the number of used slots.

Each tuple occupies the same number of bytes.  You can use the go function
unsafe.Sizeof() to determine the size in bytes of an object.  So, a GoDB integer
(represented as an int64) requires unsafe.Sizeof(int64(0)) bytes.  For strings,
we encode them as byte arrays of StringLength, so they are size
((int)(unsafe.Sizeof(byte('a')))) * StringLength bytes.  The size in bytes of a
tuple is just the sum of the size in bytes of its fields.

Once you have figured out how big a record is, you can determine the number of
slots on on the page as:

remPageSize = PageSize - 8 // bytes after header
numSlots = remPageSize / bytesPerTuple //integer division will round down

To serialize a page to a buffer, you can then:

write the number of slots as an int32
write the number of used slots as an int32
write the tuples themselves to the buffer

You will follow the inverse process to read pages from a buffer.

Note that to process deletions you will likely delete tuples at a specific
position (slot) in the heap page.  This means that after a page is read from
disk, tuples should retain the same slot number. Because GoDB will never evict a
dirty page, it's OK if tuples are renumbered when they are written back to disk.

*/

type heapPage struct {
	// TODO: some code goes here
	Desc       TupleDesc
	PageNo     int
	HeapF      *HeapFile
	tuples     []*Tuple
	IsDirty    bool
	numUsed    int
	numSlots   int
	emptySlots []int
}

// Construct a new heap page
func newHeapPage(desc *TupleDesc, pageNo int, f *HeapFile) (*heapPage, error) {
	// TODO: some code goes here
	if f == nil {
		return nil, fmt.Errorf("f is nil")
	}

	hp := heapPage{
		Desc:    *desc,
		PageNo:  pageNo,
		HeapF:   f,
		numUsed: 0,
		IsDirty: false,
	}
	hp.numSlots = hp.getNumSlots()
	emptySlots := make([]int, hp.numSlots)
	for i := 0; i < hp.numSlots; i++ {
		emptySlots[i] = i
	}
	hp.tuples = make([]*Tuple, hp.numSlots)
	hp.emptySlots = emptySlots
	return &hp, nil
}

func (h *heapPage) getNumSlots() int {
	// TODO: some code goes here
	bytesPerTuple := 0
	for _, field := range h.Desc.Fields {
		switch field.Ftype {
		case IntType:
			bytesPerTuple += int(unsafe.Sizeof(int64(0)))
		case StringType:
			bytesPerTuple += ((int)(unsafe.Sizeof(byte('a')))) * StringLength
		}
	}
	remPageSize := PageSize - 8
	numSlots := remPageSize / bytesPerTuple
	return numSlots
}

// Insert the tuple into a free slot on the page, or return an error if there are
// no free slots.  Set the tuples rid and return it.
func (h *heapPage) insertTuple(t *Tuple) (recordID, error) {
	// TODO: some code goes here

	if h.numUsed == h.numSlots {
		return nil, fmt.Errorf("no free slots")
	}

	tupleRid := HeapRecordID{
		PageNumber: h.PageNo,
		SlotNumber: h.emptySlots[len(h.emptySlots)-1],
	}
	t.Rid = tupleRid
	h.tuples[tupleRid.SlotNumber] = t
	h.numUsed += 1
	h.IsDirty = true
	h.emptySlots = h.emptySlots[:len(h.emptySlots)-1]
	return tupleRid, nil
}

// Delete the tuple at the specified record ID, or return an error if the ID is
// invalid.
func (h *heapPage) deleteTuple(rid recordID) error {
	// TODO: some code goes here
	if rid.GetSlotNumber() < 0 || rid.GetSlotNumber() >= h.numSlots || h.tuples[rid.GetSlotNumber()] == nil {
		return fmt.Errorf("id is invalid")
	}
	h.tuples[rid.GetSlotNumber()] = nil
	h.setDirty(0, true)
	h.numUsed -= 1
	h.emptySlots = append(h.emptySlots, rid.GetSlotNumber())
	return nil
}

// Page method - return whether or not the page is dirty
func (h *heapPage) isDirty() bool {
	// TODO: some code goes here
	return h.IsDirty
}

// Page method - mark the page as dirty
func (h *heapPage) setDirty(tid TransactionID, dirty bool) {
	// TODO: some code goes here
	h.IsDirty = dirty
}

// Page method - return the corresponding HeapFile
// for this page.
func (p *heapPage) getFile() DBFile {
	// TODO: some code goes here
	return p.HeapF
}

func (p *heapPage) getPageNo() int {
	return p.PageNo
}

// Allocate a new bytes.Buffer and write the heap page to it. Returns an error
// if the write to the the buffer fails. You will likely want to call this from
// your [HeapFile.flushPage] method.  You should write the page header, using
// the binary.Write method in LittleEndian order, followed by the tuples of the
// page, written using the Tuple.writeTo method.
func (h *heapPage) toBuffer() (*bytes.Buffer, error) {
	// TODO: some code goes here
	var buffer bytes.Buffer

	numSlots := h.numSlots
	usedSlots := h.numUsed

	err := binary.Write(&buffer, binary.LittleEndian, int32(numSlots))
	if err != nil {
		return nil, err
	}

	err = binary.Write(&buffer, binary.LittleEndian, int32(usedSlots))
	if err != nil {
		return nil, err
	}

	for i := 0; i < len(h.tuples); i++ {
		if h.tuples[i] != nil {
			err = h.tuples[i].writeTo(&buffer)
			if err != nil {
				return nil, err
			}
		}
	}

	if buffer.Len() < PageSize {
		padding := make([]byte, PageSize-buffer.Len())
		buffer.Write(padding)
	}
	return &buffer, nil
}

// Read the contents of the HeapPage from the supplied buffer.
func (h *heapPage) initFromBuffer(buf *bytes.Buffer) error {
	// TODO: some code goes here
	var numSlots, numUsed int32
	err := binary.Read(buf, binary.LittleEndian, &numSlots)
	if err != nil {
		return err
	}

	err = binary.Read(buf, binary.LittleEndian, &numUsed)
	if err != nil {
		return err
	}

	h.tuples = make([]*Tuple, numSlots)
	h.numSlots = int(numSlots)
	h.numUsed = int(numUsed)
	emptySlots := make([]int, h.numSlots)
	for i := 0; i < h.numSlots; i++ {
		emptySlots[i] = i
	}
	h.emptySlots = emptySlots

	for i := 0; i < h.numUsed; i++ {
		tuple, err := readTupleFrom(buf, &h.Desc)
		if err != nil {
			h.tuples[i] = nil
		}

		rid := HeapRecordID{
			PageNumber: h.PageNo,
			SlotNumber: int(i),
		}
		tuple.Rid = rid
		h.tuples[i] = tuple
	}
	return nil
}

// Return a function that iterates through the tuples of the heap page.  Be sure
// to set the rid of the tuple to the rid struct of your choosing beforing
// return it. Return nil, nil when the last tuple is reached.
func (p *heapPage) tupleIter() func() (*Tuple, error) {
	// TODO: some code goes here
	curIndex := 0
	numTuples := p.numSlots
	return func() (*Tuple, error) {
		for curIndex < numTuples {
			if p.tuples[curIndex] != nil {
				tuple := p.tuples[curIndex]
				tuple.Rid = HeapRecordID{
					PageNumber: p.PageNo,
					SlotNumber: curIndex,
				}
				curIndex += 1
				return tuple, nil
			} else {
				curIndex += 1
			}

		}
		return nil, nil
	}
}
