package roaring

import (
	"bytes"
	"fmt"
	"strconv"
)

// Bitmap16 represents a compressed bitmap where you can add integers.
type Bitmap16 struct {
	highlowcontainer container
}

// NewBitmap16 creates a new empty Bitmap16 (see also New)
func NewBitmap16() *Bitmap16 {
	return &Bitmap16{
		highlowcontainer: newArrayContainer(),
	}
}

// BitmapOf generates a new bitmap filled with the specified integers
func BitmapOf16(dat ...uint16) *Bitmap16 {
	ans := NewBitmap16()
	ans.AddMany(dat)
	return ans
}

// RunOptimize attempts to further compress the runs of consecutive values found in the bitmap
func (rb *Bitmap16) RunOptimize() {
	rb.highlowcontainer = rb.highlowcontainer.toEfficientContainer()
}

// HasRunCompression returns true if the bitmap benefits from run compression
func (rb *Bitmap16) HasRunCompression() bool {
	_, ok := rb.highlowcontainer.(*runContainer16)
	return ok
}

// Clear resets the Bitmap16 to be logically empty, but may retain
// some memory allocations that may speed up future operations
func (rb *Bitmap16) Clear() {
	rb.highlowcontainer = newArrayContainer()
}

// ToArray creates a new slice containing all of the integers stored in the Bitmap16 in sorted order
func (rb *Bitmap16) ToArray() []uint16 {
	arr := make([]uint16, rb.highlowcontainer.getCardinality())
	iter := rb.highlowcontainer.getManyIterator()
	iter.NextMany16(arr)
	return arr
}

// GetSizeInBytes estimates the memory usage of the Bitmap16. Note that this
// might differ slightly from the amount of bytes required for persistent storage
func (rb *Bitmap16) GetSizeInBytes() uint64 {
	return uint64(8 + rb.highlowcontainer.getSizeInBytes())
}

// String creates a string representation of the Bitmap16
func (rb *Bitmap16) String() string {
	// inspired by https://github.com/fzandona/goroar/
	var buffer bytes.Buffer
	start := []byte("{")
	buffer.Write(start)
	i := rb.Iterator()
	counter := 0
	if i.HasNext() {
		counter = counter + 1
		buffer.WriteString(strconv.FormatInt(int64(i.Next()), 10))
	}
	for i.HasNext() {
		buffer.WriteString(",")
		counter = counter + 1
		// to avoid exhausting the memory
		if counter > 0x40000 {
			buffer.WriteString("...")
			break
		}
		buffer.WriteString(strconv.FormatInt(int64(i.Next()), 10))
	}
	buffer.WriteString("}")
	return buffer.String()
}

// Iterator creates a new IntPeekable to iterate over the integers contained in the bitmap, in sorted order;
// the iterator becomes invalid if the bitmap is modified (e.g., with Add or Remove).
func (rb *Bitmap16) Iterator() ShortPeekable {
	return rb.highlowcontainer.getShortIterator()
}

// ReverseIterator creates a new IntIterable to iterate over the integers contained in the bitmap, in sorted order;
// the iterator becomes invalid if the bitmap is modified (e.g., with Add or Remove).
func (rb *Bitmap16) ReverseIterator() ShortIterable {
	return rb.highlowcontainer.getReverseIterator()
}

// ManyIterator creates a new ManyIntIterable to iterate over the integers contained in the bitmap, in sorted order;
// the iterator becomes invalid if the bitmap is modified (e.g., with Add or Remove).
func (rb *Bitmap16) ManyIterator() ShortManyIterable {
	return rb.highlowcontainer.getManyIterator()
}

// Clone creates a copy of the Bitmap16
func (rb *Bitmap16) Clone() *Bitmap16 {
	return &Bitmap16{
		highlowcontainer: rb.highlowcontainer.clone(),
	}
}

// Minimum get the smallest value stored in this roaring bitmap, assumes that it is not empty
func (rb *Bitmap16) Minimum() uint16 {
	return rb.highlowcontainer.minimum()
}

// Maximum get the largest value stored in this roaring bitmap, assumes that it is not empty
func (rb *Bitmap16) Maximum() uint16 {
	return rb.highlowcontainer.maximum()
}

// Contains returns true if the integer is contained in the bitmap
func (rb *Bitmap16) Contains(x uint16) bool {
	return rb.highlowcontainer.contains(x)
}

// ContainsInt returns true if the integer is contained in the bitmap (this is a convenience method, the parameter is casted to uint32 and Contains is called)
func (rb *Bitmap16) ContainsInt(x int) bool {
	return rb.Contains(uint16(x))
}

// Equals returns true if the two bitmaps contain the same integers
func (rb *Bitmap16) Equals(o interface{}) bool {
	srb, ok := o.(*Bitmap16)
	if ok {
		return srb.highlowcontainer.equals(rb.highlowcontainer)
	}
	return false
}

// AddOffset adds the value 'offset' to each and every value in a bitmap, generating a new bitmap in the process
func AddOffset16(x *Bitmap16, offset uint16) *Bitmap16 {
	offsetted := x.highlowcontainer.addOffset(offset)
	return &Bitmap16{
		highlowcontainer: offsetted[0],
	}
}

// Add the integer x to the bitmap
func (rb *Bitmap16) Add(x uint16) {
	rb.highlowcontainer = rb.highlowcontainer.iaddReturnMinimized(x)
}

// CheckedAdd adds the integer x to the bitmap and return true  if it was added (false if the integer was already present)
func (rb *Bitmap16) CheckedAdd(x uint16) bool {
	oldCardinality := rb.highlowcontainer.getCardinality()
	rb.highlowcontainer = rb.highlowcontainer.iaddReturnMinimized(x)
	return rb.highlowcontainer.getCardinality() > oldCardinality
}

// AddInt adds the integer x to the bitmap (convenience method: the parameter is casted to uint32 and we call Add)
func (rb *Bitmap16) AddInt(x int) {
	rb.Add(uint16(x))
}

// Remove the integer x from the bitmap
func (rb *Bitmap16) Remove(x uint16) {
	rb.highlowcontainer.iremove(x)
}

// CheckedRemove removes the integer x from the bitmap and return true if the integer was effectively remove (and false if the integer was not present)
func (rb *Bitmap16) CheckedRemove(x uint16) bool {
	return rb.highlowcontainer.iremove(x)
}

// IsEmpty returns true if the Bitmap16 is empty (it is faster than doing (GetCardinality() == 0))
func (rb *Bitmap16) IsEmpty() bool {
	return rb.highlowcontainer.getCardinality() == 0
}

// GetCardinality returns the number of integers contained in the bitmap
func (rb *Bitmap16) GetCardinality() uint16 {
	return uint16(rb.highlowcontainer.getCardinality())
}

// Rank returns the number of integers that are smaller or equal to x (Rank(infinity) would be GetCardinality())
func (rb *Bitmap16) Rank(x uint16) uint16 {
	return uint16(rb.highlowcontainer.rank(x))
}

// Select returns the xth integer in the bitmap
func (rb *Bitmap16) Select(x uint16) (uint16, error) {
	cardinality := uint16(rb.highlowcontainer.getCardinality())
	if cardinality <= x {
		return 0, fmt.Errorf("can't find %dth integer in a bitmap with only %d items", x, cardinality)
	}
	return uint16(rb.highlowcontainer.selectInt(x)), nil
}

// And computes the intersection between two bitmaps and stores the result in the current bitmap
func (rb *Bitmap16) And(x2 *Bitmap16) {
	rb.highlowcontainer = rb.highlowcontainer.iand(x2.highlowcontainer)
}

// OrCardinality returns the cardinality of the union between two bitmaps, bitmaps are not modified
func (rb *Bitmap16) OrCardinality(x2 *Bitmap16) uint16 {
	return uint16(rb.highlowcontainer.orCardinality(x2.highlowcontainer))
}

// AndCardinality returns the cardinality of the intersection between two bitmaps, bitmaps are not modified
func (rb *Bitmap16) AndCardinality(x2 *Bitmap16) uint16 {
	return uint16(rb.highlowcontainer.andCardinality(x2.highlowcontainer))
}

// Intersects checks whether two bitmap intersects, bitmaps are not modified
func (rb *Bitmap16) Intersects(x2 *Bitmap16) bool {
	return rb.highlowcontainer.intersects(x2.highlowcontainer)
}

// Xor computes the symmetric difference between two bitmaps and stores the result in the current bitmap
func (rb *Bitmap16) Xor(x2 *Bitmap16) {
	rb.highlowcontainer = rb.highlowcontainer.xor(x2.highlowcontainer)
}

// Or computes the union between two bitmaps and stores the result in the current bitmap
func (rb *Bitmap16) Or(x2 *Bitmap16) {
	rb.highlowcontainer = rb.highlowcontainer.ior(x2.highlowcontainer)
}

// AndNot computes the difference between two bitmaps and stores the result in the current bitmap
func (rb *Bitmap16) AndNot(x2 *Bitmap16) {
	rb.highlowcontainer = rb.highlowcontainer.iandNot(x2.highlowcontainer)
}

// Or computes the union between two bitmaps and returns the result
func Or16(x1, x2 *Bitmap16) *Bitmap16 {
	return &Bitmap16{
		highlowcontainer: x1.highlowcontainer.or(x2.highlowcontainer),
	}
}

// And computes the intersection between two bitmaps and returns the result
func And16(x1, x2 *Bitmap16) *Bitmap16 {
	return &Bitmap16{
		highlowcontainer: x1.highlowcontainer.or(x2.highlowcontainer),
	}
}

// Xor computes the symmetric difference between two bitmaps and returns the result
func Xor16(x1, x2 *Bitmap16) *Bitmap16 {
	return &Bitmap16{
		highlowcontainer: x1.highlowcontainer.xor(x2.highlowcontainer),
	}
}

// AndNot computes the difference between two bitmaps and returns the result
func AndNot16(x1, x2 *Bitmap16) *Bitmap16 {
	return &Bitmap16{
		highlowcontainer: x1.highlowcontainer.andNot(x2.highlowcontainer),
	}
}

// AddMany add all of the values in dat
func (rb *Bitmap16) AddMany(dat []uint16) {
	for _, val := range dat {
		rb.highlowcontainer = rb.highlowcontainer.iaddReturnMinimized(val)
	}
}

// Flip negates the bits in the given range (i.e., [rangeStart,rangeEnd)), any integer present in this range and in the bitmap is removed,
// and any integer present in the range and not in the bitmap is added.
// The function uses 32-bit parameters even though a Bitmap16 stores 16-bit values because it is allowed and meaningful to use [0,uint32(0x10000)) as a range
// while uint32(0x10000) cannot be represented as a 16-bit value.
func (rb *Bitmap16) Flip(rangeStart, rangeEnd uint32) {

	if rangeEnd > MaxUint16+1 {
		panic("rangeEnd > MaxUint16+1")
	}
	if rangeStart > MaxUint16+1 {
		panic("rangeStart > MaxUint16+1")
	}

	if rangeStart >= rangeEnd {
		return
	}

	rb.highlowcontainer = rb.highlowcontainer.inot(int(rangeStart), int(rangeEnd))
}

// FlipInt calls Flip after casting the parameters  (convenience method)
func (rb *Bitmap16) FlipInt(rangeStart, rangeEnd int) {
	rb.Flip(uint32(rangeStart), uint32(rangeEnd))
}

// AddRange adds the integers in [rangeStart, rangeEnd) to the bitmap.
// The function uses 32-bit parameters even though a Bitmap16 stores 16-bit values because it is allowed and meaningful to use [0,uint32(0x10000)) as a range
// while uint32(0x10000) cannot be represented as a 16-bit value.
func (rb *Bitmap16) AddRange(rangeStart, rangeEnd uint32) {
	if rangeStart >= rangeEnd {
		return
	}
	if rangeEnd-1 > MaxUint16 {
		panic("rangeEnd-1 > MaxUint16")
	}

	rb.highlowcontainer = rb.highlowcontainer.iaddRange(int(rangeStart), int(rangeEnd))
}

// RemoveRange removes the integers in [rangeStart, rangeEnd) from the bitmap.
// The function uses 32-bit parameters even though a Bitmap16 stores 16-bit values because it is allowed and meaningful to use [0,uint32(0x10000)) as a range
// while uint32(0x10000) cannot be represented as a 16-bit value.
func (rb *Bitmap16) RemoveRange(rangeStart, rangeEnd uint32) {
	if rangeStart >= rangeEnd {
		return
	}
	if rangeEnd-1 > MaxUint16 {
		// logically, we should assume that the user wants to
		// remove all values from rangeStart to infinity
		// see https://github.com/RoaringBitmap/roaring/issues/141
		rangeEnd = uint32(0x10000)
	}

	rb.highlowcontainer = rb.highlowcontainer.iremoveRange(int(rangeStart), int(rangeEnd))
}

// Flip negates the bits in the given range  (i.e., [rangeStart,rangeEnd)), any integer present in this range and in the bitmap is removed,
// and any integer present in the range and not in the bitmap is added, a new bitmap is returned leaving
// the current bitmap unchanged.
// The function uses 32-bit parameters even though a Bitmap16 stores 16-bit values because it is allowed and meaningful to use [0,uint32(0x10000)) as a range
// while uint32(0x10000) cannot be represented as a 16-bit value.
func Flip16(rb *Bitmap16, rangeStart, rangeEnd uint32) *Bitmap16 {
	if rangeStart >= rangeEnd {
		return rb.Clone()
	}

	if rangeStart > MaxUint16 {
		panic("rangeStart > MaxUint16")
	}
	if rangeEnd-1 > MaxUint16 {
		panic("rangeEnd-1 > MaxUint16")
	}

	return &Bitmap16{
		highlowcontainer: rb.highlowcontainer.not(int(rangeStart), int(rangeEnd)),
	}
}

// FlipInt calls Flip after casting the parameters (convenience method)
func FlipInt16(rb *Bitmap16, rangeStart, rangeEnd int) *Bitmap16 {
	return Flip16(rb, uint32(rangeStart), uint32(rangeEnd))
}

// Stats returns details on container type usage in a Statistics struct.
func (rb *Bitmap16) Stats() Statistics {
	stats := Statistics{}
	stats.Containers = 1
	stats.Cardinality = uint64(rb.highlowcontainer.getCardinality())

	switch rb.highlowcontainer.(type) {
	case *arrayContainer:
		stats.ArrayContainers = 1
		stats.ArrayContainerBytes = uint64(rb.highlowcontainer.getSizeInBytes())
		stats.ArrayContainerValues = uint64(rb.highlowcontainer.getCardinality())
	case *bitmapContainer:
		stats.BitmapContainers = 1
		stats.BitmapContainerBytes = uint64(rb.highlowcontainer.getSizeInBytes())
		stats.BitmapContainerValues = uint64(rb.highlowcontainer.getCardinality())
	case *runContainer16:
		stats.RunContainers++
		stats.RunContainerBytes += uint64(rb.highlowcontainer.getSizeInBytes())
		stats.RunContainerValues += uint64(rb.highlowcontainer.getCardinality())
	}

	return stats
}
