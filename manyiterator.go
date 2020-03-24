package roaring

type ShortManyIterable interface {
	NextMany(hs uint32, buf []uint32) int
	NextMany16(buf []uint16) int
}

func (si *shortIterator) NextMany(hs uint32, buf []uint32) int {
	n := 0
	l := si.loc
	s := si.slice
	for n < len(buf) && l < len(s) {
		buf[n] = uint32(s[l]) | hs
		l++
		n++
	}
	si.loc = l
	return n
}

func (si *shortIterator) NextMany16(buf []uint16) int {
	n := copy(buf, si.slice[si.loc:])
	si.loc += n
	return n
}
