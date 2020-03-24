package roaring

type ShortIterable interface {
	HasNext() bool
	Next() uint16
}

type ShortPeekable interface {
	ShortIterable
	PeekNext() uint16
	AdvanceIfNeeded(minval uint16)
}

type shortIterator struct {
	slice []uint16
	loc   int
}

func (si *shortIterator) HasNext() bool {
	return si.loc < len(si.slice)
}

func (si *shortIterator) Next() uint16 {
	a := si.slice[si.loc]
	si.loc++
	return a
}

func (si *shortIterator) PeekNext() uint16 {
	return si.slice[si.loc]
}

func (si *shortIterator) AdvanceIfNeeded(minval uint16) {
	if si.HasNext() && si.PeekNext() < minval {
		si.loc = advanceUntil(si.slice, si.loc, len(si.slice), minval)
	}
}

type reverseIterator struct {
	slice []uint16
	loc   int
}

func (si *reverseIterator) HasNext() bool {
	return si.loc >= 0
}

func (si *reverseIterator) Next() uint16 {
	a := si.slice[si.loc]
	si.loc--
	return a
}
