package token

import "sort"

type Provider struct {
	loader  *BlockLoader
	entries []*TableEntry // continuous monotonic sequence of token table entries

	curBlockIndex  int
	curTokensBlock *Block
}

func NewProvider(loader *BlockLoader, entries []*TableEntry) *Provider {
	return &Provider{
		loader:        loader,
		entries:       entries,
		curBlockIndex: -1,
	}
}

func (tp *Provider) FirstTID() uint32 {
	return tp.entries[0].StartTID
}

func (tp *Provider) LastTID() uint32 {
	lastIndex := len(tp.entries) - 1
	return tp.entries[lastIndex].StartTID + tp.entries[lastIndex].ValCount - 1
}

func (tp *Provider) Ordered() bool {
	return true
}

func (tp *Provider) findBlock(tid uint32) int {
	if tp.curBlockIndex >= 0 && tp.entries[tp.curBlockIndex].checkTIDInBlock(tid) { // fast path
		return tp.curBlockIndex
	}

	return sort.Search(len(tp.entries), func(blockIndex int) bool { return tid <= tp.entries[blockIndex].getLastTID() })
}

func (tp *Provider) GetToken(tid uint32) []byte {
	blockIndex := tp.findBlock(tid)
	if blockIndex != tp.curBlockIndex {
		tp.curBlockIndex = blockIndex
		tp.curTokensBlock = tp.loader.Load(tp.entries[blockIndex])
	}
	return tp.curTokensBlock.GetValByTID(tid)
}
