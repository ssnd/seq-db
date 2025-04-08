package token

import "sort"

type Provider struct {
	loader  *BlockLoader
	entires []*TableEntry // continuous monotonic sequence of token table entries

	curBlockIndex  int
	curTokensBlock *Block
}

func NewProvider(loader *BlockLoader, entries []*TableEntry) *Provider {
	return &Provider{
		loader:        loader,
		entires:       entries,
		curBlockIndex: -1,
	}
}

func (tp *Provider) FirstTID() uint32 {
	return tp.entires[0].StartTID
}

func (tp *Provider) LastTID() uint32 {
	lastIndex := len(tp.entires) - 1
	return tp.entires[lastIndex].StartTID + tp.entires[lastIndex].ValCount - 1
}

func (tp *Provider) Ordered() bool {
	return true
}

func (tp *Provider) findBlock(tid uint32) int {
	if tp.curBlockIndex >= 0 && tp.entires[tp.curBlockIndex].checkTIDInBlock(tid) { // fast path
		return tp.curBlockIndex
	}

	return sort.Search(len(tp.entires), func(blockIndex int) bool { return tid <= tp.entires[blockIndex].getLastTID() })
}

func (tp *Provider) GetToken(tid uint32) []byte {
	blockIndex := tp.findBlock(tid)
	if blockIndex != tp.curBlockIndex {
		tp.curBlockIndex = blockIndex
		tp.curTokensBlock = tp.loader.Load(tp.entires[blockIndex])
	}
	return tp.curTokensBlock.GetValByTID(tid)
}
