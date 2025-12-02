package ethdataset

import (
	"fmt"

	"github.com/ethereum/go-ethereum/rlp"
)

type LeafNode struct {
	Key   []byte
	Value []byte
}

type ExtensionNode struct {
	Key      []byte
	NodeHash []byte
	Embedded *MPTNode
}

func (e *ExtensionNode) IsHash() bool {
	return e.NodeHash != nil
}

func (e *ExtensionNode) IsEmbedded() bool {
	return e.Embedded != nil
}

type BranchChild struct {
	Hash     []byte
	Embedded *MPTNode
}

func (b *BranchChild) IsHash() bool {
	return b.Hash != nil
}

func (b *BranchChild) IsEmbedded() bool {
	return b.Embedded != nil
}

type BranchNode struct {
	Children [16]*BranchChild
	Value    []byte
}

type MPTNode struct {
	Leaf      *LeafNode
	Extension *ExtensionNode
	Branch    *BranchNode
}

func (n *MPTNode) IsLeafNode() bool {
	return n.Leaf != nil
}

func (n *MPTNode) IsExtensionNode() bool {
	return n.Extension != nil
}

func (n *MPTNode) IsBranchNode() bool {
	return n.Branch != nil
}

func compactToHex(compact []byte) []byte {
	if len(compact) == 0 {
		return nil
	}
	// Expand to nibbles
	nibbles := make([]byte, len(compact)*2)
	for i, b := range compact {
		nibbles[2*i] = b >> 4
		nibbles[2*i+1] = b & 0x0f
	}
	flags := nibbles[0]
	odd := flags & 1
	leaf := (flags & 2) != 0

	var hex []byte
	if odd == 1 {
		hex = nibbles[1:]
	} else {
		hex = nibbles[2:]
	}
	if leaf {
		hex = append(hex, 16)
	}
	return hex
}

func hasTerminatorFlag(s []byte) bool {
	return len(s) > 0 && s[len(s)-1] == 16
}

func ParseBranchNode(elems []byte) (*BranchNode, error) {
	bn := &BranchNode{}

	work := elems
	for i := 0; i < 16; i++ {
		kind, val, rest, err := rlp.Split(work)
		if err != nil {
			return nil, err
		}
		var child *BranchChild
		switch {
		case kind == rlp.List:
			// embedded node
			embedded, err := ParseNode(work[:len(work)-len(rest)])
			if err != nil {
				return nil, err
			}
			child = &BranchChild{
				Embedded: embedded,
			}
		case kind == rlp.String && len(val) == 32:
			// hash
			child = &BranchChild{
				Hash: val,
			}
		case kind == rlp.String && len(val) == 0:
			// empty node
			child = nil
		default:
			return nil, fmt.Errorf("Invalid RLP String size got=%d want 0 or 32", len(val))
		}
		bn.Children[i] = child
		work = rest
	}

	val, _, err := rlp.SplitString(work)
	if err != nil {
		return nil, err
	}
	if len(val) > 0 {
		bn.Value = val
	}
	return bn, nil
}

func ParseNode(b []byte) (*MPTNode, error) {
	elems, _, err := rlp.SplitList(b)
	if err != nil {
		return nil, err
	}
	switch c, _ := rlp.CountValues(elems); c {
	case 2:
		keyBuf, restAfterKey, err := rlp.SplitString(elems)
		if err != nil {
			return nil, err
		}
		key := compactToHex(keyBuf)

		kind, val, restAfterVal, err := rlp.Split(restAfterKey)
		if err != nil {
			return nil, err
		}

		if hasTerminatorFlag(key) {
			leafVal, _, err := rlp.SplitString(restAfterKey)
			if err != nil {
				return nil, err
			}
			leafKey := key[:len(key)-1]
			return &MPTNode{
				Leaf: &LeafNode{
					Key:   leafKey,
					Value: leafVal,
				},
			}, nil
		} else {
			switch {
			case kind == rlp.String && len(val) == 32:
				return &MPTNode{
					Extension: &ExtensionNode{
						Key:      key,
						NodeHash: val,
					},
				}, nil
			case kind == rlp.List:
				embedded, err := ParseNode(restAfterKey[:len(restAfterKey)-len(restAfterVal)])
				if err != nil {
					return nil, err
				}
				return &MPTNode{
					Extension: &ExtensionNode{
						Key:      key,
						Embedded: embedded,
					},
				}, nil
			default:
				return nil, fmt.Errorf("Invalid ExtensionNode")
			}
		}
	case 17:
		branchNode, err := ParseBranchNode(elems)
		if err != nil {
			return nil, err
		}
		return &MPTNode{Branch: branchNode}, nil
	default:
		return nil, fmt.Errorf("invalid number of MPT Node elements, got=%v, want 2 or 17.", c)
	}
}
