// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: pullsync.proto

package pb

import (
	fmt "fmt"
	proto "github.com/gogo/protobuf/proto"
	io "io"
	math "math"
	math_bits "math/bits"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.GoGoProtoPackageIsVersion3 // please upgrade the proto package

type Syn struct {
}

func (m *Syn) Reset()         { *m = Syn{} }
func (m *Syn) String() string { return proto.CompactTextString(m) }
func (*Syn) ProtoMessage()    {}
func (*Syn) Descriptor() ([]byte, []int) {
	return fileDescriptor_d1dee042cf9c065c, []int{0}
}
func (m *Syn) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *Syn) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_Syn.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *Syn) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Syn.Merge(m, src)
}
func (m *Syn) XXX_Size() int {
	return m.Size()
}
func (m *Syn) XXX_DiscardUnknown() {
	xxx_messageInfo_Syn.DiscardUnknown(m)
}

var xxx_messageInfo_Syn proto.InternalMessageInfo

type Ack struct {
	Cursors []uint64 `protobuf:"varint,1,rep,packed,name=Cursors,proto3" json:"Cursors,omitempty"`
}

func (m *Ack) Reset()         { *m = Ack{} }
func (m *Ack) String() string { return proto.CompactTextString(m) }
func (*Ack) ProtoMessage()    {}
func (*Ack) Descriptor() ([]byte, []int) {
	return fileDescriptor_d1dee042cf9c065c, []int{1}
}
func (m *Ack) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *Ack) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_Ack.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *Ack) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Ack.Merge(m, src)
}
func (m *Ack) XXX_Size() int {
	return m.Size()
}
func (m *Ack) XXX_DiscardUnknown() {
	xxx_messageInfo_Ack.DiscardUnknown(m)
}

var xxx_messageInfo_Ack proto.InternalMessageInfo

func (m *Ack) GetCursors() []uint64 {
	if m != nil {
		return m.Cursors
	}
	return nil
}

type Get struct {
	Bin   int32  `protobuf:"varint,1,opt,name=Bin,proto3" json:"Bin,omitempty"`
	Start uint64 `protobuf:"varint,2,opt,name=Start,proto3" json:"Start,omitempty"`
}

func (m *Get) Reset()         { *m = Get{} }
func (m *Get) String() string { return proto.CompactTextString(m) }
func (*Get) ProtoMessage()    {}
func (*Get) Descriptor() ([]byte, []int) {
	return fileDescriptor_d1dee042cf9c065c, []int{2}
}
func (m *Get) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *Get) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_Get.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *Get) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Get.Merge(m, src)
}
func (m *Get) XXX_Size() int {
	return m.Size()
}
func (m *Get) XXX_DiscardUnknown() {
	xxx_messageInfo_Get.DiscardUnknown(m)
}

var xxx_messageInfo_Get proto.InternalMessageInfo

func (m *Get) GetBin() int32 {
	if m != nil {
		return m.Bin
	}
	return 0
}

func (m *Get) GetStart() uint64 {
	if m != nil {
		return m.Start
	}
	return 0
}

type Chunk struct {
	Address []byte `protobuf:"bytes,1,opt,name=Address,proto3" json:"Address,omitempty"`
	BatchID []byte `protobuf:"bytes,2,opt,name=BatchID,proto3" json:"BatchID,omitempty"`
}

func (m *Chunk) Reset()         { *m = Chunk{} }
func (m *Chunk) String() string { return proto.CompactTextString(m) }
func (*Chunk) ProtoMessage()    {}
func (*Chunk) Descriptor() ([]byte, []int) {
	return fileDescriptor_d1dee042cf9c065c, []int{3}
}
func (m *Chunk) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *Chunk) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_Chunk.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *Chunk) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Chunk.Merge(m, src)
}
func (m *Chunk) XXX_Size() int {
	return m.Size()
}
func (m *Chunk) XXX_DiscardUnknown() {
	xxx_messageInfo_Chunk.DiscardUnknown(m)
}

var xxx_messageInfo_Chunk proto.InternalMessageInfo

func (m *Chunk) GetAddress() []byte {
	if m != nil {
		return m.Address
	}
	return nil
}

func (m *Chunk) GetBatchID() []byte {
	if m != nil {
		return m.BatchID
	}
	return nil
}

type Offer struct {
	Topmost uint64   `protobuf:"varint,1,opt,name=Topmost,proto3" json:"Topmost,omitempty"`
	Chunks  []*Chunk `protobuf:"bytes,2,rep,name=Chunks,proto3" json:"Chunks,omitempty"`
}

func (m *Offer) Reset()         { *m = Offer{} }
func (m *Offer) String() string { return proto.CompactTextString(m) }
func (*Offer) ProtoMessage()    {}
func (*Offer) Descriptor() ([]byte, []int) {
	return fileDescriptor_d1dee042cf9c065c, []int{4}
}
func (m *Offer) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *Offer) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_Offer.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *Offer) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Offer.Merge(m, src)
}
func (m *Offer) XXX_Size() int {
	return m.Size()
}
func (m *Offer) XXX_DiscardUnknown() {
	xxx_messageInfo_Offer.DiscardUnknown(m)
}

var xxx_messageInfo_Offer proto.InternalMessageInfo

func (m *Offer) GetTopmost() uint64 {
	if m != nil {
		return m.Topmost
	}
	return 0
}

func (m *Offer) GetChunks() []*Chunk {
	if m != nil {
		return m.Chunks
	}
	return nil
}

type Want struct {
	BitVector []byte `protobuf:"bytes,1,opt,name=BitVector,proto3" json:"BitVector,omitempty"`
}

func (m *Want) Reset()         { *m = Want{} }
func (m *Want) String() string { return proto.CompactTextString(m) }
func (*Want) ProtoMessage()    {}
func (*Want) Descriptor() ([]byte, []int) {
	return fileDescriptor_d1dee042cf9c065c, []int{5}
}
func (m *Want) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *Want) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_Want.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *Want) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Want.Merge(m, src)
}
func (m *Want) XXX_Size() int {
	return m.Size()
}
func (m *Want) XXX_DiscardUnknown() {
	xxx_messageInfo_Want.DiscardUnknown(m)
}

var xxx_messageInfo_Want proto.InternalMessageInfo

func (m *Want) GetBitVector() []byte {
	if m != nil {
		return m.BitVector
	}
	return nil
}

type Delivery struct {
	Address []byte `protobuf:"bytes,1,opt,name=Address,proto3" json:"Address,omitempty"`
	Data    []byte `protobuf:"bytes,2,opt,name=Data,proto3" json:"Data,omitempty"`
	Stamp   []byte `protobuf:"bytes,3,opt,name=Stamp,proto3" json:"Stamp,omitempty"`
}

func (m *Delivery) Reset()         { *m = Delivery{} }
func (m *Delivery) String() string { return proto.CompactTextString(m) }
func (*Delivery) ProtoMessage()    {}
func (*Delivery) Descriptor() ([]byte, []int) {
	return fileDescriptor_d1dee042cf9c065c, []int{6}
}
func (m *Delivery) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *Delivery) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_Delivery.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *Delivery) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Delivery.Merge(m, src)
}
func (m *Delivery) XXX_Size() int {
	return m.Size()
}
func (m *Delivery) XXX_DiscardUnknown() {
	xxx_messageInfo_Delivery.DiscardUnknown(m)
}

var xxx_messageInfo_Delivery proto.InternalMessageInfo

func (m *Delivery) GetAddress() []byte {
	if m != nil {
		return m.Address
	}
	return nil
}

func (m *Delivery) GetData() []byte {
	if m != nil {
		return m.Data
	}
	return nil
}

func (m *Delivery) GetStamp() []byte {
	if m != nil {
		return m.Stamp
	}
	return nil
}

func init() {
	proto.RegisterType((*Syn)(nil), "pullsync.Syn")
	proto.RegisterType((*Ack)(nil), "pullsync.Ack")
	proto.RegisterType((*Get)(nil), "pullsync.Get")
	proto.RegisterType((*Chunk)(nil), "pullsync.Chunk")
	proto.RegisterType((*Offer)(nil), "pullsync.Offer")
	proto.RegisterType((*Want)(nil), "pullsync.Want")
	proto.RegisterType((*Delivery)(nil), "pullsync.Delivery")
}

func init() { proto.RegisterFile("pullsync.proto", fileDescriptor_d1dee042cf9c065c) }

var fileDescriptor_d1dee042cf9c065c = []byte{
	// 298 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x7c, 0x91, 0xb1, 0x4e, 0xf3, 0x30,
	0x14, 0x85, 0xeb, 0x3a, 0xe9, 0xdf, 0xff, 0x52, 0x01, 0xb2, 0x18, 0x3c, 0x54, 0x26, 0xb2, 0x90,
	0xc8, 0x42, 0x07, 0x18, 0x99, 0x9a, 0x56, 0x42, 0x30, 0x80, 0xe4, 0x22, 0x90, 0xd8, 0xd2, 0xd4,
	0x55, 0x2b, 0xda, 0x38, 0xb2, 0x1d, 0xa4, 0xbc, 0x05, 0x8f, 0xc5, 0xd8, 0x91, 0x11, 0x25, 0x2f,
	0x82, 0xe2, 0x26, 0xb0, 0xb1, 0xdd, 0xef, 0x5c, 0x9f, 0xab, 0x73, 0x64, 0x38, 0xcc, 0xf2, 0xcd,
	0xc6, 0x14, 0x69, 0x32, 0xca, 0xb4, 0xb2, 0x8a, 0xf4, 0x5b, 0xe6, 0x3e, 0xe0, 0x59, 0x91, 0xf2,
	0x53, 0xc0, 0xe3, 0xe4, 0x95, 0x50, 0xf8, 0x37, 0xc9, 0xb5, 0x51, 0xda, 0x50, 0x14, 0xe0, 0xd0,
	0x13, 0x2d, 0xf2, 0x0b, 0xc0, 0x37, 0xd2, 0x92, 0x63, 0xc0, 0xd1, 0x3a, 0xa5, 0x28, 0x40, 0xa1,
	0x2f, 0xea, 0x91, 0x9c, 0x80, 0x3f, 0xb3, 0xb1, 0xb6, 0xb4, 0x1b, 0xa0, 0xd0, 0x13, 0x7b, 0xe0,
	0xd7, 0xe0, 0x4f, 0x56, 0x79, 0xea, 0x2e, 0x8e, 0x17, 0x0b, 0x2d, 0x8d, 0x71, 0xa6, 0x81, 0x68,
	0xb1, 0xde, 0x44, 0xb1, 0x4d, 0x56, 0xb7, 0x53, 0x67, 0x1d, 0x88, 0x16, 0xf9, 0x1d, 0xf8, 0x0f,
	0xcb, 0xa5, 0xd4, 0xf5, 0x93, 0x47, 0x95, 0x6d, 0x95, 0xb1, 0xce, 0xec, 0x89, 0x16, 0xc9, 0x39,
	0xf4, 0xdc, 0x7d, 0x43, 0xbb, 0x01, 0x0e, 0x0f, 0x2e, 0x8f, 0x46, 0x3f, 0x0d, 0x9d, 0x2e, 0x9a,
	0x35, 0x3f, 0x03, 0xef, 0x39, 0x4e, 0x2d, 0x19, 0xc2, 0xff, 0x68, 0x6d, 0x9f, 0x64, 0x62, 0x95,
	0x6e, 0x92, 0xfc, 0x0a, 0xfc, 0x1e, 0xfa, 0x53, 0xb9, 0x59, 0xbf, 0x49, 0x5d, 0xfc, 0x91, 0x98,
	0x80, 0x37, 0x8d, 0x6d, 0xdc, 0xc4, 0x75, 0x73, 0x53, 0x7f, 0x9b, 0x51, 0xec, 0xc4, 0x3d, 0x44,
	0xc3, 0x8f, 0x92, 0xa1, 0x5d, 0xc9, 0xd0, 0x57, 0xc9, 0xd0, 0x7b, 0xc5, 0x3a, 0xbb, 0x8a, 0x75,
	0x3e, 0x2b, 0xd6, 0x79, 0xe9, 0x66, 0xf3, 0x79, 0xcf, 0x7d, 0xc2, 0xd5, 0x77, 0x00, 0x00, 0x00,
	0xff, 0xff, 0xc7, 0xa0, 0xe3, 0x7d, 0x96, 0x01, 0x00, 0x00,
}

func (m *Syn) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Syn) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *Syn) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	return len(dAtA) - i, nil
}

func (m *Ack) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Ack) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *Ack) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Cursors) > 0 {
		dAtA2 := make([]byte, len(m.Cursors)*10)
		var j1 int
		for _, num := range m.Cursors {
			for num >= 1<<7 {
				dAtA2[j1] = uint8(uint64(num)&0x7f | 0x80)
				num >>= 7
				j1++
			}
			dAtA2[j1] = uint8(num)
			j1++
		}
		i -= j1
		copy(dAtA[i:], dAtA2[:j1])
		i = encodeVarintPullsync(dAtA, i, uint64(j1))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func (m *Get) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Get) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *Get) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.Start != 0 {
		i = encodeVarintPullsync(dAtA, i, uint64(m.Start))
		i--
		dAtA[i] = 0x10
	}
	if m.Bin != 0 {
		i = encodeVarintPullsync(dAtA, i, uint64(m.Bin))
		i--
		dAtA[i] = 0x8
	}
	return len(dAtA) - i, nil
}

func (m *Chunk) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Chunk) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *Chunk) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.BatchID) > 0 {
		i -= len(m.BatchID)
		copy(dAtA[i:], m.BatchID)
		i = encodeVarintPullsync(dAtA, i, uint64(len(m.BatchID)))
		i--
		dAtA[i] = 0x12
	}
	if len(m.Address) > 0 {
		i -= len(m.Address)
		copy(dAtA[i:], m.Address)
		i = encodeVarintPullsync(dAtA, i, uint64(len(m.Address)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func (m *Offer) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Offer) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *Offer) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Chunks) > 0 {
		for iNdEx := len(m.Chunks) - 1; iNdEx >= 0; iNdEx-- {
			{
				size, err := m.Chunks[iNdEx].MarshalToSizedBuffer(dAtA[:i])
				if err != nil {
					return 0, err
				}
				i -= size
				i = encodeVarintPullsync(dAtA, i, uint64(size))
			}
			i--
			dAtA[i] = 0x12
		}
	}
	if m.Topmost != 0 {
		i = encodeVarintPullsync(dAtA, i, uint64(m.Topmost))
		i--
		dAtA[i] = 0x8
	}
	return len(dAtA) - i, nil
}

func (m *Want) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Want) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *Want) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.BitVector) > 0 {
		i -= len(m.BitVector)
		copy(dAtA[i:], m.BitVector)
		i = encodeVarintPullsync(dAtA, i, uint64(len(m.BitVector)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func (m *Delivery) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Delivery) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *Delivery) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Stamp) > 0 {
		i -= len(m.Stamp)
		copy(dAtA[i:], m.Stamp)
		i = encodeVarintPullsync(dAtA, i, uint64(len(m.Stamp)))
		i--
		dAtA[i] = 0x1a
	}
	if len(m.Data) > 0 {
		i -= len(m.Data)
		copy(dAtA[i:], m.Data)
		i = encodeVarintPullsync(dAtA, i, uint64(len(m.Data)))
		i--
		dAtA[i] = 0x12
	}
	if len(m.Address) > 0 {
		i -= len(m.Address)
		copy(dAtA[i:], m.Address)
		i = encodeVarintPullsync(dAtA, i, uint64(len(m.Address)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func encodeVarintPullsync(dAtA []byte, offset int, v uint64) int {
	offset -= sovPullsync(v)
	base := offset
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return base
}
func (m *Syn) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	return n
}

func (m *Ack) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if len(m.Cursors) > 0 {
		l = 0
		for _, e := range m.Cursors {
			l += sovPullsync(uint64(e))
		}
		n += 1 + sovPullsync(uint64(l)) + l
	}
	return n
}

func (m *Get) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.Bin != 0 {
		n += 1 + sovPullsync(uint64(m.Bin))
	}
	if m.Start != 0 {
		n += 1 + sovPullsync(uint64(m.Start))
	}
	return n
}

func (m *Chunk) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.Address)
	if l > 0 {
		n += 1 + l + sovPullsync(uint64(l))
	}
	l = len(m.BatchID)
	if l > 0 {
		n += 1 + l + sovPullsync(uint64(l))
	}
	return n
}

func (m *Offer) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.Topmost != 0 {
		n += 1 + sovPullsync(uint64(m.Topmost))
	}
	if len(m.Chunks) > 0 {
		for _, e := range m.Chunks {
			l = e.Size()
			n += 1 + l + sovPullsync(uint64(l))
		}
	}
	return n
}

func (m *Want) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.BitVector)
	if l > 0 {
		n += 1 + l + sovPullsync(uint64(l))
	}
	return n
}

func (m *Delivery) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.Address)
	if l > 0 {
		n += 1 + l + sovPullsync(uint64(l))
	}
	l = len(m.Data)
	if l > 0 {
		n += 1 + l + sovPullsync(uint64(l))
	}
	l = len(m.Stamp)
	if l > 0 {
		n += 1 + l + sovPullsync(uint64(l))
	}
	return n
}

func sovPullsync(x uint64) (n int) {
	return (math_bits.Len64(x|1) + 6) / 7
}
func sozPullsync(x uint64) (n int) {
	return sovPullsync(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *Syn) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowPullsync
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: Syn: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Syn: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		default:
			iNdEx = preIndex
			skippy, err := skipPullsync(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthPullsync
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthPullsync
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *Ack) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowPullsync
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: Ack: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Ack: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType == 0 {
				var v uint64
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return ErrIntOverflowPullsync
					}
					if iNdEx >= l {
						return io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					v |= uint64(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				m.Cursors = append(m.Cursors, v)
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return ErrIntOverflowPullsync
					}
					if iNdEx >= l {
						return io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= int(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				if packedLen < 0 {
					return ErrInvalidLengthPullsync
				}
				postIndex := iNdEx + packedLen
				if postIndex < 0 {
					return ErrInvalidLengthPullsync
				}
				if postIndex > l {
					return io.ErrUnexpectedEOF
				}
				var elementCount int
				var count int
				for _, integer := range dAtA[iNdEx:postIndex] {
					if integer < 128 {
						count++
					}
				}
				elementCount = count
				if elementCount != 0 && len(m.Cursors) == 0 {
					m.Cursors = make([]uint64, 0, elementCount)
				}
				for iNdEx < postIndex {
					var v uint64
					for shift := uint(0); ; shift += 7 {
						if shift >= 64 {
							return ErrIntOverflowPullsync
						}
						if iNdEx >= l {
							return io.ErrUnexpectedEOF
						}
						b := dAtA[iNdEx]
						iNdEx++
						v |= uint64(b&0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					m.Cursors = append(m.Cursors, v)
				}
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field Cursors", wireType)
			}
		default:
			iNdEx = preIndex
			skippy, err := skipPullsync(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthPullsync
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthPullsync
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *Get) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowPullsync
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: Get: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Get: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Bin", wireType)
			}
			m.Bin = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowPullsync
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Bin |= int32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Start", wireType)
			}
			m.Start = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowPullsync
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Start |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		default:
			iNdEx = preIndex
			skippy, err := skipPullsync(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthPullsync
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthPullsync
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *Chunk) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowPullsync
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: Chunk: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Chunk: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Address", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowPullsync
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthPullsync
			}
			postIndex := iNdEx + byteLen
			if postIndex < 0 {
				return ErrInvalidLengthPullsync
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Address = append(m.Address[:0], dAtA[iNdEx:postIndex]...)
			if m.Address == nil {
				m.Address = []byte{}
			}
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field BatchID", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowPullsync
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthPullsync
			}
			postIndex := iNdEx + byteLen
			if postIndex < 0 {
				return ErrInvalidLengthPullsync
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.BatchID = append(m.BatchID[:0], dAtA[iNdEx:postIndex]...)
			if m.BatchID == nil {
				m.BatchID = []byte{}
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipPullsync(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthPullsync
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthPullsync
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *Offer) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowPullsync
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: Offer: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Offer: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Topmost", wireType)
			}
			m.Topmost = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowPullsync
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Topmost |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Chunks", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowPullsync
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthPullsync
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthPullsync
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Chunks = append(m.Chunks, &Chunk{})
			if err := m.Chunks[len(m.Chunks)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipPullsync(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthPullsync
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthPullsync
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *Want) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowPullsync
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: Want: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Want: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field BitVector", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowPullsync
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthPullsync
			}
			postIndex := iNdEx + byteLen
			if postIndex < 0 {
				return ErrInvalidLengthPullsync
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.BitVector = append(m.BitVector[:0], dAtA[iNdEx:postIndex]...)
			if m.BitVector == nil {
				m.BitVector = []byte{}
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipPullsync(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthPullsync
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthPullsync
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *Delivery) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowPullsync
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: Delivery: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Delivery: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Address", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowPullsync
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthPullsync
			}
			postIndex := iNdEx + byteLen
			if postIndex < 0 {
				return ErrInvalidLengthPullsync
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Address = append(m.Address[:0], dAtA[iNdEx:postIndex]...)
			if m.Address == nil {
				m.Address = []byte{}
			}
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Data", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowPullsync
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthPullsync
			}
			postIndex := iNdEx + byteLen
			if postIndex < 0 {
				return ErrInvalidLengthPullsync
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Data = append(m.Data[:0], dAtA[iNdEx:postIndex]...)
			if m.Data == nil {
				m.Data = []byte{}
			}
			iNdEx = postIndex
		case 3:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Stamp", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowPullsync
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthPullsync
			}
			postIndex := iNdEx + byteLen
			if postIndex < 0 {
				return ErrInvalidLengthPullsync
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Stamp = append(m.Stamp[:0], dAtA[iNdEx:postIndex]...)
			if m.Stamp == nil {
				m.Stamp = []byte{}
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipPullsync(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthPullsync
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthPullsync
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func skipPullsync(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	depth := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowPullsync
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowPullsync
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
		case 1:
			iNdEx += 8
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowPullsync
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if length < 0 {
				return 0, ErrInvalidLengthPullsync
			}
			iNdEx += length
		case 3:
			depth++
		case 4:
			if depth == 0 {
				return 0, ErrUnexpectedEndOfGroupPullsync
			}
			depth--
		case 5:
			iNdEx += 4
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
		if iNdEx < 0 {
			return 0, ErrInvalidLengthPullsync
		}
		if depth == 0 {
			return iNdEx, nil
		}
	}
	return 0, io.ErrUnexpectedEOF
}

var (
	ErrInvalidLengthPullsync        = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowPullsync          = fmt.Errorf("proto: integer overflow")
	ErrUnexpectedEndOfGroupPullsync = fmt.Errorf("proto: unexpected end of group")
)
