package database

import (
	"errors"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

type Internals struct {
	GUID         uuid.UUID      `gorm:"primaryKey" json:"guid"`
	BlockHash    common.Hash    `gorm:"column:block_hash;serializer:bytes"  db:"block_hash" json:"block_hash"`
	BlockNumber  *big.Int       `gorm:"serializer:u256;column:block_number" db:"block_number" json:"BlockNumber" form:"block_number"`
	Hash         common.Hash    `gorm:"column:hash;serializer:bytes"  db:"hash" json:"hash"`
	FromAddress  common.Address `json:"from_address" gorm:"serializer:bytes;column:from_address"`
	ToAddress    common.Address `json:"to_address" gorm:"serializer:bytes;column:to_address"`
	TokenAddress common.Address `json:"token_address" gorm:"serializer:bytes;column:token_address"`
	TokenId      string         `json:"token_id" gorm:"column:token_id"`
	TokenMeta    string         `json:"token_meta" gorm:"column:token_meta"`
	Fee          *big.Int       `gorm:"serializer:u256;column:fee" db:"fee" json:"Fee" form:"fee"`
	Amount       *big.Int       `gorm:"serializer:u256;column:amount" db:"amount" json:"Amount" form:"amount"`
	Status       uint8          `json:"status"` // 0:交易未签名, 1:交易已签名, 2:交易已经发送到区块链网络；3:交易在钱包层已完成；4:已通知业务；5:成功
	TxType       string         `json:"tx_type"`
	TxSignHex    string         `json:"tx_sign_hex" gorm:"column:tx_sign_hex"`
	Timestamp    uint64
}

type InternalsView interface {
	QueryNotifyInternal(requestId string) ([]Internals, error)
	QueryInternalsByHash(requestId string, txId string) (*Internals, error)
	UnSendInternalsList(requestId string) ([]Internals, error)
}

type InternalsDB interface {
	InternalsView

	StoreInternal(string, *Internals) error
	UpdateInternalTx(requestId string, transactionId string, signedTx string, fee *big.Int, status uint8) error
	UpdateInternalstatus(requestId string, status uint8, InternalsList []Internals) error
}

type internalsDB struct {
	gorm *gorm.DB
}

func NewInternalsDB(db *gorm.DB) InternalsDB {
	return &internalsDB{gorm: db}
}

func (db *internalsDB) QueryNotifyInternal(requestId string) ([]Internals, error) {
	var notifyInternals []Internals
	result := db.gorm.Table("internals_"+requestId).Where("status = ?", 3).Find(notifyInternals)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, gorm.ErrRecordNotFound
		}
		return nil, result.Error
	}
	return notifyInternals, nil
}

func (db *internalsDB) StoreInternal(requestId string, internals *Internals) error {
	result := db.gorm.Table("internals_" + requestId).Create(&internals)
	return result.Error
}

func (db *internalsDB) QueryInternalsByHash(requestId string, txId string) (*Internals, error) {
	var internalsEntity Internals
	result := db.gorm.Table("withdraws_"+requestId).Where("guid", txId).Take(&internalsEntity)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, result.Error
	}
	return &internalsEntity, nil
}

func (db *internalsDB) UnSendInternalsList(requestId string) ([]Internals, error) {
	var InternalsList []Internals
	err := db.gorm.Table("internals_"+requestId).Table("internals").Where("status = ?", 1).Find(&InternalsList).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return InternalsList, nil
}

func (db *internalsDB) UpdateInternalTx(requestId string, transactionId string, signedTx string, fee *big.Int, status uint8) error {
	var InternalsSingle = Internals{}

	result := db.gorm.Table("internals_"+requestId).Where("guid", transactionId).Take(&InternalsSingle)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil
		}
		return result.Error
	}
	if signedTx != "" {
		InternalsSingle.TxSignHex = signedTx
	}
	InternalsSingle.Status = status
	if fee != nil {
		InternalsSingle.Fee = fee
	}
	err := db.gorm.Table("internals_" + requestId).Save(&InternalsSingle).Error
	if err != nil {
		return err
	}
	return nil
}

func (db *internalsDB) UpdateInternalstatus(requestId string, status uint8, InternalsList []Internals) error {
	for i := 0; i < len(InternalsList); i++ {
		var InternalsSingle = Internals{}
		result := db.gorm.Table("internals_" + requestId).Where(&Transactions{Hash: InternalsList[i].Hash}).Take(&InternalsSingle)
		if result.Error != nil {
			if errors.Is(result.Error, gorm.ErrRecordNotFound) {
				return nil
			}
			return result.Error
		}
		InternalsSingle.Status = status
		err := db.gorm.Table("internals_" + requestId).Save(&InternalsSingle).Error
		if err != nil {
			return err
		}
	}
	return nil
}
