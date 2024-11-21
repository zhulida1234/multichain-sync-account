package database

import (
	"errors"
	"gorm.io/gorm"
	"math/big"
	"time"

	"github.com/google/uuid"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
)

type Withdraws struct {
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
	Status       uint8          `json:"status"` // 0:提现未签名, 1:提现交易已签名, 2:提现已经发送到区块链网络；3:提现在钱包层已完成；4:提现已通知业务；5:提现成功
	TxSignHex    string         `json:"tx_sign_hex" gorm:"column:tx_sign_hex"`
	Timestamp    uint64
}

type WithdrawsView interface {
	QueryNotifyWithdraws(string) ([]Withdraws, error)
	QueryWithdrawsByHash(requestId string, txId string) (*Withdraws, error)
	UnSendWithdrawsList(requestId string) ([]Withdraws, error)

	SubmitWithdrawFromBusiness(requestId string, fromAddress common.Address, toAddress common.Address, TokenAddress common.Address, amount *big.Int) error
}

type WithdrawsDB interface {
	WithdrawsView

	StoreWithdraw(string, *Withdraws) error
	UpdateWithdrawTx(requestId string, transactionId string, signedTx string, fee *big.Int, status uint8) error
	UpdateWithdrawStatus(requestId string, status uint8, withdrawsList []Withdraws) error
}

type withdrawsDB struct {
	gorm *gorm.DB
}

func (db *withdrawsDB) QueryNotifyWithdraws(requestId string) ([]Withdraws, error) {
	var notifyWithdraws []Withdraws
	result := db.gorm.Table("withdraws_"+requestId).Where("status = ?", 3).Find(notifyWithdraws)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, gorm.ErrRecordNotFound
		}
		return nil, result.Error
	}
	return notifyWithdraws, nil
}

func (db *withdrawsDB) UnSendWithdrawsList(requestId string) ([]Withdraws, error) {
	var withdrawsList []Withdraws
	err := db.gorm.Table("withdraws_"+requestId).Table("withdraws").Where("status = ?", 1).Find(&withdrawsList).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return withdrawsList, nil
}

func (db *withdrawsDB) QueryWithdrawsByHash(requestId string, txId string) (*Withdraws, error) {
	var withdrawsEntity Withdraws
	result := db.gorm.Table("withdraws_"+requestId).Where("guid", txId).Take(&withdrawsEntity)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, result.Error
	}
	return &withdrawsEntity, nil
}

func (db *withdrawsDB) SubmitWithdrawFromBusiness(requestId string, fromAddress common.Address, toAddress common.Address, TokenAddress common.Address, amount *big.Int) error {
	withdrawS := Withdraws{
		GUID:         uuid.New(),
		BlockHash:    common.Hash{},
		BlockNumber:  big.NewInt(1),
		Hash:         common.Hash{},
		FromAddress:  fromAddress,
		ToAddress:    toAddress,
		TokenAddress: TokenAddress,
		Fee:          big.NewInt(1),
		Amount:       amount,
		Status:       0,
		TxSignHex:    "",
		Timestamp:    uint64(time.Now().Unix()),
	}
	errC := db.gorm.Table("withdraws_" + requestId).Create(withdrawS).Error
	if errC != nil {
		log.Error("create withdraw fail", "err", errC)
		return errC
	}
	return nil
}

func (db *withdrawsDB) UpdateWithdrawTx(requestId string, transactionId string, signedTx string, fee *big.Int, status uint8) error {
	var withdrawsSingle = Withdraws{}

	result := db.gorm.Table("withdraws_"+requestId).Where("guid", transactionId).Take(&withdrawsSingle)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil
		}
		return result.Error
	}
	if signedTx != "" {
		withdrawsSingle.TxSignHex = signedTx
	}
	withdrawsSingle.Status = status
	if fee != nil {
		withdrawsSingle.Fee = fee
	}
	err := db.gorm.Table("withdraws_" + requestId).Save(&withdrawsSingle).Error
	if err != nil {
		return err
	}
	return nil
}

func NewWithdrawsDB(db *gorm.DB) WithdrawsDB {
	return &withdrawsDB{gorm: db}
}

func (db *withdrawsDB) StoreWithdraw(requestId string, withdrawsList *Withdraws) error {
	result := db.gorm.Table("withdraws_" + requestId).Create(&withdrawsList)
	return result.Error
}

func (db *withdrawsDB) UpdateWithdrawStatus(requestId string, status uint8, withdrawsList []Withdraws) error {
	for i := 0; i < len(withdrawsList); i++ {
		var withdrawsSingle = Withdraws{}
		result := db.gorm.Table("withdraws_" + requestId).Where(&Transactions{Hash: withdrawsList[i].Hash}).Take(&withdrawsSingle)
		if result.Error != nil {
			if errors.Is(result.Error, gorm.ErrRecordNotFound) {
				return nil
			}
			return result.Error
		}
		withdrawsSingle.Status = status
		err := db.gorm.Table("withdraws_" + requestId).Save(&withdrawsSingle).Error
		if err != nil {
			return err
		}
	}
	return nil
}
