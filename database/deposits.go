package database

import (
	"errors"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
)

type Deposits struct {
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
	Confirms     uint8          `json:"confirms"` // 交易确认位
	Status       uint8          `json:"status"`   // 0:充值确认中,1:充值钱包层已到账；2:充值已通知业务层；3:充值完成;
	Timestamp    uint64
}

type DepositsView interface {
	QueryNotifyDeposits(string) ([]Deposits, error)
}

type DepositsDB interface {
	DepositsView

	StoreDeposits(string, []Deposits, uint64) error
	UpdateDepositsComfirms(requestId string, blockNumber uint64, confirms uint64) error
	UpdateDepositsNotifyStatus(requestId string, status uint8, depositList []Deposits) error
}

type depositsDB struct {
	gorm *gorm.DB
}

func (db *depositsDB) QueryNotifyDeposits(requestId string) ([]Deposits, error) {
	var notifyDeposits []Deposits
	result := db.gorm.Table("deposits_"+requestId).Where("status = ? or status = ?", 0, 1).Find(notifyDeposits)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, gorm.ErrRecordNotFound
		}
		return nil, result.Error
	}
	return notifyDeposits, nil
}

// UpdateDepositsComfirms 查询所有还没有过确认位交易，用最新区块减去对应区块更新确认，如果这个大于我们预设的确认位，那么这笔交易可以认为已经入账
func (db *depositsDB) UpdateDepositsComfirms(requestId string, blockNumber uint64, confirms uint64) error {
	var unConfirmDeposits []Deposits
	result := db.gorm.Table("deposits_"+requestId).Where("block_number <= ? and status", blockNumber, 0).Find(unConfirmDeposits)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil
		}
		return result.Error
	}
	for _, deposit := range unConfirmDeposits {
		chainConfirm := blockNumber - deposit.BlockNumber.Uint64()
		if chainConfirm >= confirms {
			deposit.Confirms = uint8(confirms)
			deposit.Status = 1
		} else {
			deposit.Confirms = uint8(chainConfirm)
		}
		err := db.gorm.Table("deposits_" + requestId).Save(&deposit).Error
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *depositsDB) UpdateDepositsNotifyStatus(requestId string, status uint8, depositList []Deposits) error {
	for i := 0; i < len(depositList); i++ {
		var depositSingle = Deposits{}
		result := db.gorm.Table("deposits_" + requestId).Where(&Transactions{Hash: depositList[i].Hash}).Take(&depositSingle)
		if result.Error != nil {
			if errors.Is(result.Error, gorm.ErrRecordNotFound) {
				return nil
			}
			return result.Error
		}
		depositSingle.Status = status
		err := db.gorm.Table("transactions_" + requestId).Save(&depositSingle).Error
		if err != nil {
			return err
		}
	}
	return nil
}

func NewDepositsDB(db *gorm.DB) DepositsDB {
	return &depositsDB{gorm: db}
}

func (db *depositsDB) StoreDeposits(requestId string, depositList []Deposits, depositLength uint64) error {
	result := db.gorm.Table("deposits_"+requestId).CreateInBatches(&depositList, int(depositLength))
	if result.Error != nil {
		log.Error("create deposit batch fail", "Err", result.Error)
		return result.Error
	}
	return nil
}
