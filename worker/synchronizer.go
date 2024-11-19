package worker

import (
	"context"
	"errors"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"

	"github.com/dapplink-labs/multichain-sync-account/common/clock"
	"github.com/dapplink-labs/multichain-sync-account/database"
	"github.com/dapplink-labs/multichain-sync-account/rpcclient"
)

type Transaction struct {
	BusinessId     string
	BlockNumber    *big.Int
	FromAddress    string
	ToAddress      string
	Hash           string
	TokenAddress   string
	ContractWallet string
	TxType         string
}

type Config struct {
	LoopIntervalMsec uint
	HeaderBufferSize uint
	StartHeight      *big.Int
	Confirmations    uint64
}

type BaseSynchronizer struct {
	loopInterval     time.Duration
	headerBufferSize uint64
	syncerBatches    chan *BaseSynchronizerBatch
	rpcClient        *rpcclient.WalletChainAccountClient
	blockBatch       *rpcclient.BatchBlock
	database         *database.DB

	headers []rpcclient.BlockHeader
	worker  *clock.LoopFn
}

type BaseSynchronizerBatch struct {
	Headers      []rpcclient.BlockHeader
	HeaderMap    map[common.Hash]*rpcclient.BlockHeader
	Transactions []*Transaction
}

func (syncer *BaseSynchronizer) Start() error {
	if syncer.worker != nil {
		return errors.New("already started")
	}
	syncer.worker = clock.NewLoopFn(clock.SystemClock, syncer.tick, func() error {
		log.Info("shutting down batch producer")
		close(syncer.syncerBatches)
		return nil
	}, syncer.loopInterval)
	return nil
}

func (syncer *BaseSynchronizer) Close() error {
	if syncer.worker == nil {
		return nil
	}
	return syncer.worker.Close()
}

func (syncer *BaseSynchronizer) tick(_ context.Context) {
	if len(syncer.headers) > 0 {
		log.Info("retrying previous batch")
	} else {
		newHeaders, err := syncer.blockBatch.NextHeaders(syncer.headerBufferSize)
		if err != nil {
			log.Error("error querying for headers", "err", err)
		} else if len(newHeaders) == 0 {
			log.Warn("no new headers. syncer at head?")
		} else {
			syncer.headers = newHeaders
		}
	}
	err := syncer.processBatch(syncer.headers)
	if err == nil {
		syncer.headers = nil
	}
}

func (syncer *BaseSynchronizer) processBatch(headers []rpcclient.BlockHeader) error {
	if len(headers) == 0 {
		return nil
	}
	headerMap := make(map[common.Hash]*rpcclient.BlockHeader, len(headers))
	var businessTransactions []*Transaction
	for i := range headers {
		header := headers[i]
		txList, err := syncer.rpcClient.GetBlockInfo(header.Number)
		if err != nil {
			log.Error("get block info fail", "err", err)
			return err
		}
		businessList, err := syncer.database.Business.QueryBusinessList()
		if err != nil {
			log.Error("query business list fail", "err", err)
			return err
		}
		/* If the 'from' address is an external address and the 'to' address is an internal user address, it is a deposit; call the callback interface to notify the business side.
		 * If the 'from' address is a user address and the 'to' address is a hot wallet address, it is consolidation; call the callback interface to notify the business side.
		 * If the 'from' address is a hot wallet address and the 'to' address is an external user address, it is a withdrawal; call the callback interface to notify the business side.
		 * If the 'from' address is a hot wallet address and the 'to' address is a cold wallet address, it is a hot-to-cold transfer; call the callback interface to notify the business side.
		 * If the 'from' address is a cold wallet address and the 'to' address is a hot wallet address, it is a cold-to-hot transfer; call the callback interface to notify the business side.
		 */
		for _, tx := range txList {
			toAddress := common.HexToAddress(tx.To)
			fromAddress := common.HexToAddress(tx.From)
			for _, business := range businessList {
				existToAddress, toAddressType := syncer.database.Addresses.AddressExist(business.BusinessUid, &toAddress)
				existFromAddress, FromAddressType := syncer.database.Addresses.AddressExist(business.BusinessUid, &fromAddress)
				if !existToAddress && !existFromAddress {
					continue
				}
				txItem := &Transaction{
					BusinessId:     business.BusinessUid,
					BlockNumber:    header.Number,
					FromAddress:    tx.From,
					ToAddress:      tx.To,
					Hash:           tx.Hash,
					TokenAddress:   tx.TokenAddress,
					ContractWallet: tx.ContractWallet,
					TxType:         "unknow",
				}

				if !existFromAddress && (existToAddress && toAddressType == 0) { // 充值
					txItem.TxType = "deposit"
				}

				if (existFromAddress && FromAddressType == 1) && !existToAddress { // 提现
					txItem.TxType = "withdraw"
				}

				if (existFromAddress && FromAddressType == 0) && (existToAddress && toAddressType == 1) { // 归集
					txItem.TxType = "collection"
				}

				if (existFromAddress && FromAddressType == 1) && (existToAddress && toAddressType == 2) { // 热转冷
					txItem.TxType = "hot2cold"
				}

				if (existFromAddress && FromAddressType == 2) && (existToAddress && toAddressType == 1) { // 热转冷
					txItem.TxType = "cold2hot"
				}
				businessTransactions = append(businessTransactions, txItem)
			}
		}
		headerMap[header.Hash] = &header
	}
	headersRef := headers
	syncer.syncerBatches <- &BaseSynchronizerBatch{Transactions: businessTransactions, Headers: headersRef, HeaderMap: headerMap}
	return nil
}
