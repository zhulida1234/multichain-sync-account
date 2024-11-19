package worker

import (
	"context"
	"errors"
	"fmt"

	"math/big"
	"strconv"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"

	"github.com/dapplink-labs/multichain-sync-account/common/retry"
	"github.com/dapplink-labs/multichain-sync-account/common/tasks"
	"github.com/dapplink-labs/multichain-sync-account/config"
	"github.com/dapplink-labs/multichain-sync-account/database"
	"github.com/dapplink-labs/multichain-sync-account/rpcclient"
	"github.com/dapplink-labs/multichain-sync-account/rpcclient/chain-account/account"
)

type Deposit struct {
	BaseSynchronizer

	latestHeader   rpcclient.BlockHeader
	resourceCtx    context.Context
	resourceCancel context.CancelFunc
	tasks          tasks.Group
}

func NewDeposit(cfg *config.Config, db *database.DB, shutdown context.CancelCauseFunc) (*Deposit, error) {
	conn, err := grpc.NewClient(cfg.ChainAccountRpc, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Error("Connect to da retriever fail", "err", err)
		return nil, err
	}
	client := account.NewWalletAccountServiceClient(conn)
	accountClient, err := rpcclient.NewWalletChainAccountClient(context.Background(), client, "Ethereum")
	if err != nil {
		log.Error("new wallet account client fail", "err", err)
		return nil, err
	}

	dbLatestBlockHeader, err := db.Blocks.LatestBlocks()
	if err != nil {
		log.Error("get latest block from database fail")
		return nil, err
	}
	var fromHeader *rpcclient.BlockHeader

	if dbLatestBlockHeader != nil {
		log.Info("sync bock", "number", dbLatestBlockHeader.Number, "hash", dbLatestBlockHeader.Hash)
		fromHeader = dbLatestBlockHeader
	} else if cfg.ChainNode.StartingHeight > 0 {
		chainLatestBlockHeader, err := accountClient.GetBlockHeader(big.NewInt(int64(cfg.ChainNode.StartingHeight)))
		if err != nil {
			log.Error("get block from chain account fail", "err", err)
			return nil, err
		}
		fromHeader = chainLatestBlockHeader
	} else {
		chainLatestBlockHeader, err := accountClient.GetBlockHeader(big.NewInt(int64(1)))
		if err != nil {
			log.Error("get block from chain account fail", "err", err)
			return nil, err
		}
		fromHeader = chainLatestBlockHeader
	}

	baseSyncer := BaseSynchronizer{
		loopInterval:     time.Duration(cfg.ChainNode.SynchronizerInterval) * time.Second,
		headerBufferSize: cfg.ChainNode.BlocksStep,
		rpcClient:        accountClient,
		blockBatch:       rpcclient.NewBatchBlock(accountClient, fromHeader, big.NewInt(int64(cfg.ChainNode.Confirmations))),
		database:         db,
	}

	resCtx, resCancel := context.WithCancel(context.Background())

	return &Deposit{
		BaseSynchronizer: baseSyncer,
		resourceCtx:      resCtx,
		resourceCancel:   resCancel,
		tasks: tasks.Group{HandleCrit: func(err error) {
			shutdown(fmt.Errorf("critical error in deposit: %w", err))
		}},
	}, nil
}

func (deposit *Deposit) Close() error {
	var result error
	if err := deposit.BaseSynchronizer.Close(); err != nil {
		result = errors.Join(result, fmt.Errorf("failed to close internal base synchronizer: %w", err))
	}
	deposit.resourceCancel()
	if err := deposit.tasks.Wait(); err != nil {
		result = errors.Join(result, fmt.Errorf("failed to await batch handler completion: %w", err))
	}
	return result
}

func (deposit *Deposit) Start() error {
	log.Info("starting deposit...")
	if err := deposit.BaseSynchronizer.Start(); err != nil {
		return fmt.Errorf("failed to start internal Synchronizer: %w", err)
	}
	deposit.tasks.Go(func() error {
		for batch := range deposit.syncerBatches {
			if err := deposit.handleBatch(batch); err != nil {
				return fmt.Errorf("failed to handle batch, stopping L2 Synchronizer: %w", err)
			}
		}
		return nil
	})
	return nil
}

func (deposit *Deposit) handleBatch(batch *BaseSynchronizerBatch) error {
	var transationFlowList []database.Transactions
	var depositList []database.Deposits
	var withdrawList []database.Withdraws
	for i := range batch.Transactions {
		tx := batch.Transactions[i]
		txItem, err := deposit.rpcClient.GetTransactionByHash(tx.Hash)
		if err != nil {
			log.Info("get transaction by hash fail", "err", err)
			return err
		}
		txFee, _ := new(big.Int).SetString(txItem.Fee, 10)
		txAmount, _ := new(big.Int).SetString(txItem.Values[0].Value, 10)
		timestamp, _ := strconv.Atoi(txItem.Datetime)
		transationFlow := database.Transactions{
			GUID:         uuid.New(),
			BlockHash:    common.Hash{},
			BlockNumber:  tx.BlockNumber,
			Hash:         common.HexToHash(tx.Hash),
			FromAddress:  common.HexToAddress(tx.FromAddress),
			ToAddress:    common.HexToAddress(tx.ToAddress),
			TokenAddress: common.HexToAddress(tx.TokenAddress),
			TokenId:      "0x00",
			TokenMeta:    "0x00",
			Fee:          txFee,
			Amount:       txAmount,
			Status:       0,
			TxType:       0,
			Timestamp:    uint64(timestamp),
		}
		switch tx.TxType {
		case "deposit":
			depositItme := database.Deposits{
				GUID:         uuid.New(),
				BlockHash:    common.Hash{},
				BlockNumber:  tx.BlockNumber,
				Hash:         common.HexToHash(tx.Hash),
				FromAddress:  common.HexToAddress(tx.FromAddress),
				ToAddress:    common.HexToAddress(tx.ToAddress),
				TokenAddress: common.HexToAddress(tx.TokenAddress),
				TokenId:      "0x00",
				TokenMeta:    "0x00",
				Fee:          txFee,
				Amount:       txAmount,
				Status:       0,
				Timestamp:    uint64(timestamp),
			}
			depositList = append(depositList, depositItme)
			transationFlow.TxType = 0
			break
		case "withdraw":
			withdrawItem := database.Withdraws{
				GUID:         uuid.New(),
				BlockHash:    common.Hash{},
				BlockNumber:  tx.BlockNumber,
				Hash:         common.HexToHash(tx.Hash),
				FromAddress:  common.HexToAddress(tx.FromAddress),
				ToAddress:    common.HexToAddress(tx.ToAddress),
				TokenAddress: common.HexToAddress(tx.TokenAddress),
				TokenId:      "0x00",
				TokenMeta:    "0x00",
				Fee:          txFee,
				Amount:       txAmount,
				Status:       2,
				Timestamp:    uint64(timestamp),
			}
			withdrawList = append(withdrawList, withdrawItem)
			transationFlow.TxType = 1
			break
		case "collection":
			transationFlow.TxType = 2
			break
		case "hot2cold":
			transationFlow.TxType = 3
			break
		case "cold2hot":
			transationFlow.TxType = 4
			break
		default:
			break
		}
		transationFlowList = append(transationFlowList, transationFlow)
	}
	retryStrategy := &retry.ExponentialStrategy{Min: 1000, Max: 20_000, MaxJitter: 250}
	if _, err := retry.Do[interface{}](deposit.resourceCtx, 10, retryStrategy, func() (interface{}, error) {
		if err := deposit.database.Transaction(func(tx *database.DB) error {
			if len(depositList) > 0 {
				log.Info("Store deposit transaction success", "totalTx", len(depositList))
				if err := tx.Deposits.StoreDeposits("", depositList, uint64(len(depositList))); err != nil {
					return err
				}
			}
			if len(transationFlowList) > 0 {
				if err := tx.Transactions.StoreTransactions("", transationFlowList, uint64(len(transationFlowList))); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			log.Error("unable to persist batch", "err", err)
			return nil, err
		}
		return nil, nil
	}); err != nil {
		return err
	}
	return nil
}
