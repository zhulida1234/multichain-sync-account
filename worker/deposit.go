package worker

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strconv"

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

	confirms       uint8
	latestHeader   rpcclient.BlockHeader
	resourceCtx    context.Context
	resourceCancel context.CancelFunc
	tasks          tasks.Group
}

func NewDeposit(cfg *config.Config, db *database.DB, shutdown context.CancelCauseFunc) (*Deposit, error) {
	log.Info("New deposit", "ChainAccountRpc", cfg.ChainAccountRpc)
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

	businessList, err := db.Business.QueryBusinessList()
	if err != nil {
		log.Error("query business list fail", "err", err)
		return nil, err
	}
	var businessIds []string
	for _, business := range businessList {
		businessIds = append(businessIds, business.BusinessUid)
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
		chainLatestBlockHeader, err := accountClient.GetBlockHeader(nil)
		if err != nil {
			log.Error("get block from chain account fail", "err", err)
			return nil, err
		}
		fromHeader = chainLatestBlockHeader
	}

	businessTxChannel := make(chan map[string]*TransactionsChannel)

	baseSyncer := BaseSynchronizer{
		loopInterval:     cfg.ChainNode.SynchronizerInterval,
		headerBufferSize: cfg.ChainNode.BlocksStep,
		businessChannels: businessTxChannel,
		rpcClient:        accountClient,
		blockBatch:       rpcclient.NewBatchBlock(accountClient, fromHeader, big.NewInt(int64(cfg.ChainNode.Confirmations))),
		database:         db,
		businessIds:      businessIds,
	}

	resCtx, resCancel := context.WithCancel(context.Background())

	return &Deposit{
		BaseSynchronizer: baseSyncer,
		confirms:         uint8(cfg.ChainNode.Confirmations),
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
		log.Info("handle deposit task start")
		for batch := range deposit.businessChannels {
			log.Info("deposit business channel", "batch length", len(batch))
			if err := deposit.handleBatch(batch); err != nil {
				return fmt.Errorf("failed to handle batch, stopping L2 Synchronizer: %w", err)
			}
		}
		return nil
	})
	return nil
}

func (deposit *Deposit) handleBatch(batch map[string]*TransactionsChannel) error {
	var (
		transationFlowList []database.Transactions
		depositList        []database.Deposits
		withdrawList       []database.Withdraws
	)

	for _, businessId := range deposit.businessIds {
		_, exists := batch[businessId]
		if !exists {
			continue
		}

		chainLatestBlock := batch[businessId].BlockHeight
		batchTransactions := batch[businessId].Transactions
		log.Info("handle business flow", "businessId", businessId, "chainLatestBlock", batch[businessId].BlockHeight, "txn", len(batch[businessId].Transactions))

		for _, tx := range batchTransactions {
			log.Info("Request transaction from chain account", "txHash", tx.Hash)
			txItem, err := deposit.rpcClient.GetTransactionByHash(tx.Hash)
			if err != nil {
				log.Info("get transaction by hash fail", "err", err)
				return err
			}
			log.Info("get transaction success", "txHash", txItem.Hash)
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
					if err := tx.Deposits.StoreDeposits(businessId, depositList, uint64(len(depositList))); err != nil {
						return err
					}
					log.Info("update deposit transaction confirms", "totalTx", len(depositList))
					if err := tx.Deposits.UpdateDepositsComfirms(businessId, chainLatestBlock, uint64(deposit.confirms)); err != nil {
						return err
					}
				}

				if len(withdrawList) > 0 {
					if err := tx.Withdraws.UpdateWithdrawStatus(businessId, 3, withdrawList); err != nil {
						return err
					}
				}

				if len(transationFlowList) > 0 {
					if err := tx.Transactions.StoreTransactions(businessId, transationFlowList, uint64(len(transationFlowList))); err != nil {
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
	}
	return nil
}
