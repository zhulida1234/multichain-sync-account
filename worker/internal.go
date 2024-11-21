package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"

	"github.com/dapplink-labs/multichain-sync-account/common/tasks"
	"github.com/dapplink-labs/multichain-sync-account/config"
	"github.com/dapplink-labs/multichain-sync-account/database"
	"github.com/dapplink-labs/multichain-sync-account/rpcclient"
)

type Internal struct {
	rpcClient      *rpcclient.WalletChainAccountClient
	db             *database.DB
	resourceCtx    context.Context
	resourceCancel context.CancelFunc
	tasks          tasks.Group
	ticker         *time.Ticker
}

func NewInternal(cfg *config.Config, db *database.DB, shutdown context.CancelCauseFunc) (*Internal, error) {
	resCtx, resCancel := context.WithCancel(context.Background())
	return &Internal{
		db:             db,
		resourceCtx:    resCtx,
		resourceCancel: resCancel,
		tasks: tasks.Group{HandleCrit: func(err error) {
			shutdown(fmt.Errorf("critical error in internals: %w", err))
		}},
		ticker: time.NewTicker(time.Second * 5),
	}, nil
}

func (w *Internal) Close() error {
	var result error
	w.resourceCancel()
	w.ticker.Stop()
	log.Info("stop internal......")
	if err := w.tasks.Wait(); err != nil {
		result = errors.Join(result, fmt.Errorf("failed to await internal %w"), err)
		return result
	}
	log.Info("stop internal success")
	return nil
}

func (w *Internal) Start() error {
	log.Info("start internals......")
	w.tasks.Go(func() error {
		for {
			select {
			case <-w.ticker.C:
				businessList, err := w.db.Business.QueryBusinessList()
				if err != nil {
					log.Error("query business list fail", "err", err)
					return err
				}

				for _, businessId := range businessList {
					unSendInternalTxList, err := w.db.Internals.UnSendInternalsList(businessId.BusinessUid)
					if err != nil {
						return err
					}

					for _, unSendInternalTx := range unSendInternalTxList {
						txHash, err := w.rpcClient.SendTx(unSendInternalTx.TxSignHex)
						if err != nil {
							log.Error("send transaction fail", "err", err)
							return err
						} else {
							unSendInternalTx.Hash = common.HexToHash(txHash)
							unSendInternalTx.Status = 2
						}
					}

					err = w.db.Internals.UpdateInternalstatus(businessId.BusinessUid, 3, unSendInternalTxList)
					if err != nil {
						log.Error("update internals status fail", "err", err)
						return err
					}
				}
			case <-w.resourceCtx.Done():
				log.Info("stop internals in worker")
				return nil
			}
		}
	})
	return nil
}
