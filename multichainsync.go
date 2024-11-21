package multichain_transaction_syncs

import (
	"context"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/log"

	"github.com/dapplink-labs/multichain-sync-account/config"
	"github.com/dapplink-labs/multichain-sync-account/database"
	"github.com/dapplink-labs/multichain-sync-account/worker"
)

type MultiChainSync struct {
	Synchronizer *worker.BaseSynchronizer
	Deposit      *worker.Deposit
	Withdraw     *worker.Withdraw
	Internal     *worker.Internal

	shutdown context.CancelCauseFunc
	stopped  atomic.Bool
}

func NewMultiChainSync(ctx context.Context, cfg *config.Config, shutdown context.CancelCauseFunc) (*MultiChainSync, error) {
	db, err := database.NewDB(ctx, cfg.MasterDB)
	if err != nil {
		log.Error("init database fail", err)
		return nil, err
	}

	deposit, _ := worker.NewDeposit(cfg, db, shutdown)
	withdraw, _ := worker.NewWithdraw(cfg, db, shutdown)
	internal, _ := worker.NewInternal(cfg, db, shutdown)

	out := &MultiChainSync{
		Deposit:  deposit,
		Withdraw: withdraw,
		Internal: internal,
		shutdown: shutdown,
	}
	return out, nil
}

func (mcs *MultiChainSync) Start(ctx context.Context) error {
	err := mcs.Deposit.Start()
	if err != nil {
		return err
	}
	err = mcs.Withdraw.Start()
	if err != nil {
		return err
	}
	err = mcs.Internal.Start()
	if err != nil {
		return err
	}
	return nil
}

func (mcs *MultiChainSync) Stop(ctx context.Context) error {
	err := mcs.Deposit.Close()
	if err != nil {
		return err
	}
	err = mcs.Withdraw.Close()
	if err != nil {
		return err
	}
	err = mcs.Internal.Close()
	if err != nil {
		return err
	}
	return nil
}

func (mcs *MultiChainSync) Stopped() bool {
	return mcs.stopped.Load()
}
