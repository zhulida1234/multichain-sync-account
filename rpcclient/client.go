package rpcclient

import (
	"context"
	"math/big"
	"strconv"

	common2 "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"

	"github.com/dapplink-labs/multichain-sync-account/rpcclient/chain-account/account"
	"github.com/dapplink-labs/multichain-sync-account/rpcclient/chain-account/common"
)

type WalletChainAccountClient struct {
	Ctx             context.Context
	ChainName       string
	AccountRpClient account.WalletAccountServiceClient
}

func NewWalletChainAccountClient(ctx context.Context, rpc account.WalletAccountServiceClient, chainName string) (*WalletChainAccountClient, error) {
	return &WalletChainAccountClient{Ctx: ctx, AccountRpClient: rpc, ChainName: chainName}, nil
}

func (wac *WalletChainAccountClient) ExportAddressByPubKey(method, publicKey string) string {
	req := &account.ConvertAddressRequest{
		Chain:     wac.ChainName,
		Type:      method,
		PublicKey: publicKey,
	}
	address, err := wac.AccountRpClient.ConvertAddress(wac.Ctx, req)
	if address.Code == common.ReturnCode_ERROR {
		log.Error("covert address fail", "err", err)
		return ""
	}
	return address.Address
}

func (wac *WalletChainAccountClient) GetBlockHeader(number *big.Int) (*BlockHeader, error) {
	var height int64
	if number != nil {
		height = 0
	} else {
		height = number.Int64()
	}
	req := &account.BlockHeaderNumberRequest{
		Chain:   wac.ChainName,
		Network: "mainnet",
		Height:  height,
	}
	blockHeader, err := wac.AccountRpClient.GetBlockHeaderByNumber(wac.Ctx, req)
	if blockHeader.Code == common.ReturnCode_ERROR {
		log.Error("get latest block fail", "err", err)
		return nil, err
	}
	blockNumber, _ := new(big.Int).SetString(blockHeader.BlockHeader.Number, 10)
	header := &BlockHeader{
		Hash:       common2.HexToHash(blockHeader.BlockHeader.Hash),
		ParentHash: common2.HexToHash(blockHeader.BlockHeader.ParentHash),
		Number:     blockNumber,
		Timestamp:  blockHeader.BlockHeader.Time,
	}
	return header, nil
}

func (wac *WalletChainAccountClient) GetBlockInfo(blockNumber *big.Int) ([]*account.BlockInfoTransactionList, error) {
	req := &account.BlockNumberRequest{
		Chain:  wac.ChainName,
		Height: blockNumber.Int64(),
		ViewTx: true,
	}
	blockInfo, err := wac.AccountRpClient.GetBlockByNumber(wac.Ctx, req)
	if blockInfo.Code == common.ReturnCode_ERROR {
		log.Error("get block info fail", "err", err)
		return nil, err
	}
	return blockInfo.Transactions, nil
}

func (wac *WalletChainAccountClient) GetTransactionByHash(hash string) (*account.TxMessage, error) {
	req := &account.TxHashRequest{
		Chain:   wac.ChainName,
		Network: "mainnet",
		Hash:    hash,
	}
	txInfo, err := wac.AccountRpClient.GetTxByHash(wac.Ctx, req)
	if txInfo.Code == common.ReturnCode_ERROR {
		log.Error("get block info fail", "err", err)
		return nil, err
	}
	return txInfo.Tx, nil
}

func (wac *WalletChainAccountClient) GetAccount(address string) (int, error) {
	req := &account.AccountRequest{
		Chain:   wac.ChainName,
		Network: "mainnet",
		Address: address,
	}
	accountInfo, err := wac.AccountRpClient.GetAccount(wac.Ctx, req)
	if accountInfo.Code == common.ReturnCode_ERROR {
		log.Error("get block info fail", "err", err)
		return 0, err
	}
	return strconv.Atoi(accountInfo.AccountNumber)
}

func (wac *WalletChainAccountClient) SendTx(rawTx string) (string, error) {
	req := &account.SendTxRequest{
		Chain:   wac.ChainName,
		Network: "mainnet",
		RawTx:   rawTx,
	}
	txInfo, err := wac.AccountRpClient.SendTx(wac.Ctx, req)
	if txInfo.Code == common.ReturnCode_ERROR {
		log.Error("get block info fail", "err", err)
		return "", err
	}
	return txInfo.TxHash, nil
}
