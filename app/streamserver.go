package app

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/cosmos/cosmos-sdk/baseapp"
	sdk "github.com/cosmos/cosmos-sdk/types"
	abci "github.com/tendermint/tendermint/abci/types"
	"github.com/tendermint/tendermint/types"
	"os"
	"strings"
)

type (
	Block struct {
		Height   int64  `json:"height"`
		Hash     string `json:"hash"`
		Txn      int64  `json:"txn"`
		Time     int64  `json:"time"`
		Proposer string `json:"proposer"`
	}
	DeliverTx struct {
		Tx       string                 `json:"tx"`
		TxResult abci.ResponseDeliverTx `json:"tx_result"`
	}
	Txs struct {
		Txs []DeliverTx `json:"txs"`
	}
)

// FileStreamingService is a concrete implementation of StreamingService that writes state changes out to a file
type FileStreamingService struct {
	header     types.Header
	txCache    []DeliverTx // the cache that write tx out to
	filePrefix string      // optional prefix for each of the generated files
	writeDir   string      // directory to write files into
}

func (fss *FileStreamingService) ListenBeginBlock(ctx sdk.Context, req abci.RequestBeginBlock, res abci.ResponseBeginBlock) {
	var err error
	fss.header, err = types.HeaderFromProto(&req.Header)
	if err != nil {
		ctx.Logger().Error(err.Error())
		return
	}
	// NOTE: this could either be done synchronously or asynchronously
	// create a new file with the req info according to naming schema
	// write req to file
	// write all state changes cached for this stage to file
	// reset cache
	// write res to file
	// close file
}

func (fss *FileStreamingService) ListenEndBlock(ctx sdk.Context, req abci.RequestEndBlock, res abci.ResponseEndBlock) {
	// NOTE: this could either be done synchronously or asynchronously
	// create a new file with the req info according to naming schema
	// write req to file
	// write all state changes cached for this stage to file
	// reset cache
	// write res to file
	// close file
	defer func() {
		fss.txCache = make([]DeliverTx, 0)
		fss.header = types.Header{}
	}()
	if strings.Contains(fss.filePrefix, "_") {
		fss.filePrefix = fss.filePrefix[:strings.Index(fss.filePrefix, "_")]
	}
	fss.filePrefix = fmt.Sprint(fss.filePrefix, "_", req.Height)

	if len(fss.txCache) > 0 {
		data, err := json.Marshal(Txs{
			Txs: fss.txCache,
		})
		if err == nil {
			filename := fmt.Sprint(fss.writeDir, "/", fss.filePrefix, "_txs")
			file, err := os.Create(filename)
			if err != nil {
				ctx.Logger().Error(err.Error())
				return
			}

			file.Write(data)
			file.Close()
		}
	}

	data, err := json.Marshal(Block{
		Height:   fss.header.Height,
		Time:     fss.header.Time.Unix(),
		Hash:     fss.header.Hash().String(),
		Txn:      int64(len(fss.txCache)),
		Proposer: fss.header.ProposerAddress.String(),
	})
	if err == nil {
		blockfilename := fmt.Sprint(fss.writeDir, "/", fss.filePrefix, "_block")
		file, err := os.Create(blockfilename)
		if err != nil {
			ctx.Logger().Error(err.Error())
			return
		}

		file.Write(data)
		file.Close()
	}

}

func (fss *FileStreamingService) ListenDeliverTx(ctx sdk.Context, req abci.RequestDeliverTx, res abci.ResponseDeliverTx) {
	// NOTE: this could either be done synchronously or asynchronously
	// create a new file with the req info according to naming schema
	// NOTE: if the tx failed, handle accordingly
	// write req to file
	// write all state changes cached for this stage to file
	// reset cache
	// write res to file
	// close file

	fss.txCache = append(fss.txCache, DeliverTx{
		Tx:       hex.EncodeToString(req.Tx),
		TxResult: res,
	})

}

// NewFileStreamingService creates a new FileStreamingService for the provided writeDir, (optional) filePrefix, and storeKeys
func NewFileStreamingService(writeDir, filePrefix string) baseapp.Hook {
	return &FileStreamingService{
		filePrefix: filePrefix,
		writeDir:   writeDir,
		txCache:    make([]DeliverTx, 0),
	}
}
