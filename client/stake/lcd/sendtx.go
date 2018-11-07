package lcd

import (
	"github.com/cosmos/cosmos-sdk/codec"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/x/stake"
	"github.com/gorilla/mux"
	"github.com/irisnet/irishub/client/context"
	"github.com/irisnet/irishub/client/utils"
	"net/http"
)

func registerTxRoutes(cliCtx context.CLIContext, r *mux.Router, cdc *codec.Codec) {
	r.HandleFunc(
		"/stake/delegators/{delegatorAddr}/delegate",
		delegationsRequestHandlerFn(cdc, cliCtx),
	).Methods("POST")

	r.HandleFunc(
		"/stake/delegators/{delegatorAddr}/redelegate",
		beginRedelegatesRequestHandlerFn(cdc, cliCtx),
	).Methods("POST")

	r.HandleFunc(
		"/stake/delegators/{delegatorAddr}/unbond",
		beginUnbondingRequestHandlerFn(cdc, cliCtx),
	).Methods("POST")
}

type (
	msgDelegateInput struct {
		DelegatorAddr string `json:"delegator_addr"` // in bech32
		ValidatorAddr string `json:"validator_addr"` // in bech32
		Delegation    string `json:"delegation"`
	}

	msgRedelegateInput struct {
		DelegatorAddr    string `json:"delegator_addr"`     // in bech32
		ValidatorSrcAddr string `json:"validator_src_addr"` // in bech32
		ValidatorDstAddr string `json:"validator_dst_addr"` // in bech32
		SharesAmount     string `json:"shares"`
	}

	msgUnbondInput struct {
		DelegatorAddr string `json:"delegator_addr"` // in bech32
		ValidatorAddr string `json:"validator_addr"` // in bech32
		SharesAmount  string `json:"shares"`
	}

	// the request body for edit delegations
	DelegationsReq struct {
		BaseReq       context.BaseTx      `json:"base_tx"`
		Delegation    msgDelegateInput    `json:"delegate"`
	}

	BeginUnbondingReq struct {
		BaseReq        context.BaseTx  `json:"base_tx"`
		BeginUnbond    msgUnbondInput  `json:"unbond"`
	}

	BeginRedelegatesReq struct {
		BaseReq         context.BaseTx     `json:"base_tx"`
		BeginRedelegate msgRedelegateInput `json:"redelegate"`
	}
)

// TODO: Split this up into several smaller functions, and remove the above nolint
// TODO: use sdk.ValAddress instead of sdk.AccAddress for validators in messages
// TODO: Seriously consider how to refactor...do we need to make it multiple txs?
// If not, we can just use CompleteAndBroadcastTxREST.
func delegationsRequestHandlerFn(cdc *codec.Codec, cliCtx context.CLIContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req DelegationsReq

		err := utils.ReadPostBody(w, r, cdc, &req)
		if err != nil {
			return
		}

		baseReq := req.BaseReq.Sanitize()
		if !baseReq.ValidateBasic(w, cliCtx) {
			return
		}

		// build messages
		delAddr, err := sdk.AccAddressFromBech32(req.Delegation.DelegatorAddr)
		if err != nil {
			utils.WriteErrorResponse(w, http.StatusBadRequest, err.Error())
			return
		}

		valAddr, err := sdk.ValAddressFromBech32(req.Delegation.ValidatorAddr)
		if err != nil {
			utils.WriteErrorResponse(w, http.StatusBadRequest, err.Error())
			return
		}

		delegationToken, err := cliCtx.ParseCoin(req.Delegation.Delegation)
		if err != nil {
			utils.WriteErrorResponse(w, http.StatusInternalServerError, err.Error())
			return
		}

		msg := stake.MsgDelegate{
			DelegatorAddr: delAddr,
			ValidatorAddr: valAddr,
			Delegation:    delegationToken}
		// Broadcast or return unsigned transaction
		utils.SendOrReturnUnsignedTx(w, cliCtx, req.BaseReq, []sdk.Msg{msg})
	}
}

// TODO: Split this up into several smaller functions, and remove the above nolint
// TODO: use sdk.ValAddress instead of sdk.AccAddress for validators in messages
// TODO: Seriously consider how to refactor...do we need to make it multiple txs?
// If not, we can just use CompleteAndBroadcastTxREST.
func beginRedelegatesRequestHandlerFn(cdc *codec.Codec, cliCtx context.CLIContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req BeginRedelegatesReq

		err := utils.ReadPostBody(w, r, cdc, &req)
		if err != nil {
			return
		}

		baseReq := req.BaseReq.Sanitize()
		if !baseReq.ValidateBasic(w, cliCtx) {
			return
		}

		delAddr, err := sdk.AccAddressFromBech32(req.BeginRedelegate.DelegatorAddr)
		if err != nil {
			utils.WriteErrorResponse(w, http.StatusBadRequest, err.Error())
			return
		}

		valSrcAddr, err := sdk.ValAddressFromBech32(req.BeginRedelegate.ValidatorSrcAddr)
		if err != nil {
			utils.WriteErrorResponse(w, http.StatusBadRequest, err.Error())
			return
		}
		valDstAddr, err := sdk.ValAddressFromBech32(req.BeginRedelegate.ValidatorDstAddr)
		if err != nil {
			utils.WriteErrorResponse(w, http.StatusBadRequest, err.Error())
			return
		}

		shares, err := sdk.NewDecFromStr(req.BeginRedelegate.SharesAmount)
		if err != nil {
			utils.WriteErrorResponse(w, http.StatusBadRequest, err.Error())
			return
		}

		msg := stake.MsgBeginRedelegate{
			DelegatorAddr:    delAddr,
			ValidatorSrcAddr: valSrcAddr,
			ValidatorDstAddr: valDstAddr,
			SharesAmount:     sdk.NewDecFromInt(utils.ConvertDecToRat(shares).Quo(utils.ExRateFromStakeTokenToMainUnit(cliCtx)).Num()),
		}

		utils.SendOrReturnUnsignedTx(w, cliCtx, req.BaseReq, []sdk.Msg{msg})
	}
}

// TODO: Split this up into several smaller functions, and remove the above nolint
// TODO: use sdk.ValAddress instead of sdk.AccAddress for validators in messages
// TODO: Seriously consider how to refactor...do we need to make it multiple txs?
// If not, we can just use CompleteAndBroadcastTxREST.
func beginUnbondingRequestHandlerFn(cdc *codec.Codec, cliCtx context.CLIContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req BeginUnbondingReq

		err := utils.ReadPostBody(w, r, cdc, &req)
		if err != nil {
			return
		}

		baseReq := req.BaseReq.Sanitize()
		if !baseReq.ValidateBasic(w, cliCtx) {
			return
		}

		delAddr, err := sdk.AccAddressFromBech32(req.BeginUnbond.DelegatorAddr)
		if err != nil {
			utils.WriteErrorResponse(w, http.StatusBadRequest, err.Error())
			return
		}

		valAddr, err := sdk.ValAddressFromBech32(req.BeginUnbond.ValidatorAddr)
		if err != nil {
			utils.WriteErrorResponse(w, http.StatusBadRequest, err.Error())
			return
		}

		shares, err := sdk.NewDecFromStr(req.BeginUnbond.SharesAmount)
		if err != nil {
			utils.WriteErrorResponse(w, http.StatusBadRequest, err.Error())
			return
		}

		msg := stake.MsgBeginUnbonding{
			DelegatorAddr: delAddr,
			ValidatorAddr: valAddr,
			SharesAmount:  sdk.NewDecFromInt(utils.ConvertDecToRat(shares).Quo(utils.ExRateFromStakeTokenToMainUnit(cliCtx)).Num()),
		}

		utils.SendOrReturnUnsignedTx(w, cliCtx, req.BaseReq, []sdk.Msg{msg})
	}
}
