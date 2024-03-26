package main

import (
	"context"
	"io"
	"os"
	"time"

	"github.com/davecgh/go-spew/spew"
	commcid "github.com/filecoin-project/go-fil-commcid"
	commp "github.com/filecoin-project/go-fil-commp-hashhash"
	"github.com/filecoin-project/go-padreader"
	filabi "github.com/filecoin-project/go-state-types/abi"
	filsptypes "github.com/filecoin-project/go-state-types/builtin/v13/miner"
	lcliutil "github.com/filecoin-project/lotus/cli/util"
	"github.com/filecoin-project/lotus/storage/pipeline/piece"
	"github.com/ribasushi/go-toolbox-interplanetary/fil"
	"github.com/ribasushi/go-toolbox/cmn"
	"github.com/ribasushi/go-toolbox/ufcli"
	"golang.org/x/xerrors"
)

const MAI_ENVVAR = "MINER_API_INFO"

var (
	uf          *ufcli.UFcli
	spAPI       fil.LotusMinerAPIClientV0
	spAPICloser func()
)

func main() {
	uf = &ufcli.UFcli{
		AppConfig: ufcli.App{
			Name:  "fil-sp-toolbox",
			Usage: "Assorted utilities for Fil SP sector/piece manipulation",
			Commands: []*ufcli.Command{
				basicDdoPieceAdd,
			},
			Flags: nil,
		},
		AllowConcurrentRuns: true,
		GlobalInit: func(cctx *ufcli.Context, uf *ufcli.UFcli) (func() error, error) {
			mai, isSet := os.LookupEnv(MAI_ENVVAR)
			if !isSet {
				return nil, xerrors.Errorf("environment variable %s must be set to continue", MAI_ENVVAR)
			}
			envApiInfo := lcliutil.ParseApiInfo(mai)
			da, err := envApiInfo.DialArgs("v0")
			if err != nil {
				return nil, cmn.WrErr(err)
			}
			spAPI, spAPICloser, err = fil.NewLotusMinerAPIClientV0(cctx.Context, da, 0, string(envApiInfo.Token))
			if err != nil {
				return nil, cmn.WrErr(err)
			}
			return nil, nil
		},
		BeforeShutdown: func() error {
			if spAPICloser != nil {
				spAPICloser()
			}
			return nil
		},
	}

	uf.RunAndExit(context.Background())
}

// var (
//
//	precomputedCid string
//
// )
var basicDdoPieceAdd = &ufcli.Command{
	Name:      "basic-ddo-add",
	Usage:     "Supply input data to SectorAddPieceToAny(), without a staking voucher (a.k.a. Fil+)",
	ArgsUsage: "InputDataPath",
	Flags:     []ufcli.Flag{
		// &ufcli.StringFlag{
		// 	Name:        "precomputed-pcid",
		// 	Usage:       "Use the provided PieceCID, concurrently validating it against the provided data",
		// 	Destination: &precomputedCid,
		// },
	},
	Action: func(cctx *ufcli.Context) error {

		spActor, err := spAPI.ActorAddress(cctx.Context)
		if err != nil {
			return cmn.WrErr(err)
		}

		spSecSize, err := spAPI.ActorSectorSize(cctx.Context, spActor)
		if err != nil {
			return cmn.WrErr(err)
		}

		if cctx.NArg() != 1 {
			return xerrors.New("expecting exactly one argument: the path to the data to be injected (or /dev/stdin)")
		}

		fn := cctx.Args().First()
		fh, err := os.Open(fn)
		if err != nil {
			return cmn.WrErr(err)
		}

		fhs, err := fh.Stat()
		if err != nil {
			return cmn.WrErr(err)
		}

		maxPayload := int64(filabi.PaddedPieceSize(spSecSize).Unpadded())

		// easy pre-checks if we can help it
		if fhs.Mode().IsRegular() {
			if fhs.Size() == 0 {
				return xerrors.Errorf("supplied source file %s appears to be empty", fn)
			} else if fhs.Size() > maxPayload {
				return xerrors.Errorf("supplied source file %s is %d bytes long, which is over the maximum sector payload supported by SP %s", fn, fhs.Size(), spActor)
			}
		} else {
			return xerrors.Errorf("only regular files supported for the MVP")
		}

		// TEMPORARY, precalculate stuff
		fr32 := new(commp.Calc)
		var bytesHashed int64

		for {
			if err := cctx.Context.Err(); err != nil {
				return err
			}
			n, err := io.CopyN(fr32, fh, 128<<20)
			bytesHashed += n
			if err == io.EOF {
				break
			}
			if err != nil {
				return cmn.WrErr(err)
			}
			if bytesHashed > maxPayload {
				return xerrors.Errorf("data supplied via %s is at least %d bytes long, which is over the maximum sector payload supported by SP %s", fn, fhs.Size(), spActor)
			}
		}

		if bytesHashed < 127 {
			if _, err := fr32.Write(make([]byte, 127-bytesHashed)); err != nil {
				return cmn.WrErr(err)
			}
			bytesHashed = 127
		}

		cpHash, pieceSz, err := fr32.Digest()
		if err != nil {
			return cmn.WrErr(err)
		}
		pCID, err := commcid.DataCommitmentV1ToCID(cpHash)
		if err != nil {
			return cmn.WrErr(err)
		}

		if _, err := fh.Seek(0, 0); err != nil {
			return cmn.WrErr(err)
		}

		uf.GetLogger().Infof("hashed %d bytes hashing to %s, pad %d", bytesHashed, pCID, pieceSz)

		startEpoch := ((time.Now().Unix() - 1667326380) / 30) + 2880*2
		piecePadSz := filabi.PaddedPieceSize(pieceSz)
		padRdr, _ := padreader.New(fh, uint64(bytesHashed))
		pdi := piece.PieceDealInfo{
			DealSchedule: piece.DealSchedule{
				StartEpoch: filabi.ChainEpoch(startEpoch),
				EndEpoch:   filabi.ChainEpoch(startEpoch + 2880*200),
			},
			KeepUnsealed: false,
			PieceActivationManifest: &filsptypes.PieceActivationManifest{
				CID:  pCID,
				Size: piecePadSz,
			},
		}

		spew.Dump(pdi)

		so, err := spAPI.SectorAddPieceToAny(cctx.Context, piecePadSz.Unpadded(), padRdr, pdi)
		if err != nil {
			return cmn.WrErr(err)
		}

		spew.Dump(so)

		return nil
	},
}
